"""Write batches and their encoding as one write-ahead log record.

A batch is written to the log as a single record, so it is applied atomically:
recovery either finds the whole record intact or drops it.

Record payload (little-endian)::

    seq:u64  count:u32  op[count]
    op := kind:u8  key_len:u32  value_len:u32  key  value

The ops of a batch get consecutive sequence numbers starting at ``seq``, so a
later op in the same batch overrides an earlier one on the same key.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator
from typing import Final

from .entry import KIND_DELETE, KIND_PUT
from .errors import CorruptionError

MAX_LENGTH: Final = 0xFFFFFFFF
"""Keys and values are length-prefixed with 32 bits."""

_BATCH_HEADER = struct.Struct("<QI")
_OP_HEADER = struct.Struct("<BII")

Op = tuple[int, bytes, bytes]
"""``(kind, key, value)``; the value of a delete is empty."""

BytesLike = bytes | bytearray | memoryview
"""Accepted for keys and values; stored as ``bytes``."""


def as_bytes(name: str, data: object) -> bytes:
    """Accept bytes-like objects, reject everything else (notably ``str``)."""
    if isinstance(data, bytes):
        result = data
    elif isinstance(data, bytearray | memoryview):
        result = bytes(data)
    else:
        raise TypeError(f"{name} must be bytes, not {type(data).__name__}")
    if len(result) > MAX_LENGTH:
        raise ValueError(f"{name} is longer than {MAX_LENGTH} bytes")
    return result


class WriteBatch:
    """An ordered group of puts and deletes applied atomically by ``DB.write``."""

    __slots__ = ("_ops",)

    def __init__(self) -> None:
        self._ops: list[Op] = []

    def put(self, key: BytesLike, value: BytesLike) -> None:
        self._ops.append((KIND_PUT, as_bytes("key", key), as_bytes("value", value)))

    def delete(self, key: BytesLike) -> None:
        self._ops.append((KIND_DELETE, as_bytes("key", key), b""))

    def clear(self) -> None:
        self._ops.clear()

    def __len__(self) -> int:
        return len(self._ops)

    def __iter__(self) -> Iterator[Op]:
        return iter(self._ops)


def encode_batch(seq: int, ops: list[Op]) -> bytes:
    parts = [_BATCH_HEADER.pack(seq, len(ops))]
    for kind, key, value in ops:
        parts.append(_OP_HEADER.pack(kind, len(key), len(value)))
        parts.append(key)
        parts.append(value)
    return b"".join(parts)


def decode_batch(payload: bytes, *, path: str, offset: int) -> tuple[int, list[Op]]:
    """Decode a record payload into ``(first sequence number, ops)``.

    The payload has already passed its checksum, so any structural problem
    here means the writer and reader disagree; it is still reported as
    corruption rather than trusted.
    """

    def bad(reason: str) -> CorruptionError:
        return CorruptionError(f"malformed log record: {reason}", path=path, offset=offset)

    if len(payload) < _BATCH_HEADER.size:
        raise bad("shorter than its header")
    seq, count = _BATCH_HEADER.unpack_from(payload, 0)
    if count == 0:
        raise bad("empty batch")
    pos = _BATCH_HEADER.size
    ops: list[Op] = []
    for _ in range(count):
        if pos + _OP_HEADER.size > len(payload):
            raise bad("truncated op header")
        kind, key_len, value_len = _OP_HEADER.unpack_from(payload, pos)
        pos += _OP_HEADER.size
        if kind not in (KIND_PUT, KIND_DELETE):
            raise bad(f"unknown op kind {kind}")
        if kind == KIND_DELETE and value_len != 0:
            raise bad("delete with a value")
        end = pos + key_len + value_len
        if end > len(payload):
            raise bad("op extends past the record")
        ops.append((kind, payload[pos : pos + key_len], payload[pos + key_len : end]))
        pos = end
    if pos != len(payload):
        raise bad("trailing bytes after the last op")
    return seq, ops
