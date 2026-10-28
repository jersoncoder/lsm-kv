"""SSTables: immutable sorted files of entries.

The byte layout is specified in ``docs/FORMAT.md``. In short::

    data block 0 .. data block n-1 | filter block | properties block | index block | footer

Every block is framed as ``type:u8 body crc:u32`` with the CRC-32 covering the
type byte and the body, and is verified on every read. The footer has its own
checksum, a format version and a magic number. The reader keeps the sparse
index (the last key of each data block), the Bloom filter and the properties
in memory and reads data blocks with ``pread`` on demand.
"""

from __future__ import annotations

import os
import struct
import zlib
from array import array
from bisect import bisect_left
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Final

from . import fileops
from .bloom import BloomFilter, key_hash
from .entry import KIND_DELETE, KIND_PUT, MAX_SEQUENCE, Entry, internal_order
from .errors import CorruptionError, DBClosedError, UnsupportedFormatError

MAGIC: Final = b"LSMKVSST"
FORMAT_VERSION: Final = 1

BLOCK_DATA: Final = 1
BLOCK_FILTER: Final = 2
BLOCK_PROPERTIES: Final = 3
BLOCK_INDEX: Final = 4
_BLOCK_NAMES: Final = {
    BLOCK_DATA: "data",
    BLOCK_FILTER: "filter",
    BLOCK_PROPERTIES: "properties",
    BLOCK_INDEX: "index",
}

_U32: Final = struct.Struct("<I")
"""Lengths and checksums."""
BLOCK_OVERHEAD: Final = 1 + _U32.size
"""Type byte plus trailing checksum."""

_ENTRY: Final = struct.Struct("<IIQB")
"""Data block entry header: key_len, value_len, seq, kind."""

_INDEX_ENTRY: Final = struct.Struct("<IQQI")
"""Index entry header: key_len, seq, block offset, block size."""

_PROPERTIES: Final = struct.Struct("<QQQ")
"""Properties header: entry_count, min_seq, max_seq; then two length-prefixed keys."""

_FOOTER_BODY: Final = struct.Struct("<QIQIQII")
"""filter offset/size, properties offset/size, index offset/size, format version."""

FOOTER_SIZE: Final = _FOOTER_BODY.size + _U32.size + len(MAGIC)


@dataclass(frozen=True, slots=True)
class TableProperties:
    entry_count: int
    min_seq: int
    max_seq: int
    smallest: bytes
    largest: bytes


def _frame(block_type: int, body: bytes) -> bytes:
    block = bytes((block_type,)) + body
    return block + _U32.pack(zlib.crc32(block))


class TableWriter:
    """Writes one table. Entries must arrive in strictly increasing internal order."""

    def __init__(self, path: str, *, block_size: int, bloom_bits_per_key: int):
        self._path = path
        self._block_size = block_size
        self._bloom_bits_per_key = bloom_bits_per_key
        self._fd: int | None = fileops.create(path)
        self._offset = 0
        self._parts: list[bytes] = []
        self._block_bytes = 0
        self._index: list[tuple[bytes, int, int, int]] = []
        self._hashes: array[int] = array("Q")
        self._last: tuple[bytes, int] | None = None
        self._count = 0
        self._min_seq = MAX_SEQUENCE
        self._max_seq = 0
        self._smallest = b""

    @property
    def entry_count(self) -> int:
        return self._count

    def add(self, key: bytes, seq: int, kind: int, value: bytes) -> None:
        last = self._last
        if last is not None:
            if key < last[0] or (key == last[0] and seq >= last[1]):
                raise ValueError("entries must be added in strictly increasing internal order")
            if key != last[0]:
                self._hashes.append(key_hash(key))
        else:
            self._hashes.append(key_hash(key))
            self._smallest = key
        self._parts += (_ENTRY.pack(len(key), len(value), seq, kind), key, value)
        self._block_bytes += _ENTRY.size + len(key) + len(value)
        self._last = (key, seq)
        self._count += 1
        self._min_seq = min(self._min_seq, seq)
        self._max_seq = max(self._max_seq, seq)
        if self._block_bytes >= self._block_size:
            self._finish_data_block()

    def finish(self) -> tuple[TableProperties, int]:
        """Write the metadata blocks and footer, fsync and close.

        Returns the table's properties and its size in bytes.
        """
        if self._last is None:
            raise ValueError("a table needs at least one entry")
        if self._parts:
            self._finish_data_block()
        props = TableProperties(
            entry_count=self._count,
            min_seq=self._min_seq,
            max_seq=self._max_seq,
            smallest=self._smallest,
            largest=self._last[0],
        )
        bloom = BloomFilter.build(self._hashes, self._bloom_bits_per_key)
        filter_offset, filter_size = self._write_block(BLOCK_FILTER, bloom.encode())
        props_body = b"".join(
            (
                _PROPERTIES.pack(props.entry_count, props.min_seq, props.max_seq),
                _U32.pack(len(props.smallest)),
                props.smallest,
                _U32.pack(len(props.largest)),
                props.largest,
            )
        )
        props_offset, props_size = self._write_block(BLOCK_PROPERTIES, props_body)
        index_body = b"".join(
            _INDEX_ENTRY.pack(len(key), seq, offset, size) + key
            for key, seq, offset, size in self._index
        )
        index_offset, index_size = self._write_block(BLOCK_INDEX, index_body)
        footer_body = _FOOTER_BODY.pack(
            filter_offset,
            filter_size,
            props_offset,
            props_size,
            index_offset,
            index_size,
            FORMAT_VERSION,
        )
        self._write(footer_body + _U32.pack(zlib.crc32(footer_body)) + MAGIC)
        fd = self._fd
        assert fd is not None
        fileops.fsync(fd)
        fileops.close(fd)
        self._fd = None
        return props, self._offset

    def abandon(self) -> None:
        """Close and delete a table that will not be finished."""
        if self._fd is not None:
            fileops.close(self._fd)
            self._fd = None
        fileops.remove(self._path)

    def _finish_data_block(self) -> None:
        assert self._last is not None
        offset, size = self._write_block(BLOCK_DATA, b"".join(self._parts))
        self._index.append((self._last[0], self._last[1], offset, size))
        self._parts = []
        self._block_bytes = 0

    def _write_block(self, block_type: int, body: bytes) -> tuple[int, int]:
        block = _frame(block_type, body)
        offset = self._offset
        self._write(block)
        return offset, len(block)

    def _write(self, data: bytes) -> None:
        if self._fd is None:
            raise ValueError("table writer is closed")
        fileops.write(self._fd, data)
        self._offset += len(data)


class TableReader:
    """Reads one table. Opening it verifies the footer and all metadata blocks."""

    def __init__(self, path: str):
        self._path = path
        self._fd = os.open(path, os.O_RDONLY)
        try:
            self._load()
        except BaseException:
            os.close(self._fd)
            raise

    @property
    def path(self) -> str:
        return self._path

    @property
    def file_size(self) -> int:
        return self._file_size

    @property
    def properties(self) -> TableProperties:
        return self._props

    @property
    def block_count(self) -> int:
        return len(self._block_offsets)

    def close(self) -> None:
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1

    def may_contain(self, key: bytes) -> bool:
        return self._bloom.may_contain(key)

    def get(self, key: bytes, seq: int) -> tuple[int, int, bytes] | None:
        """Return ``(seq, kind, value)`` of the newest version of ``key`` at or below ``seq``."""
        if not self._bloom.may_contain(key):
            return None
        target = (key, -seq)
        i = bisect_left(self._index_keys, target)
        if i == len(self._index_keys):
            return None
        block = self._read_data_block(i)
        j = bisect_left(block, target, key=internal_order)
        # The index guarantees the block's last entry is >= target, so j is in range.
        found_key, found_seq, kind, value = block[j]
        if found_key != key:
            return None
        return found_seq, kind, value

    def entries(self, start: bytes | None = None, end: bytes | None = None) -> Iterator[Entry]:
        """Yield entries with ``start <= key < end`` in internal order."""
        first = 0
        if start is not None:
            first = bisect_left(self._index_keys, (start, -MAX_SEQUENCE))
        for i in range(first, len(self._index_keys)):
            block = self._read_data_block(i)
            j = 0
            if i == first and start is not None:
                j = bisect_left(block, (start, -MAX_SEQUENCE), key=internal_order)
            for entry in block[j:] if j else block:
                if end is not None and entry[0] >= end:
                    return
                yield entry

    # -- reading and verifying ------------------------------------------------

    def _corrupt(self, message: str, offset: int | None = None) -> CorruptionError:
        return CorruptionError(message, path=self._path, offset=offset)

    def _read_block(self, offset: int, size: int, expected_type: int) -> bytes:
        if self._fd < 0:
            raise DBClosedError("table reader is closed")
        name = _BLOCK_NAMES[expected_type]
        data = os.pread(self._fd, size, offset)
        if len(data) != size:
            raise self._corrupt(f"{name} block extends past the end of the file", offset)
        (stored_crc,) = _U32.unpack_from(data, size - _U32.size)
        if zlib.crc32(memoryview(data)[: size - _U32.size]) != stored_crc:
            raise self._corrupt(f"{name} block checksum mismatch", offset)
        if data[0] != expected_type:
            raise self._corrupt(f"expected a {name} block, found block type {data[0]}", offset)
        return data[1 : size - _U32.size]

    def _read_data_block(self, i: int) -> list[Entry]:
        offset = self._block_offsets[i]
        body = self._read_block(offset, self._block_sizes[i], BLOCK_DATA)
        entries: list[Entry] = []
        pos = 0
        n = len(body)
        while pos < n:
            if pos + _ENTRY.size > n:
                raise self._corrupt("truncated entry in data block", offset)
            key_len, value_len, seq, kind = _ENTRY.unpack_from(body, pos)
            pos += _ENTRY.size
            end = pos + key_len + value_len
            if end > n or kind not in (KIND_PUT, KIND_DELETE):
                raise self._corrupt("malformed entry in data block", offset)
            entries.append((body[pos : pos + key_len], seq, kind, body[pos + key_len : end]))
            pos = end
        if not entries:
            raise self._corrupt("empty data block", offset)
        last = entries[-1]
        if (last[0], -last[1]) != self._index_keys[i]:
            raise self._corrupt("data block does not end with its index key", offset)
        return entries

    def _load(self) -> None:
        size = os.fstat(self._fd).st_size
        self._file_size = size
        if size < FOOTER_SIZE:
            raise self._corrupt("file is shorter than a table footer", 0)
        footer_offset = size - FOOTER_SIZE
        footer = os.pread(self._fd, FOOTER_SIZE, footer_offset)
        if footer[-len(MAGIC) :] != MAGIC:
            raise self._corrupt("bad magic number in table footer", size - len(MAGIC))
        body = footer[: _FOOTER_BODY.size]
        (stored_crc,) = _U32.unpack_from(footer, _FOOTER_BODY.size)
        if zlib.crc32(body) != stored_crc:
            raise self._corrupt("table footer checksum mismatch", footer_offset)
        (
            filter_offset,
            filter_size,
            props_offset,
            props_size,
            index_offset,
            index_size,
            version,
        ) = _FOOTER_BODY.unpack(body)
        if version != FORMAT_VERSION:
            raise UnsupportedFormatError(
                f"table format version {version} is not supported "
                f"(this code reads version {FORMAT_VERSION}): {self._path}"
            )
        if (
            min(filter_size, props_size, index_size) < BLOCK_OVERHEAD
            or props_offset != filter_offset + filter_size
            or index_offset != props_offset + props_size
            or footer_offset != index_offset + index_size
        ):
            raise self._corrupt("table footer describes an impossible layout", footer_offset)

        try:
            self._bloom = BloomFilter.decode(
                self._read_block(filter_offset, filter_size, BLOCK_FILTER)
            )
        except ValueError as exc:
            raise self._corrupt(f"malformed filter block: {exc}", filter_offset) from None
        self._props = self._parse_properties(
            self._read_block(props_offset, props_size, BLOCK_PROPERTIES), props_offset
        )
        self._parse_index(self._read_block(index_offset, index_size, BLOCK_INDEX), index_offset)
        if not self._index_keys or filter_offset != (
            self._block_offsets[-1] + self._block_sizes[-1]
        ):
            raise self._corrupt("index does not cover the data blocks", index_offset)
        if self._index_keys[-1][0] != self._props.largest:
            raise self._corrupt("index and properties disagree on the largest key", index_offset)

    def _parse_properties(self, body: bytes, offset: int) -> TableProperties:
        try:
            entry_count, min_seq, max_seq = _PROPERTIES.unpack_from(body, 0)
            pos = _PROPERTIES.size
            (smallest_len,) = _U32.unpack_from(body, pos)
            pos += _U32.size
            smallest = body[pos : pos + smallest_len]
            pos += smallest_len
            (largest_len,) = _U32.unpack_from(body, pos)
            pos += _U32.size
            largest = body[pos : pos + largest_len]
            pos += largest_len
        except struct.error:
            raise self._corrupt("truncated properties block", offset) from None
        if pos != len(body) or len(largest) != largest_len or min_seq > max_seq:
            raise self._corrupt("malformed properties block", offset)
        return TableProperties(entry_count, min_seq, max_seq, smallest, largest)

    def _parse_index(self, body: bytes, offset: int) -> None:
        keys: list[tuple[bytes, int]] = []
        offsets: list[int] = []
        sizes: list[int] = []
        expected_offset = 0
        pos = 0
        while pos < len(body):
            if pos + _INDEX_ENTRY.size > len(body):
                raise self._corrupt("truncated index entry", offset)
            key_len, seq, block_offset, block_size = _INDEX_ENTRY.unpack_from(body, pos)
            pos += _INDEX_ENTRY.size
            key = body[pos : pos + key_len]
            pos += key_len
            order = (key, -seq)
            if (
                len(key) != key_len
                or block_offset != expected_offset
                or block_size < BLOCK_OVERHEAD
                or (keys and order <= keys[-1])
            ):
                raise self._corrupt("malformed index entry", offset)
            keys.append(order)
            offsets.append(block_offset)
            sizes.append(block_size)
            expected_offset = block_offset + block_size
        self._index_keys = keys
        self._block_offsets = offsets
        self._block_sizes = sizes
