"""The write-ahead log: framing, appending and recovery scanning.

A log file is a sequence of records with no file header. Each record is::

    length:u32  length_crc:u32  payload_crc:u32  payload[length]

All integers are little-endian. ``length_crc`` is the CRC-32 of the four
``length`` bytes and ``payload_crc`` the CRC-32 of the payload (zlib's CRC-32,
the IEEE 802.3 polynomial). Checking the length separately means a damaged
length field is detected instead of sending the reader to a wrong offset.

Recovery has to tell an interrupted append (a torn tail, which is expected
after a crash and is cut off) from damage to data that was written in full
(which must stop recovery). :func:`read_log` applies these rules to the record
starting at offset ``p``:

* fewer than 12 bytes remain: torn tail;
* the header is intact and the record runs past the end of the file: torn tail;
* the header is intact, the payload checksum fails and the record ends exactly
  at the end of the file: torn tail (a damaged last record);
* the header is intact, the payload checksum fails and more bytes follow:
  corruption;
* the header checksum fails: corruption if an intact record can be found at
  any later offset, otherwise torn tail (this covers zero-filled or garbage
  tails some file systems leave after a power failure).
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from typing import Final

from . import fileops
from .errors import CorruptionError

HEADER: Final = struct.Struct("<III")
HEADER_SIZE: Final = HEADER.size
_LENGTH: Final = struct.Struct("<I")


def encode_record(payload: bytes) -> bytes:
    length = _LENGTH.pack(len(payload))
    return HEADER.pack(len(payload), zlib.crc32(length), zlib.crc32(payload)) + payload


class LogWriter:
    """Appends records to one log file."""

    __slots__ = ("_fd", "_path", "_size")

    def __init__(self, path: str, fd: int, size: int):
        self._path = path
        self._fd = fd
        self._size = size

    @classmethod
    def create(cls, path: str) -> LogWriter:
        return cls(path, fileops.create(path), 0)

    @classmethod
    def reopen(cls, path: str, size: int) -> LogWriter:
        """Continue appending to an existing log whose intact prefix is ``size`` bytes."""
        return cls(path, fileops.open_append(path), size)

    @property
    def path(self) -> str:
        return self._path

    @property
    def size(self) -> int:
        return self._size

    def append(self, payload: bytes) -> int:
        """Hand one record to the operating system; return the bytes written."""
        record = encode_record(payload)
        fileops.write(self._fd, record)
        self._size += len(record)
        return len(record)

    def sync(self) -> None:
        fileops.fsync(self._fd)

    def close(self) -> None:
        fileops.close(self._fd)


@dataclass(frozen=True, slots=True)
class LogContents:
    records: list[tuple[int, bytes]]
    """``(offset, payload)`` of each intact record, in order."""

    end: int
    """Offset just past the last intact record."""

    size: int
    """File size. Larger than ``end`` when the file has a torn tail."""

    @property
    def torn(self) -> bool:
        return self.size > self.end


def read_log(path: str) -> LogContents:
    """Read every intact record of a log file, applying the torn-tail rules.

    Raises :class:`CorruptionError` for damage that is not at the tail.
    """
    with open(path, "rb") as f:
        data = f.read()
    records: list[tuple[int, bytes]] = []
    pos = 0
    size = len(data)
    while pos < size:
        end = _intact_record_end(data, pos)
        if end is not None:
            records.append((pos, data[pos + HEADER_SIZE : end]))
            pos = end
            continue
        if _is_torn_tail(data, pos):
            break
        raise CorruptionError("damaged record in the middle of the log", path=path, offset=pos)
    return LogContents(records=records, end=pos, size=size)


def _intact_record_end(data: bytes, pos: int) -> int | None:
    """Return the end offset of the record at ``pos`` if it is intact."""
    if len(data) - pos < HEADER_SIZE:
        return None
    length: int
    length_crc: int
    payload_crc: int
    length, length_crc, payload_crc = HEADER.unpack_from(data, pos)
    if zlib.crc32(data[pos : pos + 4]) != length_crc:
        return None
    end = pos + HEADER_SIZE + length
    if end > len(data):
        return None
    if zlib.crc32(data[pos + HEADER_SIZE : end]) != payload_crc:
        return None
    return end


def _is_torn_tail(data: bytes, pos: int) -> bool:
    """Decide whether the non-intact record at ``pos`` is a torn tail."""
    size = len(data)
    if size - pos < HEADER_SIZE:
        return True
    length: int
    length_crc: int
    length, length_crc, _ = HEADER.unpack_from(data, pos)
    if zlib.crc32(data[pos : pos + 4]) == length_crc:
        # The length is trustworthy: the record is torn if it reaches the end.
        return pos + HEADER_SIZE + length >= size
    # The length is not trustworthy. Damage in the middle of the log leaves
    # intact records after it; a garbage tail does not. Zero-filled tails are
    # the common case and are checked without the byte-by-byte search.
    if not data[pos:].strip(b"\x00"):
        return True
    return not any(_intact_record_end(data, p) is not None for p in range(pos + 1, size))
