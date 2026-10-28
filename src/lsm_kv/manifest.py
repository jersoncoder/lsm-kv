"""The manifest: the durable record of which tables are live.

The manifest is small and rewritten whole on every change, never edited in
place. :func:`store` writes it to a temporary file, fsyncs it, renames it over
the old one and fsyncs the directory, so after a crash the directory holds
either the old manifest or the new one, never a mix. The byte layout is in
``docs/FORMAT.md``.
"""

from __future__ import annotations

import contextlib
import json
import os
import struct
import zlib
from dataclasses import dataclass
from typing import Any, Final

from . import fileops
from .errors import CorruptionError, UnsupportedFormatError

MANIFEST_NAME: Final = "MANIFEST"
TEMP_NAME: Final = "MANIFEST.tmp"
MAGIC: Final = b"LSMKVMAN"
FORMAT_VERSION: Final = 1

_HEADER: Final = struct.Struct("<8sIII")
"""magic, format version, payload length, CRC-32 of (version, length, payload)."""


@dataclass(frozen=True, slots=True)
class TableInfo:
    number: int
    tier: int
    size: int
    entry_count: int
    min_seq: int
    max_seq: int
    smallest: bytes
    largest: bytes


@dataclass(frozen=True, slots=True)
class Manifest:
    next_file_number: int
    """Lower bound for the number of the next file created."""

    log_number: int
    """Logs numbered below this hold only data that is already in tables."""

    last_sequence: int
    """Highest sequence number stored in tables."""

    tables: tuple[TableInfo, ...]
    """Live tables, newest first."""


def encode(manifest: Manifest) -> bytes:
    payload = json.dumps(
        {
            "next_file_number": manifest.next_file_number,
            "log_number": manifest.log_number,
            "last_sequence": manifest.last_sequence,
            "tables": [
                {
                    "number": t.number,
                    "tier": t.tier,
                    "size": t.size,
                    "entry_count": t.entry_count,
                    "min_seq": t.min_seq,
                    "max_seq": t.max_seq,
                    "smallest": t.smallest.hex(),
                    "largest": t.largest.hex(),
                }
                for t in manifest.tables
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    checked = struct.pack("<II", FORMAT_VERSION, len(payload)) + payload
    return _HEADER.pack(MAGIC, FORMAT_VERSION, len(payload), zlib.crc32(checked)) + payload


def decode(data: bytes, path: str) -> Manifest:
    if len(data) < _HEADER.size:
        raise CorruptionError("manifest is shorter than its header", path=path, offset=0)
    magic, version, length, stored_crc = _HEADER.unpack_from(data, 0)
    if magic != MAGIC:
        raise CorruptionError("bad magic number in manifest", path=path, offset=0)
    if len(data) != _HEADER.size + length:
        raise CorruptionError("manifest length does not match its header", path=path, offset=0)
    if zlib.crc32(data[8:16] + data[_HEADER.size :]) != stored_crc:
        raise CorruptionError("manifest checksum mismatch", path=path, offset=0)
    if version != FORMAT_VERSION:
        raise UnsupportedFormatError(
            f"manifest format version {version} is not supported "
            f"(this code reads version {FORMAT_VERSION}): {path}"
        )
    try:
        return _from_json(json.loads(data[_HEADER.size :]))
    except (ValueError, TypeError, KeyError) as exc:
        raise CorruptionError(f"malformed manifest payload: {exc}", path=path) from None


def load(directory: str) -> Manifest | None:
    """Read the manifest, or return None if the directory has none."""
    path = os.path.join(directory, MANIFEST_NAME)
    try:
        with open(path, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return None
    return decode(data, path)


def store(directory: str, manifest: Manifest) -> int:
    """Atomically replace the manifest. Returns the bytes written."""
    data = encode(manifest)
    temp = os.path.join(directory, TEMP_NAME)
    with contextlib.suppress(FileNotFoundError):
        fileops.remove(temp)
    fd = fileops.create(temp)
    try:
        fileops.write(fd, data)
        fileops.fsync(fd)
    finally:
        fileops.close(fd)
    fileops.rename(temp, os.path.join(directory, MANIFEST_NAME))
    fileops.fsync_dir(directory)
    return len(data)


def _int(obj: dict[str, Any], name: str) -> int:
    value = obj[name]
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _from_json(obj: Any) -> Manifest:
    if not isinstance(obj, dict) or not isinstance(obj["tables"], list):
        raise ValueError("unexpected structure")
    tables = []
    for t in obj["tables"]:
        if not isinstance(t, dict):
            raise ValueError("unexpected table entry")
        tables.append(
            TableInfo(
                number=_int(t, "number"),
                tier=_int(t, "tier"),
                size=_int(t, "size"),
                entry_count=_int(t, "entry_count"),
                min_seq=_int(t, "min_seq"),
                max_seq=_int(t, "max_seq"),
                smallest=bytes.fromhex(t["smallest"]),
                largest=bytes.fromhex(t["largest"]),
            )
        )
    return Manifest(
        next_file_number=_int(obj, "next_file_number"),
        log_number=_int(obj, "log_number"),
        last_sequence=_int(obj, "last_sequence"),
        tables=tuple(tables),
    )
