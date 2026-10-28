from __future__ import annotations

import json
import struct
import zlib
from pathlib import Path

import pytest

from lsm_kv import CorruptionError, UnsupportedFormatError
from lsm_kv.manifest import (
    MAGIC,
    MANIFEST_NAME,
    TEMP_NAME,
    Manifest,
    TableInfo,
    decode,
    encode,
    load,
    store,
)

STATE = Manifest(
    next_file_number=12,
    log_number=11,
    last_sequence=500,
    tables=(
        TableInfo(10, 0, 4096, 40, 401, 500, b"a", b"z\xff"),
        TableInfo(7, 1, 9000, 120, 1, 400, b"", b"y"),
    ),
)


def test_round_trip() -> None:
    assert decode(encode(STATE), "MANIFEST") == STATE


def test_store_replaces_the_manifest_and_leaves_no_temporary_file(tmp_path: Path) -> None:
    assert load(str(tmp_path)) is None
    store(str(tmp_path), STATE)
    newer = Manifest(13, 12, 600, STATE.tables[1:])
    (tmp_path / TEMP_NAME).write_bytes(b"left over from a crash")
    store(str(tmp_path), newer)
    assert load(str(tmp_path)) == newer
    assert sorted(p.name for p in tmp_path.iterdir()) == [MANIFEST_NAME]


def test_any_flipped_byte_is_detected() -> None:
    data = encode(STATE)
    for offset in range(len(data)):
        damaged = bytearray(data)
        damaged[offset] ^= 0x01
        with pytest.raises(CorruptionError):
            decode(bytes(damaged), "MANIFEST")


def test_any_truncation_is_detected() -> None:
    data = encode(STATE)
    for length in range(len(data)):
        with pytest.raises(CorruptionError):
            decode(data[:length], "MANIFEST")


def framed(payload: bytes, version: int = 1) -> bytes:
    checked = struct.pack("<II", version, len(payload)) + payload
    return struct.pack("<8sIII", MAGIC, version, len(payload), zlib.crc32(checked)) + payload


def test_newer_format_version_is_reported_as_unsupported() -> None:
    with pytest.raises(UnsupportedFormatError, match="version 2"):
        decode(framed(b"{}", version=2), "MANIFEST")


@pytest.mark.parametrize(
    "payload",
    [
        b"not json",
        b"[]",
        json.dumps({"next_file_number": 1, "log_number": 1, "last_sequence": 0}).encode(),
        json.dumps(
            {"next_file_number": -1, "log_number": 1, "last_sequence": 0, "tables": []}
        ).encode(),
        json.dumps(
            {"next_file_number": True, "log_number": 1, "last_sequence": 0, "tables": []}
        ).encode(),
        json.dumps(
            {"next_file_number": 1, "log_number": 1, "last_sequence": 0, "tables": [{}]}
        ).encode(),
    ],
)
def test_well_framed_but_malformed_payload_is_corruption(payload: bytes) -> None:
    with pytest.raises(CorruptionError, match="malformed manifest"):
        decode(framed(payload), "MANIFEST")
