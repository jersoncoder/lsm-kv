"""Write-ahead log framing, batch encoding and the torn-tail rules."""

from __future__ import annotations

import os
import random
from pathlib import Path

import pytest

from lsm_kv import CorruptionError, WriteBatch
from lsm_kv.batch import as_bytes, decode_batch, encode_batch
from lsm_kv.entry import KIND_DELETE, KIND_PUT
from lsm_kv.wal import HEADER_SIZE, LogWriter, encode_record, read_log

PAYLOADS = [b"first record", b"", b"x" * 300, b"last one"]


def write_log(path: Path, payloads: list[bytes]) -> list[int]:
    """Write records and return the offset where each one starts."""
    writer = LogWriter.create(str(path))
    offsets = []
    for payload in payloads:
        offsets.append(writer.size)
        writer.append(payload)
    writer.close()
    return offsets


def flip(path: Path, offset: int) -> None:
    data = bytearray(path.read_bytes())
    data[offset] ^= 0xFF
    path.write_bytes(bytes(data))


# -- batch encoding ----------------------------------------------------------


def test_batch_round_trip() -> None:
    ops = [(KIND_PUT, b"a", b"1"), (KIND_DELETE, b"b", b""), (KIND_PUT, b"", b"")]
    seq, decoded = decode_batch(encode_batch(42, ops), path="log", offset=0)
    assert seq == 42
    assert decoded == ops


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (b"\x01\x02", "shorter than its header"),
        (encode_batch(1, [])[:12], "empty batch"),
        (encode_batch(1, [(KIND_PUT, b"k", b"v")])[:-1], "extends past the record"),
        (encode_batch(1, [(KIND_PUT, b"k", b"v")]) + b"\x00", "trailing bytes"),
        (encode_batch(1, [(7, b"k", b"v")]), "unknown op kind"),
        (encode_batch(1, [(KIND_DELETE, b"k", b"v")]), "delete with a value"),
        (encode_batch(1, [(KIND_PUT, b"k", b"v")])[:15], "truncated op header"),
    ],
)
def test_batch_decoding_rejects_malformed_payloads(payload: bytes, reason: str) -> None:
    with pytest.raises(CorruptionError, match=reason):
        decode_batch(payload, path="log", offset=0)


def test_write_batch_collects_ops_in_order() -> None:
    batch = WriteBatch()
    batch.put(b"a", b"1")
    batch.delete(bytearray(b"b"))
    batch.put(memoryview(b"c"), b"3")
    assert list(batch) == [(KIND_PUT, b"a", b"1"), (KIND_DELETE, b"b", b""), (KIND_PUT, b"c", b"3")]
    assert len(batch) == 3
    batch.clear()
    assert len(batch) == 0


def test_keys_and_values_must_be_bytes() -> None:
    with pytest.raises(TypeError, match="key must be bytes, not str"):
        as_bytes("key", "text")
    batch = WriteBatch()
    with pytest.raises(TypeError, match="value must be bytes"):
        batch.put(b"k", 5)  # type: ignore[arg-type]


# -- reading logs ------------------------------------------------------------


def test_reads_back_every_record(tmp_path: Path) -> None:
    path = tmp_path / "1.log"
    offsets = write_log(path, PAYLOADS)
    contents = read_log(str(path))
    assert contents.records == list(zip(offsets, PAYLOADS, strict=True))
    assert contents.end == contents.size == path.stat().st_size
    assert not contents.torn


def test_empty_log_has_no_records(tmp_path: Path) -> None:
    path = tmp_path / "1.log"
    path.write_bytes(b"")
    contents = read_log(str(path))
    assert contents.records == []
    assert contents.end == 0


def test_torn_tail_at_every_offset_of_the_last_record(tmp_path: Path) -> None:
    path = tmp_path / "1.log"
    offsets = write_log(path, PAYLOADS)
    full = path.read_bytes()
    last = offsets[-1]
    for cut in range(last, len(full)):
        path.write_bytes(full[:cut])
        contents = read_log(str(path))
        assert [p for _, p in contents.records] == PAYLOADS[:-1], cut
        assert contents.end == last
        assert contents.torn == (cut > last)


@pytest.mark.parametrize("tail", [b"\x00" * 5000, bytes(random.Random(7).randbytes(777))])
def test_zero_filled_or_garbage_tail_is_cut(tmp_path: Path, tail: bytes) -> None:
    path = tmp_path / "1.log"
    write_log(path, PAYLOADS)
    end = path.stat().st_size
    with open(path, "ab") as f:
        f.write(tail)
    contents = read_log(str(path))
    assert [p for _, p in contents.records] == PAYLOADS
    assert contents.end == end
    assert contents.torn


def test_damage_to_any_byte_of_a_middle_record_is_corruption(tmp_path: Path) -> None:
    path = tmp_path / "1.log"
    offsets = write_log(path, PAYLOADS)
    original = path.read_bytes()
    bounds = [*offsets, len(original)]
    for record in range(len(PAYLOADS) - 1):
        for offset in range(bounds[record], bounds[record + 1]):
            path.write_bytes(original)
            flip(path, offset)
            with pytest.raises(CorruptionError, match="middle of the log") as info:
                read_log(str(path))
            assert info.value.offset == bounds[record]
            assert info.value.path == str(path)


def test_damage_to_any_byte_of_the_last_record_drops_it(tmp_path: Path) -> None:
    path = tmp_path / "1.log"
    offsets = write_log(path, PAYLOADS)
    original = path.read_bytes()
    for offset in range(offsets[-1], len(original)):
        path.write_bytes(original)
        flip(path, offset)
        contents = read_log(str(path))
        assert [p for _, p in contents.records] == PAYLOADS[:-1]
        assert contents.end == offsets[-1]


def test_damaged_length_of_a_middle_record_is_not_mistaken_for_a_tail(tmp_path: Path) -> None:
    # A corrupted length that points past the end of the file would look like
    # an incomplete record if the length were trusted without its checksum.
    path = tmp_path / "1.log"
    offsets = write_log(path, PAYLOADS)
    data = bytearray(path.read_bytes())
    data[offsets[1] + 3] = 0x7F
    path.write_bytes(bytes(data))
    with pytest.raises(CorruptionError):
        read_log(str(path))


def test_record_framing() -> None:
    record = encode_record(b"abc")
    assert len(record) == HEADER_SIZE + 3
    assert record[:4] == (3).to_bytes(4, "little")
    assert record[HEADER_SIZE:] == b"abc"


def test_writer_reopens_at_the_intact_end(tmp_path: Path) -> None:
    path = tmp_path / "1.log"
    write_log(path, PAYLOADS[:2])
    size = path.stat().st_size
    writer = LogWriter.reopen(str(path), size)
    writer.append(b"appended")
    writer.close()
    assert [p for _, p in read_log(str(path)).records] == [*PAYLOADS[:2], b"appended"]
    assert os.path.getsize(path) == writer.size
