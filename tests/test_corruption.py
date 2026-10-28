"""Corruption tests: damaged bytes are reported, never returned as data.

Every block of a table is checksummed, so flipping any byte of a table must
make opening it, or reading the damaged block, raise CorruptionError. The
log and the manifest are checked the same way through ``DB.open``, which must
also leave the directory untouched when it refuses to open.
"""

from __future__ import annotations

import contextlib
import os
import struct
import zlib
from pathlib import Path

import pytest

from lsm_kv import (
    DB,
    CorruptionError,
    DBFailedError,
    Options,
    SyncPolicy,
    UnsupportedFormatError,
)
from lsm_kv.batch import encode_batch
from lsm_kv.entry import KIND_DELETE, KIND_PUT, Entry
from lsm_kv.sstable import FOOTER_SIZE, TableReader, TableWriter
from lsm_kv.wal import LogWriter, read_log


def sample_entry(i: int) -> Entry:
    if i % 9 == 0:
        return (b"k%03d" % i, 100 - i, KIND_DELETE, b"")
    return (b"k%03d" % i, 100 - i, KIND_PUT, b"value %d" % i)


ENTRIES = [sample_entry(i) for i in range(40)]


def build_table(path: Path) -> bytes:
    writer = TableWriter(str(path), block_size=128, bloom_bits_per_key=10)
    for entry in ENTRIES:
        writer.add(*entry)
    writer.finish()
    return path.read_bytes()


def layout(path: Path) -> list[tuple[str, int, int]]:
    reader = TableReader(str(path))
    try:
        return reader.blocks()
    finally:
        reader.close()


def damage(path: Path, original: bytes, offset: int, mask: int = 0xFF) -> None:
    data = bytearray(original)
    data[offset] ^= mask
    path.write_bytes(bytes(data))


def snapshot_files(directory: str) -> dict[str, bytes]:
    return {
        name: Path(directory, name).read_bytes()
        for name in sorted(os.listdir(directory))
        if name != "LOCK"
    }


# -- tables -------------------------------------------------------------------


def test_layout_covers_the_whole_file(tmp_path: Path) -> None:
    path = tmp_path / "t.sst"
    data = build_table(path)
    blocks = layout(path)
    kinds = [kind for kind, _, _ in blocks]
    assert kinds[-4:] == ["filter", "properties", "index", "footer"]
    assert kinds.count("data") >= 3
    position = 0
    for _, offset, size in blocks:
        assert offset == position
        position += size
    assert position == len(data)


@pytest.mark.parametrize("mask", [0x01, 0x80, 0xFF])
def test_any_damaged_byte_in_a_table_is_detected(tmp_path: Path, mask: int) -> None:
    path = tmp_path / "t.sst"
    original = build_table(path)
    expected = {e[0]: e for e in ENTRIES}
    for offset in range(len(original)):
        damage(path, original, offset, mask)
        try:
            reader = TableReader(str(path))
        except CorruptionError:
            continue
        try:
            # Point reads return the right entry or raise; never another entry.
            for key, (_, seq, kind, value) in expected.items():
                with contextlib.suppress(CorruptionError):
                    assert reader.get(key, 10**6) == (seq, kind, value), (offset, key)
            # A full scan reads every data block, so it must hit the damage.
            with pytest.raises(CorruptionError):
                list(reader.entries())
        finally:
            reader.close()


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("filter", "filter block checksum mismatch"),
        ("properties", "properties block checksum mismatch"),
        ("index", "index block checksum mismatch"),
        ("footer", "table footer checksum mismatch"),
    ],
)
def test_damaged_metadata_is_reported_on_open(tmp_path: Path, kind: str, message: str) -> None:
    path = tmp_path / "t.sst"
    original = build_table(path)
    offset = next(o for k, o, _ in layout(path) if k == kind)
    damage(path, original, offset + 1)
    with pytest.raises(CorruptionError, match=message) as info:
        TableReader(str(path))
    assert info.value.path == str(path)
    assert info.value.offset == offset


def test_damaged_data_block_is_reported_when_read(tmp_path: Path) -> None:
    path = tmp_path / "t.sst"
    original = build_table(path)
    _, offset, _ = [b for b in layout(path) if b[0] == "data"][1]
    damage(path, original, offset + 10)
    reader = TableReader(str(path))  # data blocks are verified on read
    assert reader.get(ENTRIES[0][0], 10**6) == ENTRIES[0][1:]  # first block is intact
    with pytest.raises(CorruptionError, match="data block checksum mismatch") as info:
        list(reader.entries())
    assert info.value.offset == offset
    reader.close()


def test_wrong_magic_and_truncation(tmp_path: Path) -> None:
    path = tmp_path / "t.sst"
    original = build_table(path)
    damage(path, original, len(original) - 1)
    with pytest.raises(CorruptionError, match="bad magic number"):
        TableReader(str(path))
    for length in (0, FOOTER_SIZE - 1):
        path.write_bytes(original[:length])
        with pytest.raises(CorruptionError, match="shorter than a table footer"):
            TableReader(str(path))
    path.write_bytes(original[:-100])
    with pytest.raises(CorruptionError):
        TableReader(str(path))


def test_newer_table_version_is_unsupported(tmp_path: Path) -> None:
    path = tmp_path / "t.sst"
    original = bytearray(build_table(path))
    body_start = len(original) - FOOTER_SIZE
    body_end = body_start + 40
    struct.pack_into("<I", original, body_end - 4, 2)  # format version field
    struct.pack_into("<I", original, body_end, zlib.crc32(original[body_start:body_end]))
    path.write_bytes(bytes(original))
    with pytest.raises(UnsupportedFormatError, match="version 2"):
        TableReader(str(path))


# -- through the database -------------------------------------------------------

OPTIONS = Options(sync=SyncPolicy.ON_CLOSE, memtable_size=1024, block_size=128)


def make_db(path: str) -> dict[bytes, bytes]:
    data = {b"key%03d" % i: b"value %d" % i for i in range(60)}
    with DB.open(path, OPTIONS) as db:
        for key, value in data.items():
            db.put(key, value)
    return data


def files_of(path: str, suffix: str) -> list[str]:
    return sorted(os.path.join(path, n) for n in os.listdir(path) if n.endswith(suffix))


def test_damaged_log_record_in_the_middle_stops_open(db_path: str) -> None:
    make_db(db_path)
    (log,) = files_of(db_path, ".log")
    offsets = [offset for offset, _ in read_log(log).records]
    assert len(offsets) >= 3
    original = Path(log).read_bytes()
    damage(Path(log), original, offsets[1] + 14)
    before = snapshot_files(db_path)
    with pytest.raises(CorruptionError, match="middle of the log") as info:
        DB.open(db_path, OPTIONS)
    assert info.value.path == log
    assert info.value.offset == offsets[1]
    assert snapshot_files(db_path) == before  # a failed open changes nothing


def test_damaged_last_log_record_is_dropped(db_path: str) -> None:
    data = make_db(db_path)
    (log,) = files_of(db_path, ".log")
    last_offset = read_log(log).records[-1][0]
    damage(Path(log), Path(log).read_bytes(), last_offset + 20)
    with DB.open(db_path, OPTIONS) as db:
        assert dict(db.scan()) == dict(list(data.items())[:-1])
    assert os.path.getsize(log) == last_offset  # the damaged tail was cut


def test_sequence_gap_in_the_log_stops_open(db_path: str) -> None:
    make_db(db_path)
    (log,) = files_of(db_path, ".log")
    writer = LogWriter.reopen(log, os.path.getsize(log))
    writer.append(encode_batch(10**6, [(KIND_PUT, b"k", b"v")]))
    writer.close()
    with pytest.raises(CorruptionError, match="sequence number 1000000"):
        DB.open(db_path, OPTIONS)


@pytest.mark.parametrize("kind", ["filter", "properties", "index", "footer"])
def test_damaged_table_metadata_stops_open(db_path: str, kind: str) -> None:
    make_db(db_path)
    table = Path(files_of(db_path, ".sst")[0])
    offset = next(o for k, o, _ in layout(table) if k == kind)
    damage(table, table.read_bytes(), offset + 2)
    before = snapshot_files(db_path)
    with pytest.raises(CorruptionError, match="checksum mismatch"):
        DB.open(db_path, OPTIONS)
    assert snapshot_files(db_path) == before


def test_damaged_data_block_fails_reads_and_compaction(db_path: str) -> None:
    data = make_db(db_path)
    table = Path(files_of(db_path, ".sst")[0])
    _, offset, _ = next(b for b in layout(table) if b[0] == "data")
    damage(table, table.read_bytes(), offset + 5)
    with DB.open(db_path, OPTIONS) as db:
        first_key = min(data)
        with pytest.raises(CorruptionError, match="data block"):
            db.get(first_key)
        with pytest.raises(CorruptionError, match="data block"):
            list(db.scan())
        db.flush()  # so that compact() below has only the merge left to do
        manifest = Path(db_path, "MANIFEST").read_bytes()
        tables = files_of(db_path, ".sst")
        with pytest.raises(CorruptionError, match="data block"):
            db.compact()
        assert Path(db_path, "MANIFEST").read_bytes() == manifest  # nothing committed
        assert files_of(db_path, ".sst") == tables  # the partial output was removed
        with pytest.raises(DBFailedError):
            db.put(b"k", b"v")


def test_missing_or_resized_table_stops_open(db_path: str) -> None:
    make_db(db_path)
    table = files_of(db_path, ".sst")[0]
    original = Path(table).read_bytes()
    with open(table, "ab") as f:
        f.write(b"extra")
    with pytest.raises(CorruptionError):
        DB.open(db_path, OPTIONS)
    os.remove(table)
    with pytest.raises(CorruptionError, match="missing"):
        DB.open(db_path, OPTIONS)
    Path(table).write_bytes(original)
    DB.open(db_path, OPTIONS).close()


def test_damaged_manifest_stops_open(db_path: str) -> None:
    make_db(db_path)
    manifest = Path(db_path, "MANIFEST")
    original = manifest.read_bytes()
    for offset in range(0, len(original), 7):
        damage(manifest, original, offset)
        before = snapshot_files(db_path)
        with pytest.raises(CorruptionError, match="manifest"):
            DB.open(db_path, OPTIONS)
        assert snapshot_files(db_path) == before
    manifest.write_bytes(original[: len(original) // 2])
    with pytest.raises(CorruptionError, match="manifest"):
        DB.open(db_path, OPTIONS)


def test_torn_log_that_is_not_the_newest_stops_open(db_path: str) -> None:
    # Only the newest log can legitimately end in a torn record: a flush
    # fsyncs the old log before it creates the next one.
    make_db(db_path)
    (log,) = files_of(db_path, ".log")
    os.truncate(log, os.path.getsize(log) - 3)
    Path(db_path, "999999.log").write_bytes(b"")
    with pytest.raises(CorruptionError, match="not the newest"):
        DB.open(db_path, OPTIONS)
