from __future__ import annotations

from pathlib import Path

import pytest

from lsm_kv import DBClosedError
from lsm_kv.entry import KIND_DELETE, KIND_PUT, Entry
from lsm_kv.sstable import TableReader, TableWriter


def sample_entries() -> list[Entry]:
    """Keys k000..k199 with several versions each and some tombstones."""
    entries: list[Entry] = []
    seq = 1000
    for i in range(200):
        key = f"k{i:03d}".encode()
        for version in range(i % 3 + 1):
            if (i + version) % 7 == 0:
                entries.append((key, seq, KIND_DELETE, b""))
            else:
                entries.append((key, seq, KIND_PUT, f"{key.decode()}-{seq}".encode()))
            seq -= 1
        seq += 10  # keep seqs unique across keys while descending within a key
    return entries


def write_table(path: Path, entries: list[Entry], block_size: int = 256) -> TableReader:
    writer = TableWriter(str(path), block_size=block_size, bloom_bits_per_key=10)
    for entry in entries:
        writer.add(*entry)
    writer.finish()
    return TableReader(str(path))


def test_round_trip_across_many_blocks(tmp_path: Path) -> None:
    entries = sample_entries()
    reader = write_table(tmp_path / "t.sst", entries)
    assert reader.block_count > 10
    assert list(reader.entries()) == entries
    props = reader.properties
    assert props.entry_count == len(entries)
    assert props.smallest == b"k000"
    assert props.largest == b"k199"
    assert props.min_seq == min(e[1] for e in entries)
    assert props.max_seq == max(e[1] for e in entries)
    assert reader.file_size == (tmp_path / "t.sst").stat().st_size
    reader.close()


def test_get_finds_the_newest_version_at_or_below_a_sequence_number(tmp_path: Path) -> None:
    entries = sample_entries()
    reader = write_table(tmp_path / "t.sst", entries)
    by_key: dict[bytes, list[Entry]] = {}
    for entry in entries:
        by_key.setdefault(entry[0], []).append(entry)
    for key, versions in by_key.items():
        newest = versions[0]
        assert reader.get(key, newest[1]) == newest[1:]
        assert reader.get(key, newest[1] + 1000) == newest[1:]
        oldest = versions[-1]
        assert reader.get(key, oldest[1]) == oldest[1:]
        assert reader.get(key, oldest[1] - 1) is None
    assert reader.get(b"k0005", 10**9) is None  # between two keys
    assert reader.get(b"a", 10**9) is None  # before the first key
    assert reader.get(b"z", 10**9) is None  # after the last key
    reader.close()


def test_entries_respect_bounds(tmp_path: Path) -> None:
    entries = sample_entries()
    reader = write_table(tmp_path / "t.sst", entries)
    got = list(reader.entries(b"k050", b"k060"))
    assert got == [e for e in entries if b"k050" <= e[0] < b"k060"]
    assert list(reader.entries(b"k1995")) == []
    assert list(reader.entries(None, b"k000")) == []
    reader.close()


def test_value_larger_than_a_block(tmp_path: Path) -> None:
    big = bytes(range(256)) * 40
    entries: list[Entry] = [(b"a", 3, KIND_PUT, b"small"), (b"b", 2, KIND_PUT, big)]
    reader = write_table(tmp_path / "t.sst", entries, block_size=64)
    assert reader.get(b"b", 10) == (2, KIND_PUT, big)
    assert list(reader.entries()) == entries
    reader.close()


def test_bloom_filter_screens_absent_keys(tmp_path: Path) -> None:
    reader = write_table(tmp_path / "t.sst", sample_entries())
    absent = [f"missing{i}".encode() for i in range(2000)]
    assert sum(reader.may_contain(k) for k in absent) < 60
    reader.close()


def test_writer_rejects_entries_out_of_order(tmp_path: Path) -> None:
    writer = TableWriter(str(tmp_path / "t.sst"), block_size=64, bloom_bits_per_key=10)
    writer.add(b"b", 5, KIND_PUT, b"")
    with pytest.raises(ValueError, match="increasing"):
        writer.add(b"a", 9, KIND_PUT, b"")
    with pytest.raises(ValueError, match="increasing"):
        writer.add(b"b", 6, KIND_PUT, b"")  # same key, newer version after an older one
    writer.abandon()
    assert not (tmp_path / "t.sst").exists()


def test_writer_refuses_an_empty_table(tmp_path: Path) -> None:
    writer = TableWriter(str(tmp_path / "t.sst"), block_size=64, bloom_bits_per_key=10)
    with pytest.raises(ValueError, match="at least one entry"):
        writer.finish()
    writer.abandon()


def test_closed_reader_refuses_reads(tmp_path: Path) -> None:
    reader = write_table(tmp_path / "t.sst", sample_entries())
    reader.close()
    reader.close()
    with pytest.raises(DBClosedError):
        reader.get(b"k001", 10**9)
