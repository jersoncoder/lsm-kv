"""The public API, end to end."""

from __future__ import annotations

import gc
import os
import threading
from pathlib import Path

import pytest

from lsm_kv import (
    DB,
    CorruptionError,
    DBClosedError,
    DBFailedError,
    DBLockedError,
    LSMKVError,
    Options,
    SyncPolicy,
    WriteBatch,
    fileops,
)

SMALL = Options(sync=SyncPolicy.ON_CLOSE, memtable_size=2048, block_size=128, compaction_fanout=3)


def tables(path: str) -> list[str]:
    return sorted(name for name in os.listdir(path) if name.endswith(".sst"))


def fill(db: DB, n: int, prefix: bytes = b"key") -> dict[bytes, bytes]:
    data = {prefix + b"%05d" % i: b"value-%d" % i for i in range(n)}
    for key, value in data.items():
        db.put(key, value)
    return data


def test_put_get_delete(db_path: str) -> None:
    with DB.open(db_path) as db:
        assert db.get(b"a") is None
        db.put(b"a", b"1")
        db.put(b"b", b"2")
        db.put(b"a", b"3")
        assert db.get(b"a") == b"3"
        db.delete(b"b")
        db.delete(b"never existed")
        assert db.get(b"b") is None
        assert db.get(b"never existed") is None


def test_empty_key_and_empty_value_are_ordinary(db_path: str) -> None:
    with DB.open(db_path, SMALL) as db:
        db.put(b"", b"empty key")
        db.put(b"k", b"")
        db.flush()
        assert db.get(b"") == b"empty key"
        assert db.get(b"k") == b""
        assert list(db.scan()) == [(b"", b"empty key"), (b"k", b"")]


def test_keys_must_be_bytes(db_path: str) -> None:
    with DB.open(db_path) as db:
        with pytest.raises(TypeError):
            db.put("text", b"v")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            db.get("text")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            db.scan("a")  # type: ignore[arg-type]


def test_scan_is_ordered_and_half_open(db_path: str) -> None:
    with DB.open(db_path, SMALL) as db:
        data = fill(db, 300)
        for key in list(data)[::3]:
            db.delete(key)
            del data[key]
        expected = sorted(data.items())
        assert list(db.scan()) == expected
        assert list(db.scan(b"key00100", b"key00200")) == [
            kv for kv in expected if b"key00100" <= kv[0] < b"key00200"
        ]
        assert list(db.scan(start=b"key00290")) == [kv for kv in expected if kv[0] >= b"key00290"]
        assert list(db.scan(end=b"key00010")) == [kv for kv in expected if kv[0] < b"key00010"]
        assert list(db.scan(b"z")) == []
        assert list(db.scan(b"key00200", b"key00100")) == []


def test_reads_merge_memtable_and_every_tier(db_path: str) -> None:
    with DB.open(db_path, SMALL) as db:
        data = fill(db, 400)
        for i in range(0, 400, 7):
            key = b"key%05d" % i
            db.put(key, b"overwritten")
            data[key] = b"overwritten"
        stats = db.stats()
        assert len(stats.tables_per_tier) >= 2, "the test needs several tiers"
        assert all(count < SMALL.compaction_fanout for count in stats.tables_per_tier.values())
        assert all(db.get(k) == v for k, v in data.items())
        assert list(db.scan()) == sorted(data.items())


def test_reopen_restores_tables_and_log(db_path: str) -> None:
    with DB.open(db_path, SMALL) as db:
        data = fill(db, 250)
        db.delete(b"key00007")
        del data[b"key00007"]
        assert db.stats().flushes > 0
    with DB.open(db_path, SMALL) as db:
        assert list(db.scan()) == sorted(data.items())


def test_write_batch_is_applied_in_order(db_path: str) -> None:
    with DB.open(db_path) as db:
        db.put(b"gone", b"x")
        batch = WriteBatch()
        batch.put(b"k", b"first")
        batch.put(b"k", b"second")
        batch.delete(b"gone")
        batch.put(b"other", b"v")
        db.write(batch)
        assert db.get(b"k") == b"second"
        assert db.get(b"gone") is None
        assert db.get(b"other") == b"v"
        db.write(WriteBatch())  # an empty batch is a no-op


def test_write_batch_block_discards_writes_when_it_raises(db_path: str) -> None:
    with DB.open(db_path) as db:
        with db.write_batch() as batch:
            batch.put(b"a", b"1")
            batch.put(b"b", b"2")
        assert db.get(b"a") == b"1"

        def abort_midway() -> None:
            with db.write_batch() as batch:
                batch.put(b"c", b"3")
                raise RuntimeError("abort")

        with pytest.raises(RuntimeError):
            abort_midway()
        assert db.get(b"c") is None


def test_snapshot_survives_overwrites_flushes_and_compaction(db_path: str) -> None:
    with DB.open(db_path, SMALL) as db:
        before = fill(db, 200)
        snap = db.snapshot()
        for i in range(200):
            if i % 2:
                db.delete(b"key%05d" % i)
            else:
                db.put(b"key%05d" % i, b"new")
        db.put(b"zzz", b"after the snapshot")
        db.compact()
        assert snap.get(b"key00001") == before[b"key00001"]
        assert snap.get(b"zzz") is None
        assert list(snap.scan()) == sorted(before.items())
        assert db.get(b"key00001") is None
        assert db.get(b"key00000") == b"new"
        snap.release()
        with pytest.raises(DBClosedError, match="released"):
            snap.get(b"key00001")


def test_released_snapshot_versions_are_collected(db_path: str) -> None:
    with DB.open(db_path, SMALL) as db:
        db.put(b"k", b"old")
        with db.snapshot() as snap:
            db.put(b"k", b"new")
            db.compact()
            assert snap.get(b"k") == b"old"
            held = db.stats().compaction_bytes
        db.compact()
        assert db.get(b"k") == b"new"
        assert db.stats().compaction_bytes - held < held  # the old version is gone


def test_scan_sees_a_fixed_view_while_the_database_changes(db_path: str) -> None:
    with DB.open(db_path, SMALL) as db:
        data = fill(db, 200)
        it = db.scan()
        first = next(it)
        for i in range(200):
            db.put(b"key%05d" % i, b"changed")
        db.put(b"key", b"new key before the cursor")
        db.put(b"zzz", b"new key after the cursor")
        db.compact()  # deletes every table the iterator is reading
        rest = list(it)
        assert [first, *rest] == sorted(data.items())
        assert len(tables(db_path)) == 1  # retired tables went away with the iterator


def test_abandoned_scan_releases_its_tables(db_path: str) -> None:
    with DB.open(db_path, SMALL) as db:
        fill(db, 200)
        it = db.scan()
        next(it)
        db.compact()
        assert len(tables(db_path)) > 1
        del it
        gc.collect()
        assert len(tables(db_path)) == 1


def test_scan_iterator_as_context_manager(db_path: str) -> None:
    with DB.open(db_path, SMALL) as db:
        fill(db, 50)
        with db.scan() as it:
            assert next(it) == (b"key00000", b"value-0")
        assert list(it) == []


def test_closed_database_refuses_everything(db_path: str) -> None:
    db = DB.open(db_path)
    db.put(b"a", b"1")
    snap = db.snapshot()
    it = db.scan()
    db.close()
    db.close()
    for call in (
        lambda: db.get(b"a"),
        lambda: db.put(b"a", b"2"),
        lambda: db.scan(),
        lambda: db.snapshot(),
        lambda: db.flush(),
        lambda: db.compact(),
        lambda: snap.get(b"a"),
        lambda: next(it),
    ):
        with pytest.raises(DBClosedError):
            call()
    assert "closed" in repr(db)


def test_second_instance_is_locked_out(db_path: str) -> None:
    db = DB.open(db_path)
    with pytest.raises(DBLockedError):
        DB.open(db_path)
    db.close()
    DB.open(db_path).close()


def test_missing_database_without_create_if_missing(tmp_path: Path) -> None:
    with pytest.raises(LSMKVError, match="no database"):
        DB.open(tmp_path / "absent", Options(create_if_missing=False))
    with pytest.raises(LSMKVError, match="no database"):
        DB.open(tmp_path, Options(create_if_missing=False))


def test_data_files_without_a_manifest_are_refused(db_path: str) -> None:
    with DB.open(db_path) as db:
        db.put(b"a", b"1")
    os.remove(os.path.join(db_path, "MANIFEST"))
    with pytest.raises(CorruptionError, match="no MANIFEST"):
        DB.open(db_path)


def test_leftovers_of_interrupted_work_are_removed_on_open(db_path: str) -> None:
    with DB.open(db_path, SMALL) as db:
        fill(db, 100)
        live = set(os.listdir(db_path))
    for name in ("999998.sst", "MANIFEST.tmp", "000000.log"):
        Path(db_path, name).write_bytes(b"junk")
    with DB.open(db_path, SMALL):
        assert set(os.listdir(db_path)) == live


def test_sync_policies_fsync_when_documented(db_path: str, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []
    real_fsync = fileops.fsync

    def counting_fsync(fd: int) -> None:
        calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(fileops, "fsync", counting_fsync)

    def writes_then_close(options: Options, path: str) -> tuple[int, int]:
        db = DB.open(path, options)
        calls.clear()
        for i in range(10):
            db.put(b"k%d" % i, b"v")
        during = len(calls)
        db.close()
        return during, len(calls) - during

    assert writes_then_close(Options(sync=SyncPolicy.EVERY_WRITE), db_path + "1") == (10, 1)
    assert writes_then_close(Options(sync=SyncPolicy.ON_CLOSE), db_path + "2") == (0, 1)
    slow = Options(sync=SyncPolicy.PERIODIC, sync_interval=3600)
    assert writes_then_close(slow, db_path + "3") == (0, 1)
    fast = Options(sync=SyncPolicy.PERIODIC, sync_interval=1e-9)
    assert writes_then_close(fast, db_path + "4") == (10, 1)


def test_explicit_sync(db_path: str, monkeypatch: pytest.MonkeyPatch) -> None:
    with DB.open(db_path, Options(sync=SyncPolicy.ON_CLOSE)) as db:
        db.put(b"a", b"1")
        calls: list[int] = []
        monkeypatch.setattr(fileops, "fsync", calls.append)
        db.sync()
        assert len(calls) == 1


def test_io_error_on_write_stops_further_writes(
    db_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = DB.open(db_path)
    db.put(b"kept", b"1")

    def failing_write(fd: int, data: bytes) -> None:
        raise OSError(28, "No space left on device")

    real_write = fileops.write
    monkeypatch.setattr(fileops, "write", failing_write)
    with pytest.raises(OSError, match="No space"):
        db.put(b"lost", b"2")
    monkeypatch.setattr(fileops, "write", real_write)
    with pytest.raises(DBFailedError):
        db.put(b"other", b"3")
    assert db.get(b"kept") == b"1"  # reads still work
    assert db.get(b"lost") is None
    db.close()
    with DB.open(db_path) as db:
        assert db.get(b"kept") == b"1"
        db.put(b"other", b"3")


def test_io_error_during_flush_stops_further_writes(
    db_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = DB.open(db_path, SMALL)
    data = fill(db, 20)

    def failing_rename(src: str, dst: str) -> None:
        raise OSError(5, "Input/output error")

    monkeypatch.setattr(fileops, "rename", failing_rename)
    with pytest.raises(OSError, match="Input/output"):
        db.flush()
    with pytest.raises(DBFailedError):
        db.put(b"x", b"y")
    db.close()
    monkeypatch.undo()
    with DB.open(db_path, SMALL) as db:
        assert list(db.scan()) == sorted(data.items())


def test_concurrent_writers(db_path: str) -> None:
    with DB.open(db_path, SMALL) as db:

        def writer(n: int) -> None:
            for i in range(300):
                db.put(b"t%d-%04d" % (n, i), b"%d" % i)

        threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(list(db.scan())) == 1200


def test_stats_track_bytes_written(db_path: str) -> None:
    with DB.open(db_path, SMALL) as db:
        fill(db, 500)
        stats = db.stats()
    assert stats.user_bytes == sum(len(b"key%05d" % i) + len(b"value-%d" % i) for i in range(500))
    assert stats.flushes > 0
    assert stats.compactions > 0
    assert stats.wal_bytes > stats.user_bytes
    assert stats.write_amplification > 1
