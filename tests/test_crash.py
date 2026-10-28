"""Crash tests: recovery after a crash at every point of a recorded workload.

A workload runs once under :class:`crashsim.Recorder`. It writes enough to
trigger many flushes and multi-tier compactions, then runs an explicit flush,
an explicit full compaction and a last series of writes. Each test rebuilds
the directory as a crash at some recorded operation (and, for the writes that
matter most, at every byte offset inside the write) would leave it, opens it,
and compares the contents with the dict model.

A crash keeps exactly the writes whose log record reached the file complete,
so after a process crash recovery must produce the model after that many
writes: never a partial batch, never a mix. After a power failure the result
must still be the model after some prefix of the writes, and under
``SyncPolicy.EVERY_WRITE`` that prefix must include every write acknowledged
before the crash.
"""

from __future__ import annotations

import os
import random
from bisect import bisect_right
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest
from crashsim import History, Recorder, materialize

from lsm_kv import DB, Options, SyncPolicy, WriteBatch

pytestmark = pytest.mark.usefixtures("no_fsync")

TAIL_WRITES = 8


@dataclass
class Run:
    recorder: Recorder
    options: Options
    models: list[dict[bytes, bytes]]
    """``models[i]`` is the expected contents after ``i`` acknowledged writes."""
    acked_at: list[int]
    """``acked_at[i]`` is the number of recorded operations when write ``i + 1`` returned."""
    flush: range
    compaction: range
    tail: range

    @property
    def ops(self) -> int:
        return len(self.recorder.ops)

    def written(self, k: int) -> int:
        """Writes whose log record was complete before operation ``k``."""
        return self.recorder.log_records_before(k)

    def acknowledged(self, k: int) -> int:
        """Writes that had returned to the caller before operation ``k``."""
        return bisect_right(self.acked_at, k)


def run_workload(path: str, sync: SyncPolicy) -> Run:
    opts = Options(sync=sync, memtable_size=700, block_size=96, compaction_fanout=2)
    rng = random.Random(2024)
    model: dict[bytes, bytes] = {}
    models: list[dict[bytes, bytes]] = [{}]
    acked_at: list[int] = []
    os.makedirs(path)
    # Recording starts before the database is created, so crashes during
    # creation are covered too.
    with Recorder(path) as recorder:
        db = DB.open(path, opts)

        def write() -> None:
            batch = WriteBatch()
            for _ in range(rng.choice([1, 1, 1, 3])):
                key = b"key-%02d" % rng.randrange(40)
                if rng.random() < 0.25:
                    batch.delete(key)
                    model.pop(key, None)
                else:
                    value = rng.randbytes(rng.randrange(4, 24))
                    batch.put(key, value)
                    model[key] = value
            db.write(batch)
            models.append(dict(model))
            acked_at.append(len(recorder.ops))

        def last_writes() -> None:
            for _ in range(TAIL_WRITES):
                write()

        def recorded(action: Callable[[], None]) -> range:
            start = len(recorder.ops)
            action()
            return range(start, len(recorder.ops))

        for _ in range(90):
            write()
        flush = recorded(db.flush)
        for _ in range(20):
            write()
        compaction = recorded(db.compact)
        tail = recorded(last_writes)
        db.close()
    return Run(recorder, opts, models, acked_at, flush, compaction, tail)


def recover(run: Run, files: dict[str, bytes], directory: Path) -> dict[bytes, bytes]:
    with DB.open(materialize(files, directory), run.options) as db:
        return dict(db.scan())


def assert_writable(run: Run, directory: Path, expected: dict[bytes, bytes]) -> None:
    """The recovered log must accept new records that survive another reopen."""
    with DB.open(directory, run.options) as db:
        db.put(b"after-recovery", b"ok")
    with DB.open(directory, run.options) as db:
        assert dict(db.scan()) == {**expected, b"after-recovery": b"ok"}


def matching_prefixes(run: Run, recovered: dict[bytes, bytes], lo: int, hi: int) -> list[int]:
    return [j for j in range(lo, hi + 1) if run.models[j] == recovered]


@pytest.fixture
def run(tmp_path: Path) -> Run:
    return run_workload(str(tmp_path / "db"), SyncPolicy.EVERY_WRITE)


def test_workload_covers_flushes_compactions_and_cleanup(run: Run) -> None:
    kinds = [(op.kind, op.name.rsplit(".", 1)[-1]) for op in run.recorder.ops]
    assert kinds.count(("create", "sst")) >= 10
    assert kinds.count(("rename", "tmp")) >= 10  # manifest commits
    assert kinds.count(("remove", "sst")) >= 5  # tables retired by compaction
    assert kinds.count(("remove", "log")) >= 5  # logs retired by flushes
    assert len(run.models) == 90 + 20 + TAIL_WRITES + 1


def test_process_crash_before_every_operation(run: Run, tmp_path: Path) -> None:
    for k in range(run.ops + 1):
        recovered = recover(run, run.recorder.process_crash(k), tmp_path / "state")
        assert recovered == run.models[run.written(k)], f"crash before op {k}"


def test_process_crash_inside_each_of_the_last_log_records(run: Run, tmp_path: Path) -> None:
    log_writes = run.recorder.write_ops(run.tail.start, run.tail.stop)
    assert len(log_writes) == TAIL_WRITES
    for k in log_writes:
        for cut in range(1, len(run.recorder.ops[k].data)):
            state = tmp_path / "state"
            expected = run.models[run.written(k)]
            assert recover(run, run.recorder.process_crash(k, cut), state) == expected, (k, cut)
            assert_writable(run, state, expected)


@pytest.mark.parametrize("phase", ["flush", "compaction"])
def test_process_crash_inside_table_and_manifest_writes(
    run: Run, tmp_path: Path, phase: str
) -> None:
    span: range = getattr(run, phase)
    writes = run.recorder.write_ops(span.start, span.stop)
    names = {run.recorder.ops[k].name.rsplit(".", 1)[-1] for k in writes}
    assert {"sst", "tmp"} <= names  # table blocks and the new manifest
    for k in writes:
        for cut in range(1, len(run.recorder.ops[k].data)):
            recovered = recover(run, run.recorder.process_crash(k, cut), tmp_path / "state")
            assert recovered == run.models[run.written(k)], (phase, k, cut)


def test_power_failure_keeps_every_acknowledged_write(run: Run, tmp_path: Path) -> None:
    states = 0
    for k in range(run.ops + 1):
        for files in run.recorder.power_failures(k):
            recovered = recover(run, files, tmp_path / "state")
            lo, hi = run.acknowledged(k), run.written(k)
            assert matching_prefixes(run, recovered, lo, hi), f"power failure before op {k}"
            states += 1
    assert states > run.ops  # some states had unsynced directory operations


def test_power_failure_without_fsync_per_write_keeps_a_prefix(tmp_path: Path) -> None:
    run = run_workload(str(tmp_path / "db"), SyncPolicy.ON_CLOSE)
    lost_acknowledged_writes = False
    for k in range(run.ops + 1):
        for files in run.recorder.power_failures(k):
            recovered = recover(run, files, tmp_path / "state")
            prefixes = matching_prefixes(run, recovered, 0, run.written(k))
            assert prefixes, f"power failure before op {k}"
            lost_acknowledged_writes |= max(prefixes) < run.acknowledged(k)
    # This is the documented cost of not syncing every write.
    assert lost_acknowledged_writes

    # A file system may also have written any part of the unsynced log tail.
    # Crash just before close() fsyncs the log, and try every length of it.
    ops = run.recorder.ops
    log = max(op.name for op in ops if op.name.endswith(".log"))
    k = max(i for i, op in enumerate(ops) if op.kind == "fsync" and op.name == log)
    full = run.recorder.process_crash(k)[log]
    synced = run.recorder.power_failure(k)[log]
    assert len(full) - len(synced) > 100, "the tail sweep needs unsynced records"
    for extra in range(len(full) - len(synced) + 1):
        files = run.recorder.power_failure(k, keep={log: extra})
        recovered = recover(run, files, tmp_path / "state")
        assert matching_prefixes(run, recovered, 0, run.written(k)), extra


def test_power_failure_after_recovering_from_a_process_crash(run: Run, tmp_path: Path) -> None:
    """A process crash, a recovery that acknowledges a new write, then a power failure.

    Recovery can find directory changes that the crashed process made but no
    directory fsync covered, such as a log it had just created. It must make
    them durable before relying on them: otherwise the power failure can roll
    them back underneath a write acknowledged after recovery.
    """
    for k in range(run.ops + 1):
        second_dir = tmp_path / "second"
        materialize(run.recorder.process_crash(k), second_dir)
        with Recorder(str(second_dir)) as second, DB.open(second_dir, run.options) as db:
            db.put(b"acked", b"after recovery")
        history = History(run.recorder.base, run.recorder.ops[:k] + second.ops)
        for files in history.power_failures(len(history.ops)):
            recovered = recover(run, files, tmp_path / "state")
            assert recovered.pop(b"acked", None) == b"after recovery", f"crash before op {k}"
            # The second session saw these writes before acknowledging its own.
            assert recovered == run.models[run.written(k)], f"crash before op {k}"
