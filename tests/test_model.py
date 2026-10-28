"""Model-based randomized test: the database must behave exactly like a dict.

Thousands of seeded random operations (puts, deletes, batches, snapshot reads,
range scans, flushes, compactions, close and reopen) run against the database
and a plain dict side by side. After every step the keys the step touched and
one random key are compared; the full contents are compared every few steps,
after every flush, compaction and reopen, and at the end. Every live snapshot
is compared against a copy of the dict taken when the snapshot was created.

Small memtables, blocks and fanouts make flushes and multi-tier compactions
happen every few dozen operations. A failure message names the seed and step,
which is enough to replay the run.
"""

from __future__ import annotations

import random

import pytest

from lsm_kv import DB, Options, Snapshot, SyncPolicy, WriteBatch

# Short keys over a tiny alphabet: many collisions, many keys that are
# prefixes of others, and the extreme byte values 0x00 and 0xff.
ALPHABET = b"ab\x00\xff"
KEYS = [b""] + [
    bytes(ALPHABET[(i // len(ALPHABET) ** d) % len(ALPHABET)] for d in range(n))
    for n in range(1, 5)
    for i in range(len(ALPHABET) ** n)
]

PROFILES = {
    "tiny-tables": Options(
        sync=SyncPolicy.ON_CLOSE, memtable_size=1024, block_size=48, compaction_fanout=2
    ),
    "every-write": Options(
        sync=SyncPolicy.EVERY_WRITE,
        memtable_size=4096,
        block_size=256,
        compaction_fanout=3,
        bloom_bits_per_key=3,
    ),
    "periodic": Options(
        sync=SyncPolicy.PERIODIC,
        sync_interval=0.001,
        memtable_size=2048,
        block_size=128,
        compaction_fanout=4,
    ),
}

STEPS = 2500
FULL_CHECK_EVERY = 25
MAX_SNAPSHOTS = 4


class Harness:
    def __init__(self, path: str, options: Options, seed: int):
        self.path = path
        self.options = options
        self.rng = random.Random(seed)
        self.seed = seed
        self.step_number = 0
        self.db = DB.open(path, options)
        self.model: dict[bytes, bytes] = {}
        self.snapshots: list[tuple[Snapshot, dict[bytes, bytes]]] = []

    def where(self) -> str:
        return f"seed={self.seed} step={self.step_number}"

    def key(self) -> bytes:
        return self.rng.choice(KEYS)

    def value(self) -> bytes:
        size = 300 if self.rng.random() < 0.02 else self.rng.randrange(0, 40)
        return self.rng.randbytes(size)

    # -- checks -----------------------------------------------------------------

    def check_keys(self, keys: list[bytes]) -> None:
        for key in keys:
            assert self.db.get(key) == self.model.get(key), f"get({key!r}) {self.where()}"

    def check_everything(self) -> None:
        assert list(self.db.scan()) == sorted(self.model.items()), f"full scan {self.where()}"
        for snap, frozen in self.snapshots:
            assert list(snap.scan()) == sorted(frozen.items()), f"snapshot scan {self.where()}"

    # -- operations -------------------------------------------------------------

    def op_put(self) -> list[bytes]:
        key, value = self.key(), self.value()
        self.db.put(key, value)
        self.model[key] = value
        return [key]

    def op_delete(self) -> list[bytes]:
        key = self.key()
        self.db.delete(key)
        self.model.pop(key, None)
        return [key]

    def op_batch(self) -> list[bytes]:
        batch = WriteBatch()
        staged = dict(self.model)
        touched = []
        for _ in range(self.rng.randrange(1, 9)):
            key = self.key()
            touched.append(key)
            if self.rng.random() < 0.7:
                value = self.value()
                batch.put(key, value)
                staged[key] = value
            else:
                batch.delete(key)
                staged.pop(key, None)
        self.db.write(batch)
        self.model = staged
        return touched

    def op_scan(self) -> list[bytes]:
        start, end = sorted((self.key(), self.key()))
        if self.rng.random() < 0.2:
            start = b""
        expected = sorted((k, v) for k, v in self.model.items() if start <= k < end)
        assert list(self.db.scan(start, end)) == expected, f"scan {self.where()}"
        return []

    def op_snapshot(self) -> list[bytes]:
        if len(self.snapshots) < MAX_SNAPSHOTS:
            self.snapshots.append((self.db.snapshot(), dict(self.model)))
        return []

    def op_read_snapshot(self) -> list[bytes]:
        if self.snapshots:
            snap, frozen = self.rng.choice(self.snapshots)
            for _ in range(5):
                key = self.key()
                assert snap.get(key) == frozen.get(key), f"snapshot get {self.where()}"
        return []

    def op_release_snapshot(self) -> list[bytes]:
        if self.snapshots:
            snap, _ = self.snapshots.pop(self.rng.randrange(len(self.snapshots)))
            snap.release()
        return []

    def op_flush(self) -> list[bytes]:
        self.db.flush()
        self.check_everything()
        return []

    def op_compact(self) -> list[bytes]:
        self.db.compact()
        self.check_everything()
        return []

    def op_reopen(self) -> list[bytes]:
        self.db.close()
        self.snapshots.clear()  # snapshots do not survive a close
        self.db = DB.open(self.path, self.options)
        self.check_everything()
        return []

    def run(self, steps: int) -> None:
        operations = [
            (self.op_put, 40),
            (self.op_delete, 15),
            (self.op_batch, 10),
            (self.op_scan, 8),
            (self.op_snapshot, 3),
            (self.op_read_snapshot, 8),
            (self.op_release_snapshot, 2),
            (self.op_flush, 1),
            (self.op_compact, 1),
            (self.op_reopen, 1),
        ]
        functions = [op for op, _ in operations]
        weights = [weight for _, weight in operations]
        for step in range(steps):
            self.step_number = step
            operation = self.rng.choices(functions, weights)[0]
            touched = operation()
            self.check_keys([*touched, self.key()])
            if self.step_number % FULL_CHECK_EVERY == 0:
                self.check_everything()
        self.check_everything()
        stats = self.db.stats()
        assert all(n < self.options.compaction_fanout for n in stats.tables_per_tier.values())
        self.db.close()


@pytest.mark.usefixtures("no_fsync")
@pytest.mark.parametrize("profile", sorted(PROFILES))
@pytest.mark.parametrize("seed", [1, 2])
def test_database_matches_a_dict(db_path: str, profile: str, seed: int) -> None:
    Harness(db_path, PROFILES[profile], seed).run(STEPS)


@pytest.mark.usefixtures("no_fsync")
def test_harness_exercises_flushes_compactions_and_tiers(db_path: str) -> None:
    """Guard against the randomized test silently testing only the memtable."""
    harness = Harness(db_path, PROFILES["tiny-tables"], seed=3)
    for _ in range(600):
        harness.op_put()
    stats = harness.db.stats()
    assert stats.flushes >= 10
    assert stats.compactions >= 5
    assert len(stats.tables_per_tier) >= 2
    harness.db.close()
