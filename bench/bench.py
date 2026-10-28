"""Measure throughput and write amplification of lsm-kv on this machine.

Each workload runs in a fresh database under ``--dir`` (a temporary directory
by default, which on some systems is a RAM-backed tmpfs where fsync is free;
pass a directory on a real disk to measure durable writes). The whole set runs
``--repeat`` times and the median time of each workload is reported, as a
Markdown table.

    python bench/bench.py --dir /path/on/disk --count 100000
"""

from __future__ import annotations

import argparse
import os
import platform
import random
import shutil
import statistics
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass

from lsm_kv import DB, Options, SyncPolicy


@dataclass
class Result:
    name: str
    ops: int
    seconds: float
    note: str = ""

    @property
    def rate(self) -> float:
        return self.ops / self.seconds


def key(i: int) -> bytes:
    return b"%016d" % i


def timed(name: str, ops: int, action: Callable[[], None], note: str = "") -> Result:
    start = time.perf_counter()
    action()
    return Result(name, ops, time.perf_counter() - start, note)


def run(directory: str, count: int, value_size: int, sync_count: int, seed: int) -> list[Result]:
    rng = random.Random(seed)
    pool = rng.randbytes(value_size + 4096)

    def value(i: int) -> bytes:
        offset = (i * 7919) % 4096
        return pool[offset : offset + value_size]

    fast = Options(sync=SyncPolicy.ON_CLOSE)
    results = []

    with DB.open(os.path.join(directory, "seq"), fast) as db:

        def fill_sequential() -> None:
            for i in range(count):
                db.put(key(i), value(i))

        results.append(timed("fill sequential keys", count, fill_sequential, "ON_CLOSE"))

    random_keys = [rng.randrange(count) for _ in range(count)]
    db = DB.open(os.path.join(directory, "random"), fast)

    def fill_random() -> None:
        for i, k in enumerate(random_keys):
            db.put(key(k), value(i))

    results.append(timed("fill random keys", count, fill_random, "ON_CLOSE"))
    stats = db.stats()
    tables = stats.flush_bytes + stats.compaction_bytes
    results[-1].note += (
        f"; write amplification {stats.write_amplification:.2f}"
        f" (tables alone {tables / stats.user_bytes:.2f}), tiers {stats.tables_per_tier}"
    )

    lookups = [key(rng.choice(random_keys)) for _ in range(count)]

    def read_present() -> None:
        for k in lookups:
            if db.get(k) is None:
                raise AssertionError(k)

    results.append(timed("get existing keys", count, read_present))

    # Absent keys that sort between present ones, so that every table's key
    # range admits them and only the Bloom filter can rule them out.
    missing = [key(rng.randrange(count)) + b"~" for _ in range(count)]

    def read_missing() -> None:
        for k in missing:
            if db.get(k) is not None:
                raise AssertionError(k)

    results.append(timed("get missing keys", count, read_missing, "Bloom filters"))

    live = len(set(random_keys))

    def scan_all() -> None:
        if sum(1 for _ in db.scan()) != live:
            raise AssertionError("scan count")

    results.append(timed("full scan", live, scan_all, "entries"))
    db.close()

    with DB.open(os.path.join(directory, "sync"), Options(sync=SyncPolicy.EVERY_WRITE)) as db:

        def fill_synced() -> None:
            for i in range(sync_count):
                db.put(key(i), value(i))

        results.append(timed("fill, fsync every write", sync_count, fill_synced, "EVERY_WRITE"))
    return results


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dir", help="parent directory for the databases (default: a temp dir)")
    parser.add_argument("--count", type=int, default=100_000, help="operations per workload")
    parser.add_argument("--value-size", type=int, default=100, help="value size in bytes")
    parser.add_argument("--sync-count", type=int, default=2000, help="writes in the fsync run")
    parser.add_argument("--repeat", type=int, default=3, help="runs to take the median of")
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args(argv)

    runs = []
    for _ in range(args.repeat):
        directory = tempfile.mkdtemp(prefix="lsm-kv-bench-", dir=args.dir)
        try:
            runs.append(run(directory, args.count, args.value_size, args.sync_count, args.seed))
        finally:
            shutil.rmtree(directory)
    results = []
    for column in zip(*runs, strict=True):
        first = column[0]  # the workloads are seeded, so names, counts and notes agree
        seconds = statistics.median(r.seconds for r in column)
        results.append(Result(first.name, first.ops, seconds, first.note))

    print(
        f"Python {platform.python_version()}, {platform.system()} {platform.machine()}, "
        f"{os.cpu_count()} CPUs. {args.count} operations per workload, 16-byte keys, "
        f"{args.value_size}-byte values, default Options except the sync policy. "
        f"Median of {args.repeat} runs.\n"
    )
    print("| Workload | Operations | Seconds | Ops/s | Notes |")
    print("| --- | ---: | ---: | ---: | --- |")
    for r in results:
        print(f"| {r.name} | {r.ops} | {r.seconds:.2f} | {r.rate:,.0f} | {r.note} |")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
