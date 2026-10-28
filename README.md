# lsm-kv

lsm-kv is an embedded key-value storage engine built on a log-structured merge
tree, written in pure Python with no runtime dependencies. It stores byte
keys and values in a directory, supports atomic write batches, snapshots and
ordered range scans, and recovers to a consistent state after a crash. It is
small enough to read in an afternoon, and most of its code and tests are about
the part of a storage engine that is easiest to get wrong: what is on disk
after the machine stops at an arbitrary moment.

## Quick start

Python 3.11 or newer on Linux or another POSIX system. From a clone of this
repository:

```bash
python -m pip install .
```

```python
from lsm_kv import DB

with DB.open("example-db") as db:
    db.put(b"user:1", b"Ada")
    db.put(b"user:2", b"Grace")
    with db.write_batch() as batch:  # applied atomically when the block exits
        batch.put(b"user:3", b"Barbara")
        batch.delete(b"user:2")

    with db.snapshot() as snap:  # a consistent read view
        db.put(b"user:1", b"Ada Lovelace")
        print(db.get(b"user:1"))  # b'Ada Lovelace'
        print(snap.get(b"user:1"))  # b'Ada'

    for key, value in db.scan(b"user:", b"user;"):  # start inclusive, end exclusive
        print(key, value)

with DB.open("example-db") as db:  # reopening replays the write-ahead log
    print(db.get(b"user:3"))  # b'Barbara'
```

`DB.open(path, Options(...))` takes tuning options (see
`src/lsm_kv/options.py`). The default sync policy fsyncs the log on every
write. There is also a small
command line interface:

```bash
python -m lsm_kv example-db scan
python -m lsm_kv example-db put user:4 Edsger
```

## Design

```text
                 put / delete / write(batch)
                              |
                              | 1. append one record, fsync per sync policy
                              v
   +-----------------------------------------+
   | write-ahead log              000007.log |
   +-----------------------------------------+
                              |
                              | 2. apply, then acknowledge
                              v
   +-----------------------------------------+    get / scan / snapshot read
   | memtable: sorted keys, every version    | <-- the memtable first, then the
   +-----------------------------------------+    tables from newest to oldest
                              |
                              | flush when full
                              v
   tier 0    [000012.sst] [000010.sst]                newest
   tier 1    [000009.sst]                               |
   tier 2    [000004.sst] [000002.sst]                oldest

   A tier that reaches compaction_fanout tables is merged into one table
   at the front of the next tier.

   MANIFEST  the live tables and the oldest live log. Rewritten after every
             flush and merge: write MANIFEST.tmp, fsync, rename, fsync the
             directory.
```

**Writes.** Every write is a batch (a single `put` is a batch of one). The
batch is encoded as one log record with a length, a checksum of the length,
and a checksum of the payload, appended to the current log with `os.write`,
and fsynced according to the sync policy. Only then is it applied to the
memtable and acknowledged. Each operation gets the next 64-bit sequence
number.

**Memtable.** A sorted list of keys (binary search, `bisect.insort` for new
keys) and, per key, every version written since the last flush. Keeping old
versions is what lets a snapshot taken before an overwrite still read the old
value.

**Flush.** When the memtable reaches `Options.memtable_size`, the next write
first flushes it. The protocol, in order:

1. Write the versions still needed (see compaction) to a new table, fsync it,
   fsync the directory.
2. Fsync the current log, create the next log.
3. Commit a new manifest that lists the table and says logs below the new
   log's number are obsolete: write `MANIFEST.tmp`, fsync, rename over
   `MANIFEST`, fsync the directory.
4. Delete the old log.

A crash before step 3 completes leaves the old manifest and the old log,
which still hold everything; a crash after it leaves the new manifest and the
table. Leftovers (an unreferenced table, an old log, `MANIFEST.tmp`) are
removed on the next open.

**Tables.** Immutable files of sorted versions: checksummed data blocks, a
Bloom filter, a properties block, a sparse index (the last key of each data
block) and a footer with offsets, a format version, its own checksum and a
magic number. `docs/FORMAT.md` specifies every byte.

**Reads.** `get` looks in the memtable, then in the tables from newest to
oldest, and stops at the first version it finds; a table is skipped if the key
is outside its key range or its Bloom filter rules the key out. `scan` merges
the memtable and the overlapping tables with a heap (`heapq.merge`), takes the
newest version of each key and skips tombstones. A scan pins the memtable and
tables it started with, so it sees a fixed view even while writes, flushes and
compactions continue; tables deleted by compaction are removed from disk when
the last scan using them finishes.

**Snapshots.** A snapshot is a sequence number. Reads through it ignore
versions written after it. While it is alive, flush and compaction keep the
versions it can see.

**Compaction.** Size-tiered: flushes create tables in tier 0, and when a tier
holds `compaction_fanout` tables (4 by default) they are merged into one table
in the next tier. While merging, a version is dropped when no reader can see
it: it is shadowed by a newer version and no live snapshot falls between the
two. A tombstone is dropped only when nothing older than the merged tables
could still hold the key (checked with the older tables' key ranges and Bloom
filters) and no older version of the key is kept. `DB.compact()` flushes and
merges everything into one table.

**Recovery.** `DB.open` takes the directory lock, loads and verifies the
manifest, opens and verifies every live table, and replays every log at or
above the manifest's log number into a new memtable, checking that sequence
numbers continue without gaps. A torn last record (an interrupted append) is
cut off; damage followed by intact records raises `CorruptionError`. For an
existing database, nothing on disk is changed until all of that succeeds.
Then it fsyncs the directory (the crashed process may have created or renamed
files without doing so), removes leftovers, and starts accepting writes.

## Decisions and trade-offs

**Size-tiered rather than leveled compaction.** Each merge takes a whole tier
and writes its output at the front of the next one, so tables stay ordered by
age and their sequence number ranges never overlap. That keeps the read path
and the tombstone rule simple (the first table with a visible version has the
right answer; "older tables" are just the ones further down the list) and
gives low write amplification: each byte is rewritten about once per tier. The
costs are the usual ones for tiering: more tables to consult per read (Bloom
filters keep that cheap for absent keys), and space amplification, since up to
`fanout - 1` tables per tier can hold overlapping versions. Leveled compaction
would bound both, at the price of rewriting data more often and of more code
to pick and split overlapping ranges.

**Synchronous flush and compaction under one lock.** All public methods take
one re-entrant lock and flushes and merges run in the writing thread. There is
no background thread whose failures or races need handling, and the crash
tests can record a deterministic sequence of file operations. The cost is
latency: a write that triggers a flush or a large merge waits for it.

**Durable by default.** `SyncPolicy.EVERY_WRITE` is the default. The other
policies are documented by exactly what they lose: under `PERIODIC` and
`ON_CLOSE` a power failure can lose the most recent acknowledged writes, but
what recovery returns is always the result of a prefix of them (or, in the
one case described under Limitations, a `CorruptionError`). A process crash
alone loses nothing under any policy, because every write reaches the
operating system before it is acknowledged.

**Stop writing after an I/O error.** If an append or fsync fails, the log may
end in a partial record, and after a failed fsync the kernel may have dropped
the dirty pages. Appending more would turn a torn tail into corruption in the
middle of the log, so the instance raises `DBFailedError` for writes until it
is reopened (reads keep working). The same applies to a failed flush or merge;
in that case the log is still healthy, and `sync()` and `close()` still fsync
it, so writes acknowledged under the weaker sync policies can be made durable.

**Checksums everywhere, verified on every read.** Every log record, block,
footer and manifest is covered by CRC-32 (`zlib.crc32`, which runs in C). A
data block is verified each time it is read, which costs time on every read
but means damaged data is reported rather than returned. CRC-32C, which most
storage engines use, would need either a dependency or a much slower
pure-Python implementation; for detecting accidental damage the two are
equivalent here.

**Fixed-width fields and a JSON manifest.** Entries use fixed-width
little-endian headers parsed with `struct`, which is simple to specify and
fast in CPython, at the cost of a few bytes per entry compared with varints.
The manifest is a small checksummed JSON document rewritten whole on each
change instead of an append-only edit log: easy to inspect, atomic through
rename, and cheap for the tens of tables this engine keeps.

**A separate checksum for the log record length.** If only the payload were
checksummed, a damaged length field in the middle of the log could point past
the end of the file and look like an interrupted append, silently dropping
every record after it. With the length checked on its own, that case is
reported as corruption.

## How correctness is tested

The test suite (`pytest`, 155 tests, about 20 seconds on the machine used for
the numbers below) has four layers.

**Unit tests per component**: log framing and batch encoding, memtable
ordering and visibility, Bloom filter false positive rate, table round trips,
manifest encoding, and the version garbage collection rule, which is also
checked by brute force against random histories and snapshots: no reader may
see a different result after collection (`tests/test_compaction.py`).

**Model-based randomized test** (`tests/test_model.py`): six seeded runs of
2,500 random puts, deletes, batches, range scans, snapshot reads, flushes,
full compactions and close/reopen cycles, side by side with a plain dict.
After every step the keys it touched (and one random key) are compared; the
full contents are compared every 25 steps and after every flush, compaction
and reopen, and each live snapshot is compared with a frozen copy of the dict.
Small memtables, blocks and fanouts make flushes and multi-tier merges happen
every few dozen operations.

**Crash tests** (`tests/test_crash.py`, `tests/crashsim.py`). Every file
operation the engine makes goes through `lsm_kv/fileops.py`. The tests record
a workload's operations (database creation, many flushes and merges, an
explicit flush, a full compaction, a last series of writes) and rebuild the
directory as a crash would leave it, in three ways:

* a process crash before every recorded operation, and at every byte offset
  inside the last eight log records and inside every table and manifest write
  of a flush and of a compaction. Recovery must produce exactly the model after
  the writes whose log record was complete. After a cut inside a log record,
  the recovered database must also accept new writes that survive another
  reopen (the torn tail must really be gone);
* a power failure before every recorded operation, where files keep only
  their fsynced bytes (or, for one file at a time, the first byte, half or all
  of its unsynced tail) and the directory keeps the changes covered by its
  last fsync plus any subset of later ones. Under `EVERY_WRITE` recovery must
  keep every acknowledged write; under `ON_CLOSE` it must produce the result
  of a prefix of the writes, and the test checks that acknowledged writes can
  indeed be lost, as documented;
* both in sequence: a process crash before every recorded operation, a
  recovery that acknowledges one more write, then every power failure after
  it. The new write and everything the recovered database showed before it
  must survive.

**Corruption tests** (`tests/test_corruption.py`): flipping any byte of a
table (three bit patterns, every offset) must make opening it or reading the
damaged block raise `CorruptionError`, and point reads must never return
another entry. Damaged log records, gaps in sequence numbers, damaged or
resized tables and a damaged manifest must stop `DB.open` without changing
the directory.

`scripts/mutations.py` checks that these tests have teeth: it removes one
safeguard at a time (each fsync and directory fsync of the protocol, the
torn-tail truncation, the sequence check, checksum checks) from a copy of the
code and confirms the tests fail.

Two real problems turned up while these tests were being built. Dropping the
fsync of the old log at a flush looked safe under the process-crash model, but
under a power failure it can leave a torn log that is not the newest, which
recovery rightly refuses; the fsync stays, with that reason in a comment. And
a review found that recovery relied on directory changes the crashed process
had never fsynced (a log it had just created, a manifest it had just renamed),
so a power failure after recovery could lose writes acknowledged by the
recovered database. Recovery now fsyncs the directory before relying on
anything it found, and the crash-then-power-failure test above reproduces the
problem without that fsync.

Finally, `tests/test_format_doc.py` rebuilds the hex examples in
`docs/FORMAT.md` from the encoders, so the specification cannot drift from
the code.

## Performance

Measured on one laptop (Linux under WSL2, databases on ext4) with:

```bash
python bench/bench.py --dir <a directory on disk> --count 200000
```

Each figure is the median of three runs. The machine was running other work
at the time, and run-to-run differences were large, so read these as orders
of magnitude for this implementation, not as a comparison with anything else.

Python 3.12.14, Linux x86_64, 16 CPUs. 200,000 operations per workload,
16-byte keys, 100-byte values, default `Options` except the sync policy.

| Workload | Operations | Seconds | Ops/s | Notes |
| --- | ---: | ---: | ---: | --- |
| fill sequential keys | 200,000 | 2.91 | 68,614 | `ON_CLOSE` |
| fill random keys | 200,000 | 3.24 | 61,770 | `ON_CLOSE`; ends with 3 tables in tier 0 and 1 in tier 1 |
| get existing keys | 200,000 | 5.83 | 34,312 | |
| get missing keys | 200,000 | 1.38 | 144,948 | inside every table's key range, so the Bloom filters decide |
| full scan | 126,364 | 0.21 | 589,693 | entries returned |
| fill, fsync every write | 2,000 | 3.41 | 587 | `EVERY_WRITE` |

Write amplification after the random fill was 2.87: for each byte of keys and
values, 2.87 bytes were written to logs and tables (1.59 to tables alone; the
log adds its record and batch headers to every write).

Reads of present keys are the slow path: there is no block cache, so each one
reads a data block, verifies its checksum and parses it in Python. The
fsync-per-write figure is dominated by the cost of fsync itself.

## Limitations

These are deliberate omissions, not plans.

* **Single process, one lock.** Operations are serialized; flushes and merges
  run inline and stall the write that triggers them.
* **No block cache, no compression.** Every read goes to the operating
  system's page cache and parses a block in Python; keys are stored in full
  (no prefix compression) and blocks are not compressed.
* **Unbounded merge outputs.** A merge writes one table however large, and a
  full compaction needs free space equal to the live data. Each open table
  keeps its index and Bloom filter in memory.
* **Recovery holds a log in memory.** A log is read whole during recovery;
  its size is bounded by the memtable size plus one batch.
* **Forward iteration only.** No reverse scans, prefix iterators with seek,
  TTLs, column families or merge operators.
* **POSIX only.** The engine relies on `fcntl.flock`, `os.pread` and fsync of
  directories. CI runs on Linux; other systems are untested.
* **The crash model is a model.** The simulator assumes that fsync and
  directory fsync work as POSIX describes and that a write reaches the disk
  as a prefix. It does not test real power cuts, and durability depends on the
  device honoring fsync.
* **A damaged last log record is indistinguishable from a torn write** and is
  dropped. With `PERIODIC` or `ON_CLOSE`, a file system that writes unsynced
  log pages out of order can leave a hole in the log; recovery then raises
  `CorruptionError` rather than guess.
* **Snapshots live only in memory.** They end when the database is closed, and
  an unreleased snapshot keeps old versions alive until then.
* **No repair or backup tools.**

## Development

```bash
python -m venv .venv && . .venv/bin/activate   # or: uv venv && uv pip install -e '.[dev]'
python -m pip install -e '.[dev]'

ruff check .
ruff format --check .
mypy
pytest

python scripts/mutations.py                    # about two minutes
python bench/bench.py --dir /path/on/disk
```

`mypy` runs in strict mode over `src`, `tests`, `bench` and `scripts`. CI
(`.github/workflows/ci.yml`) runs the four checks on Python 3.11, 3.12 and
3.13.

## License

MIT, see `LICENSE`.
