"""The database: write path, read path, flush, compaction and recovery.

Directory contents::

    LOCK            advisory lock held while the database is open
    MANIFEST        live tables and log positions (see manifest.py)
    NNNNNN.log      write-ahead logs
    NNNNNN.sst      tables

File numbers come from one counter and are never reused.

Concurrency: every public method takes one re-entrant lock, so an instance may
be shared between threads, but operations run one at a time. Flushes and
compactions run inline, in the thread whose write found the memtable full,
while holding the lock. Only one instance (in any process) may have a
directory open; a second ``DB.open`` raises :class:`DBLockedError`.
"""

from __future__ import annotations

import fcntl
import heapq
import os
import re
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from types import TracebackType
from typing import Final, Self

from . import fileops, wal
from . import manifest as manifest_mod
from .batch import BytesLike, WriteBatch, as_bytes, decode_batch, encode_batch
from .compaction import collect_garbage, pick_tier
from .entry import KIND_PUT, Entry, internal_order
from .errors import CorruptionError, DBClosedError, DBFailedError, DBLockedError, LSMKVError
from .manifest import Manifest, TableInfo
from .memtable import MemTable
from .options import Options, SyncPolicy
from .sstable import TableReader, TableWriter

LOCK_NAME: Final = "LOCK"
_LOG_RE: Final = re.compile(r"^(\d{6,})\.log$")
_TABLE_RE: Final = re.compile(r"^(\d{6,})\.sst$")


def log_name(number: int) -> str:
    return f"{number:06d}.log"


def table_name(number: int) -> str:
    return f"{number:06d}.sst"


@dataclass
class Stats:
    """Counters since the database was opened."""

    user_bytes: int = 0
    """Key and value bytes passed to put, delete and write."""

    wal_bytes: int = 0
    flush_bytes: int = 0
    compaction_bytes: int = 0
    manifest_bytes: int = 0
    flushes: int = 0
    compactions: int = 0
    tables_per_tier: dict[int, int] = field(default_factory=dict)

    @property
    def write_amplification(self) -> float:
        """Bytes written to logs and tables per byte of user data."""
        if not self.user_bytes:
            return 0.0
        written = self.wal_bytes + self.flush_bytes + self.compaction_bytes
        return written / self.user_bytes


class _Table:
    """A live or retired table and the number of readers using it."""

    __slots__ = ("info", "obsolete", "reader", "refs")

    def __init__(self, info: TableInfo, reader: TableReader):
        self.info = info
        self.reader = reader
        self.refs = 1  # the reference held by the database's table list
        self.obsolete = False


class DB:
    """An embedded key-value store. Create one with :meth:`DB.open`."""

    _tables: list[_Table]
    """Live tables, newest first."""
    _mem: MemTable
    _wal: wal.LogWriter
    _last_seq: int
    """Sequence number of the last acknowledged write."""
    _flushed_seq: int
    """Highest sequence number stored in tables, as recorded in the manifest."""
    _log_number: int
    _next_file: int
    _last_sync: float
    _lock_fd: int

    def __init__(self, path: str | os.PathLike[str], options: Options | None = None):
        self._path = os.path.abspath(os.fspath(path))
        self._options = options if options is not None else Options()
        self._lock = threading.RLock()
        self._closed = True
        self._failed: BaseException | None = None
        self._snapshots: dict[int, int] = {}
        self._retired: set[_Table] = set()
        self._stats = Stats()
        self._open()

    @classmethod
    def open(cls, path: str | os.PathLike[str], options: Options | None = None) -> DB:
        """Open the database in directory ``path``, creating it if allowed.

        Opening runs recovery: it loads the manifest, verifies every live
        table, replays the write-ahead logs and cuts off a torn last record.
        If any check fails it raises before changing anything on disk (other
        than creating the ``LOCK`` file).
        """
        return cls(path, options)

    # -- public API -----------------------------------------------------------

    @property
    def path(self) -> str:
        return self._path

    def put(self, key: BytesLike, value: BytesLike) -> None:
        batch = WriteBatch()
        batch.put(key, value)
        self.write(batch)

    def delete(self, key: BytesLike) -> None:
        batch = WriteBatch()
        batch.delete(key)
        self.write(batch)

    def write(self, batch: WriteBatch) -> None:
        """Apply every operation in ``batch`` atomically, in order."""
        ops = list(batch)
        if not ops:
            return
        with self._lock:
            self._check_writable()
            if self._mem.approximate_size >= self._options.memtable_size:
                self._maintenance(self._flush_locked)
            seq = self._last_seq + 1
            try:
                self._stats.wal_bytes += self._wal.append(encode_batch(seq, ops))
                self._sync_after_write()
            except BaseException as exc:
                # The log may now end in a partial record; appending after it
                # would turn a torn tail into corruption in the middle.
                self._fail(exc)
                raise
            mem = self._mem
            for i, (kind, key, value) in enumerate(ops):
                mem.add(seq + i, kind, key, value)
                self._stats.user_bytes += len(key) + len(value)
            self._last_seq = seq + len(ops) - 1

    @contextmanager
    def write_batch(self) -> Iterator[WriteBatch]:
        """Collect writes in a block and apply them atomically when it exits.

        Nothing is written if the block raises.
        """
        batch = WriteBatch()
        yield batch
        self.write(batch)

    def get(self, key: BytesLike) -> bytes | None:
        with self._lock:
            self._check_open()
            return self._get(as_bytes("key", key), self._last_seq)

    def scan(self, start: BytesLike | None = None, end: BytesLike | None = None) -> ScanIterator:
        """Iterate over ``(key, value)`` with ``start <= key < end`` in key order.

        ``None`` leaves that side unbounded. The iterator reads a consistent
        view as of this call; writes made while it is open are not visible to
        it. Close it (or exhaust it) to let deleted tables be removed.
        """
        with self._lock:
            self._check_open()
            return self._scan(start, end, self._last_seq)

    def snapshot(self) -> Snapshot:
        """Return a read-only view fixed at the current state.

        Old versions the snapshot can see are kept by compaction until it is
        released, so release it (or use it as a context manager) when done.
        """
        with self._lock:
            self._check_open()
            seq = self._last_seq
            self._snapshots[seq] = self._snapshots.get(seq, 0) + 1
            return Snapshot(self, seq)

    def flush(self) -> None:
        """Write the memtable to a table now, then compact tiers if needed."""
        with self._lock:
            self._check_writable()
            self._maintenance(self._flush_locked)

    def compact(self) -> None:
        """Flush, then merge every table into one.

        This drops every shadowed version and tombstone that no live snapshot
        needs.
        """
        with self._lock:
            self._check_writable()
            self._maintenance(self._compact_all_locked)

    def sync(self) -> None:
        """Fsync the write-ahead log, making every acknowledged write durable."""
        with self._lock:
            self._check_writable()
            try:
                self._wal.sync()
            except BaseException as exc:
                self._fail(exc)
                raise
            self._last_sync = time.monotonic()

    def stats(self) -> Stats:
        with self._lock:
            tiers: dict[int, int] = {}
            for table in self._tables:
                tiers[table.info.tier] = tiers.get(table.info.tier, 0) + 1
            s = self._stats
            return Stats(
                user_bytes=s.user_bytes,
                wal_bytes=s.wal_bytes,
                flush_bytes=s.flush_bytes,
                compaction_bytes=s.compaction_bytes,
                manifest_bytes=s.manifest_bytes,
                flushes=s.flushes,
                compactions=s.compactions,
                tables_per_tier=dict(sorted(tiers.items())),
            )

    def close(self) -> None:
        """Fsync the log and release every resource. Calling it twice is harmless."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                if self._failed is None:
                    self._wal.sync()
            finally:
                with suppress(OSError):
                    self._wal.close()
                for table in self._tables:
                    table.reader.close()
                for table in self._retired:
                    table.reader.close()
                    with suppress(OSError):
                        fileops.remove(table.reader.path)
                self._tables = []
                self._retired.clear()
                self._snapshots.clear()
                os.close(self._lock_fd)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<DB {self._path!r} {state}>"

    # -- reads ----------------------------------------------------------------

    def _get(self, key: bytes, seq: int) -> bytes | None:
        found = self._mem.get(key, seq)
        if found is not None:
            kind, value = found
            return value if kind == KIND_PUT else None
        for table in self._tables:
            info = table.info
            if info.min_seq > seq or key < info.smallest or key > info.largest:
                continue
            hit = table.reader.get(key, seq)
            if hit is not None:
                _, kind, value = hit
                return value if kind == KIND_PUT else None
        return None

    def _scan(
        self, start_key: BytesLike | None, end_key: BytesLike | None, seq: int
    ) -> ScanIterator:
        start = None if start_key is None else as_bytes("start", start_key)
        end = None if end_key is None else as_bytes("end", end_key)
        tables = [
            t
            for t in self._tables
            if t.info.min_seq <= seq
            and (start is None or t.info.largest >= start)
            and (end is None or t.info.smallest < end)
        ]
        for table in tables:
            table.refs += 1
        return ScanIterator(self, self._mem, tables, start, end, seq)

    def _unref(self, table: _Table) -> None:
        table.refs -= 1
        if table.refs == 0:
            table.reader.close()
            if table.obsolete:
                self._retired.discard(table)
                # A failed delete leaves an orphan that the next open removes.
                with suppress(OSError):
                    fileops.remove(table.reader.path)

    def _release_snapshot(self, seq: int) -> None:
        count = self._snapshots.get(seq, 0)
        if count <= 1:
            self._snapshots.pop(seq, None)
        else:
            self._snapshots[seq] = count - 1

    # -- state checks ---------------------------------------------------------

    def _check_open(self) -> None:
        if self._closed:
            raise DBClosedError("database is closed")

    def _check_writable(self) -> None:
        self._check_open()
        if self._failed is not None:
            raise DBFailedError(
                "an earlier error left this instance unable to accept writes; "
                "reopen the database to recover"
            ) from self._failed

    def _fail(self, exc: BaseException) -> None:
        if self._failed is None:
            self._failed = exc

    def _maintenance(self, operation: Callable[[], None]) -> None:
        try:
            operation()
        except BaseException as exc:
            self._fail(exc)
            raise

    def _sync_after_write(self) -> None:
        policy = self._options.sync
        if policy is SyncPolicy.EVERY_WRITE:
            self._wal.sync()
        elif policy is SyncPolicy.PERIODIC:
            now = time.monotonic()
            if now - self._last_sync >= self._options.sync_interval:
                self._wal.sync()
                self._last_sync = now

    # -- flush and compaction -------------------------------------------------

    def _new_file_number(self) -> int:
        number = self._next_file
        self._next_file += 1
        return number

    def _flush_locked(self) -> None:
        if self._mem.is_empty():
            return
        entries = collect_garbage(
            self._mem.entries(), sorted(self._snapshots), _older_lookup(self._tables)
        )
        table = self._write_table(entries, tier=0)
        new_wal: wal.LogWriter | None = None
        try:
            # Recovery accepts a torn tail only in the newest log. Once the new
            # log exists, a power failure before the manifest commit would make
            # the old log a torn log that is not the newest, so sync it first.
            # (The new log's directory entry needs no fsync of its own: the
            # manifest commit fsyncs the directory before any record reaches it.)
            self._wal.sync()
            new_number = self._new_file_number()
            new_wal = wal.LogWriter.create(os.path.join(self._path, log_name(new_number)))
            tables = ([table] if table is not None else []) + self._tables
            self._commit(tables, log_number=new_number, last_sequence=self._last_seq)
        except BaseException:
            with suppress(OSError):
                if new_wal is not None:
                    new_wal.close()
                if table is not None:
                    table.reader.close()
            raise
        if table is not None:
            self._stats.flush_bytes += table.info.size
        old_wal, self._wal = self._wal, new_wal
        self._log_number = new_number
        self._flushed_seq = self._last_seq
        self._mem = MemTable()
        self._last_sync = time.monotonic()
        self._stats.flushes += 1
        old_wal.close()
        self._remove_old_logs()
        self._compact_tiers()

    def _compact_tiers(self) -> None:
        while (tier := pick_tier((t.info.tier for t in self._tables), self._fanout)) is not None:
            inputs = [t for t in self._tables if t.info.tier == tier]
            older = [t for t in self._tables if t.info.tier > tier]
            self._merge(inputs, older, tier + 1)

    def _compact_all_locked(self) -> None:
        self._flush_locked()
        if self._tables:
            tier = max(t.info.tier for t in self._tables)
            self._merge(list(self._tables), [], tier)

    @property
    def _fanout(self) -> int:
        return self._options.compaction_fanout

    def _merge(self, inputs: list[_Table], older: list[_Table], output_tier: int) -> None:
        merged = heapq.merge(*(t.reader.entries() for t in inputs), key=internal_order)
        entries = collect_garbage(merged, sorted(self._snapshots), _older_lookup(older))
        output = self._write_table(entries, tier=output_tier)
        tables = [t for t in self._tables if t not in inputs]
        if output is not None:
            tables.append(output)
        tables.sort(key=lambda t: (t.info.tier, -t.info.max_seq))
        try:
            self._commit(tables, log_number=self._log_number, last_sequence=self._flushed_seq)
        except BaseException:
            if output is not None:
                output.reader.close()
            raise
        if output is not None:
            self._stats.compaction_bytes += output.info.size
        self._stats.compactions += 1
        for table in inputs:
            table.obsolete = True
            self._retired.add(table)
            self._unref(table)

    def _write_table(self, entries: Iterable[Entry], tier: int) -> _Table | None:
        number = self._new_file_number()
        path = os.path.join(self._path, table_name(number))
        writer = TableWriter(
            path,
            block_size=self._options.block_size,
            bloom_bits_per_key=self._options.bloom_bits_per_key,
        )
        try:
            for key, seq, kind, value in entries:
                writer.add(key, seq, kind, value)
            if writer.entry_count == 0:
                writer.abandon()
                return None
            props, size = writer.finish()
        except BaseException:
            with suppress(OSError):
                writer.abandon()
            raise
        fileops.fsync_dir(self._path)
        info = TableInfo(
            number=number,
            tier=tier,
            size=size,
            entry_count=props.entry_count,
            min_seq=props.min_seq,
            max_seq=props.max_seq,
            smallest=props.smallest,
            largest=props.largest,
        )
        return _Table(info, TableReader(path))

    def _commit(self, tables: list[_Table], *, log_number: int, last_sequence: int) -> None:
        state = Manifest(
            next_file_number=self._next_file,
            log_number=log_number,
            last_sequence=last_sequence,
            tables=tuple(t.info for t in tables),
        )
        self._stats.manifest_bytes += manifest_mod.store(self._path, state)
        self._tables = tables

    def _remove_old_logs(self) -> None:
        for name in os.listdir(self._path):
            match = _LOG_RE.match(name)
            if match and int(match.group(1)) < self._log_number:
                with suppress(FileNotFoundError):
                    fileops.remove(os.path.join(self._path, name))

    # -- opening and recovery -------------------------------------------------

    def _open(self) -> None:
        path = self._path
        if not os.path.isdir(path):
            if not self._options.create_if_missing:
                raise LSMKVError(f"no database at {path}")
            os.makedirs(path)
            fileops.fsync_dir(os.path.dirname(os.path.abspath(path)))
        self._lock_fd = _acquire_lock(path)
        try:
            self._recover()
        except BaseException:
            os.close(self._lock_fd)
            raise
        self._closed = False

    def _recover(self) -> None:
        path = self._path
        names = os.listdir(path)
        logs_on_disk = sorted(int(m.group(1)) for n in names if (m := _LOG_RE.match(n)))
        tables_on_disk = {int(m.group(1)) for n in names if (m := _TABLE_RE.match(n))}

        state = manifest_mod.load(path)
        if state is None:
            if logs_on_disk or tables_on_disk:
                raise CorruptionError("directory holds data files but no MANIFEST", path=path)
            if not self._options.create_if_missing:
                raise LSMKVError(f"no database at {path}")
            state = Manifest(next_file_number=1, log_number=0, last_sequence=0, tables=())
            self._stats.manifest_bytes += manifest_mod.store(path, state)
        _check_table_order(state, path)

        # Verify everything before changing anything on disk.
        tables: list[_Table] = []
        try:
            for info in state.tables:
                tables.append(_open_table(path, info))
            mem, last_seq, live_logs = _replay_logs(path, state, logs_on_disk)
        except BaseException:
            for table in tables:
                table.reader.close()
            raise

        self._tables = tables
        self._mem = mem
        self._last_seq = last_seq
        self._flushed_seq = state.last_sequence
        self._log_number = state.log_number
        on_disk = [*logs_on_disk, *tables_on_disk]
        self._next_file = max([state.next_file_number, *(n + 1 for n in on_disk)])

        try:
            self._clean_up(names, state)
            if live_logs:
                number, contents = live_logs[-1]
                log_path = os.path.join(path, log_name(number))
                if contents.torn:
                    fileops.truncate(log_path, contents.end)
                self._wal = wal.LogWriter.reopen(log_path, contents.end)
            else:
                number = self._new_file_number()
                self._wal = wal.LogWriter.create(os.path.join(path, log_name(number)))
                fileops.fsync_dir(path)
        except BaseException:
            for table in tables:
                table.reader.close()
            raise
        self._last_sync = time.monotonic()

    def _clean_up(self, names: list[str], state: Manifest) -> None:
        """Remove what an interrupted flush or compaction left behind."""
        live_tables = {info.number for info in state.tables}
        for name in names:
            log_match = _LOG_RE.match(name)
            table_match = _TABLE_RE.match(name)
            if (
                name == manifest_mod.TEMP_NAME
                or (table_match and int(table_match.group(1)) not in live_tables)
                or (log_match and int(log_match.group(1)) < state.log_number)
            ):
                with suppress(FileNotFoundError):
                    fileops.remove(os.path.join(self._path, name))


class ScanIterator:
    """Iterator returned by :meth:`DB.scan` and :meth:`Snapshot.scan`.

    It merges the memtable and the tables with a heap, newest version first,
    and yields the newest visible version of each key, skipping tombstones.
    """

    def __init__(
        self,
        db: DB,
        mem: MemTable,
        tables: list[_Table],
        start: bytes | None,
        end: bytes | None,
        seq: int,
    ):
        self._db = db
        self._tables = tables
        self._seq = seq
        self._last_key: bytes | None = None
        self._done = False
        sources = [mem.entries(start, end), *(t.reader.entries(start, end) for t in tables)]
        self._merged: Iterator[Entry] = heapq.merge(*sources, key=internal_order)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> tuple[bytes, bytes]:
        db = self._db
        with db._lock:
            if self._done:
                raise StopIteration
            if db._closed:
                self._release()
                raise DBClosedError("database is closed")
            try:
                for key, seq, kind, value in self._merged:
                    if seq > self._seq or key == self._last_key:
                        continue
                    self._last_key = key
                    if kind == KIND_PUT:
                        return key, value
            except BaseException:
                self._release()
                raise
            self._release()
            raise StopIteration

    def close(self) -> None:
        with self._db._lock:
            self._release()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        if not getattr(self, "_done", True):
            with suppress(Exception):
                self.close()

    def _release(self) -> None:
        if self._done:
            return
        self._done = True
        self._merged = iter(())
        if not self._db._closed:
            for table in self._tables:
                self._db._unref(table)
        self._tables = []


class Snapshot:
    """A read-only view of the database at one point in its history."""

    def __init__(self, db: DB, seq: int):
        self._db = db
        self._seq = seq
        self._released = False

    @property
    def sequence(self) -> int:
        """Sequence number of the last write visible to this snapshot."""
        return self._seq

    def get(self, key: BytesLike) -> bytes | None:
        with self._db._lock:
            self._check()
            return self._db._get(as_bytes("key", key), self._seq)

    def scan(self, start: BytesLike | None = None, end: BytesLike | None = None) -> ScanIterator:
        with self._db._lock:
            self._check()
            return self._db._scan(start, end, self._seq)

    def release(self) -> None:
        """Let compaction drop the old versions this snapshot was holding."""
        with self._db._lock:
            if not self._released:
                self._released = True
                if not self._db._closed:
                    self._db._release_snapshot(self._seq)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()

    def _check(self) -> None:
        if self._released:
            raise DBClosedError("snapshot has been released")
        self._db._check_open()


# -- helpers --------------------------------------------------------------------


def _acquire_lock(path: str) -> int:
    fd = os.open(os.path.join(path, LOCK_NAME), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise DBLockedError(f"{path} is already open in another DB instance") from None
    return fd


def _older_lookup(older: list[_Table]) -> Callable[[bytes], bool]:
    def may_contain(key: bytes) -> bool:
        return any(
            t.info.smallest <= key <= t.info.largest and t.reader.may_contain(key) for t in older
        )

    return may_contain


def _check_table_order(state: Manifest, path: str) -> None:
    """Live tables must be ordered by age with disjoint sequence number ranges."""
    previous: TableInfo | None = None
    for info in state.tables:
        if info.min_seq > info.max_seq or info.max_seq > state.last_sequence:
            raise CorruptionError(
                f"manifest entry for table {info.number} is inconsistent", path=path
            )
        if previous is not None and (info.tier < previous.tier or info.max_seq >= previous.min_seq):
            raise CorruptionError("manifest lists tables out of order", path=path)
        previous = info


def _open_table(path: str, info: TableInfo) -> _Table:
    table_path = os.path.join(path, table_name(info.number))
    if not os.path.exists(table_path):
        raise CorruptionError("table listed in the manifest is missing", path=table_path)
    reader = TableReader(table_path)
    props = reader.properties
    if (
        reader.file_size != info.size
        or props.entry_count != info.entry_count
        or props.min_seq != info.min_seq
        or props.max_seq != info.max_seq
        or props.smallest != info.smallest
        or props.largest != info.largest
    ):
        reader.close()
        raise CorruptionError("table does not match its manifest entry", path=table_path)
    return _Table(info, reader)


def _replay_logs(
    path: str, state: Manifest, logs_on_disk: list[int]
) -> tuple[MemTable, int, list[tuple[int, wal.LogContents]]]:
    """Rebuild the memtable from every log the manifest does not cover."""
    mem = MemTable()
    last_seq = state.last_sequence
    live = [n for n in logs_on_disk if n >= state.log_number]
    contents: list[tuple[int, wal.LogContents]] = []
    for i, number in enumerate(live):
        log_path = os.path.join(path, log_name(number))
        log = wal.read_log(log_path)
        if log.torn and i != len(live) - 1:
            raise CorruptionError(
                "log that is not the newest ends in a damaged record", path=log_path, offset=log.end
            )
        for offset, payload in log.records:
            seq, ops = decode_batch(payload, path=log_path, offset=offset)
            if seq != last_seq + 1:
                raise CorruptionError(
                    f"log record has sequence number {seq}, expected {last_seq + 1}",
                    path=log_path,
                    offset=offset,
                )
            for j, (kind, key, value) in enumerate(ops):
                mem.add(seq + j, kind, key, value)
            last_seq = seq + len(ops) - 1
        contents.append((number, log))
    return mem, last_seq, contents
