"""Configuration for a database instance."""

from __future__ import annotations

import enum
from dataclasses import dataclass


class SyncPolicy(enum.Enum):
    """When the write-ahead log is forced to stable storage with ``fsync``.

    Every policy hands each write to the operating system before the call
    returns, so a crash of the Python process alone never loses an acknowledged
    write. The policies differ in what survives an operating system crash or a
    power failure:

    ``EVERY_WRITE``
        The log is fsynced before ``put``, ``delete`` or ``write`` returns.
        Every acknowledged write survives a power failure.

    ``PERIODIC``
        The log is fsynced by the first write that happens at least
        ``Options.sync_interval`` seconds after the previous fsync, and by
        ``sync`` and ``close``. A power failure can lose the writes
        acknowledged since the last fsync. There is no background thread, so if
        writes stop, the tail stays unsynced until the next write, ``sync`` or
        ``close``.

    ``ON_CLOSE``
        The log is fsynced only by ``sync`` and ``close``. A power failure can
        lose every write since the last of those.

    Under every policy a flush also makes all earlier writes durable: it
    fsyncs the log it retires and the table that replaces it.

    Under all three policies the state recovered after a crash is the result of
    a prefix of the acknowledged writes: a batch is applied entirely or not at
    all, and a later write is never kept while an earlier one is lost.
    """

    EVERY_WRITE = "every_write"
    PERIODIC = "periodic"
    ON_CLOSE = "on_close"


@dataclass(frozen=True, slots=True)
class Options:
    """Tuning knobs. The defaults favour safety over speed."""

    sync: SyncPolicy = SyncPolicy.EVERY_WRITE
    """When the write-ahead log is fsynced; see :class:`SyncPolicy`."""

    sync_interval: float = 1.0
    """Seconds between fsyncs under ``SyncPolicy.PERIODIC``."""

    memtable_size: int = 4 * 1024 * 1024
    """Approximate bytes buffered in memory before the memtable is flushed to a table."""

    block_size: int = 4096
    """Target size in bytes of an SSTable data block before its trailer."""

    bloom_bits_per_key: int = 10
    """Bloom filter bits per distinct key; 10 gives roughly a 1% false positive rate."""

    compaction_fanout: int = 4
    """Number of tables in one tier that triggers merging them into the next tier."""

    create_if_missing: bool = True
    """Create the database if the directory holds none."""

    def __post_init__(self) -> None:
        if not isinstance(self.sync, SyncPolicy):
            raise TypeError("sync must be a SyncPolicy")
        if self.sync_interval <= 0:
            raise ValueError("sync_interval must be positive")
        if self.memtable_size < 1:
            raise ValueError("memtable_size must be at least 1 byte")
        if self.block_size < 1:
            raise ValueError("block_size must be at least 1 byte")
        if not 1 <= self.bloom_bits_per_key <= 64:
            raise ValueError("bloom_bits_per_key must be between 1 and 64")
        if self.compaction_fanout < 2:
            raise ValueError("compaction_fanout must be at least 2")
