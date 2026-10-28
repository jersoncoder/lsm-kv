"""lsm-kv: an embedded key-value store built on a log-structured merge tree."""

from .batch import WriteBatch
from .db import DB, ScanIterator, Snapshot, Stats
from .errors import (
    CorruptionError,
    DBClosedError,
    DBFailedError,
    DBLockedError,
    LSMKVError,
    UnsupportedFormatError,
)
from .options import Options, SyncPolicy

__version__ = "0.1.0"

__all__ = [
    "DB",
    "CorruptionError",
    "DBClosedError",
    "DBFailedError",
    "DBLockedError",
    "LSMKVError",
    "Options",
    "ScanIterator",
    "Snapshot",
    "Stats",
    "SyncPolicy",
    "UnsupportedFormatError",
    "WriteBatch",
]
