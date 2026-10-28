"""lsm-kv: an embedded key-value store built on a log-structured merge tree."""

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
    "CorruptionError",
    "DBClosedError",
    "DBFailedError",
    "DBLockedError",
    "LSMKVError",
    "Options",
    "SyncPolicy",
    "UnsupportedFormatError",
]
