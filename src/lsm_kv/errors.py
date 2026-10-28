"""Exception types raised by lsm-kv."""

from __future__ import annotations


class LSMKVError(Exception):
    """Base class for every error raised by lsm-kv."""


class CorruptionError(LSMKVError):
    """Stored data failed a checksum or a structural check.

    The engine raises this instead of returning data it cannot verify. ``path``
    and ``offset`` locate the damage when they are known.
    """

    def __init__(self, message: str, *, path: str | None = None, offset: int | None = None):
        self.path = path
        self.offset = offset
        location = ""
        if path is not None:
            location = f" [{path}" + (f" at offset {offset}" if offset is not None else "") + "]"
        super().__init__(message + location)


class UnsupportedFormatError(LSMKVError):
    """A file is intact but was written with a format version this code cannot read."""


class DBClosedError(LSMKVError):
    """The database, or the snapshot or iterator being used, has been closed."""


class DBLockedError(LSMKVError):
    """Another open database instance holds the directory lock."""


class DBFailedError(LSMKVError):
    """An earlier I/O error left this instance unable to accept writes.

    Reads keep working. Reopening the database runs recovery and clears the state.
    """
