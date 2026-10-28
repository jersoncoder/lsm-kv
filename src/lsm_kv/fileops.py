"""The file system calls that change durable state.

Every write, fsync, rename and delete the engine performs goes through this
module, for two reasons: the durability protocol is visible in one place, and
the crash tests replace these functions to record the exact sequence of
operations and replay any prefix of it. Call them through the module
(``fileops.write(...)``) so that replacement takes effect.

All writes are appends: the engine never overwrites bytes in place.
"""

from __future__ import annotations

import os


def create(path: str) -> int:
    """Create a new, empty file for writing and return its descriptor.

    Fails if the file already exists: file numbers are never reused, so an
    existing file means a bug or a damaged directory, not something to clobber.
    """
    return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)


def open_append(path: str) -> int:
    """Open an existing file for appending and return its descriptor."""
    return os.open(path, os.O_WRONLY | os.O_APPEND)


def write(fd: int, data: bytes) -> None:
    """Write all of ``data`` at the end of the file, retrying short writes."""
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def fsync(fd: int) -> None:
    """Force the file's data and metadata to stable storage."""
    os.fsync(fd)


def fsync_dir(path: str) -> None:
    """Make creations, renames and deletions inside a directory durable."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def close(fd: int) -> None:
    """Close a descriptor returned by :func:`create` or :func:`open_append`."""
    os.close(fd)


def rename(src: str, dst: str) -> None:
    """Atomically replace ``dst`` with ``src``. Not durable until :func:`fsync_dir`."""
    os.replace(src, dst)


def remove(path: str) -> None:
    """Delete a file."""
    os.remove(path)


def truncate(path: str, size: int) -> None:
    """Cut a file to ``size`` bytes and fsync it."""
    fd = os.open(path, os.O_WRONLY)
    try:
        os.ftruncate(fd, size)
        os.fsync(fd)
    finally:
        os.close(fd)
