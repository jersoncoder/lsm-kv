"""Record the engine's file system operations and rebuild crash states from them.

:class:`Recorder` replaces the functions in ``lsm_kv.fileops`` with wrappers
that log every operation (creates, appends, fsyncs, renames, deletes) and then
perform it. Real fsyncs are skipped: durability is what the simulation itself
decides. From the log, two crash models rebuild the directory as it could be
found after a crash at any point:

``process_crash(k, cut)``
    The process dies before operation ``k``. Everything it handed to the
    operating system is kept, because the kernel still has it; if operation
    ``k`` is a write, its first ``cut`` bytes made it.

``power_failure(k, keep, persisted)``
    The machine loses power before operation ``k``. A file keeps only the
    bytes covered by its last fsync. The directory keeps the entries
    (creations, renames, deletions) covered by its last fsync, plus any
    subset of the later ones (``persisted``), because a file system may write
    directory changes early and in any order. ``keep`` lets part of a file's
    unsynced tail survive as well. :meth:`Recorder.power_failures` enumerates
    every subset.

The base state is the directory when recording started, treated as durable.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any, Self

from lsm_kv import fileops


@dataclass(frozen=True)
class Op:
    kind: str
    """create, write, fsync, fsync_dir, rename, remove, truncate or close."""
    name: str = ""
    data: bytes = b""
    target: str = ""
    size: int = 0


_PATCHED = (
    "create",
    "open_append",
    "write",
    "fsync",
    "fsync_dir",
    "rename",
    "remove",
    "truncate",
    "close",
)


class Recorder:
    """Records file operations while used as a context manager."""

    def __init__(self, directory: str):
        self.directory = os.path.abspath(directory)
        self.base = {
            name: Path(self.directory, name).read_bytes() for name in os.listdir(self.directory)
        }
        self.ops: list[Op] = []
        self._names: dict[int, str] = {}
        self._saved: dict[str, Callable[..., Any]] = {}

    def __enter__(self) -> Self:
        self._saved = {name: getattr(fileops, name) for name in _PATCHED}
        real = self._saved

        def name_of(path: str) -> str:
            assert os.path.dirname(os.path.abspath(path)) == self.directory, path
            return os.path.basename(path)

        def create(path: str) -> int:
            fd: int = real["create"](path)
            self._names[fd] = name_of(path)
            self.ops.append(Op("create", name_of(path)))
            return fd

        def open_append(path: str) -> int:
            fd: int = real["open_append"](path)
            self._names[fd] = name_of(path)
            return fd

        def write(fd: int, data: bytes) -> None:
            real["write"](fd, data)
            self.ops.append(Op("write", self._names[fd], data=bytes(data)))

        def fsync(fd: int) -> None:
            self.ops.append(Op("fsync", self._names[fd]))

        def fsync_dir(path: str) -> None:
            if os.path.abspath(path) == self.directory:
                self.ops.append(Op("fsync_dir"))

        def rename(src: str, dst: str) -> None:
            real["rename"](src, dst)
            self.ops.append(Op("rename", name_of(src), target=name_of(dst)))

        def remove(path: str) -> None:
            real["remove"](path)
            self.ops.append(Op("remove", name_of(path)))

        def truncate(path: str, size: int) -> None:
            real["truncate"](path, size)
            self.ops.append(Op("truncate", name_of(path), size=size))

        def close(fd: int) -> None:
            real["close"](fd)
            self.ops.append(Op("close", self._names.pop(fd)))

        wrappers = [create, open_append, write, fsync, fsync_dir, rename, remove, truncate, close]
        for name, wrapper in zip(_PATCHED, wrappers, strict=True):
            setattr(fileops, name, wrapper)
        return self

    def __exit__(self, *exc_info: object) -> None:
        for name, function in self._saved.items():
            setattr(fileops, name, function)

    def process_crash(self, k: int, cut: int = 0) -> dict[str, bytes]:
        files = dict(self.base)
        for op in self.ops[:k]:
            if op.kind == "create":
                files[op.name] = b""
            elif op.kind == "write":
                files[op.name] += op.data
            elif op.kind == "rename":
                files[op.target] = files.pop(op.name)
            elif op.kind == "remove":
                del files[op.name]
            elif op.kind == "truncate":
                files[op.name] = files[op.name][: op.size]
        if cut:
            op = self.ops[k]
            assert op.kind == "write", op
            assert cut < len(op.data), (cut, len(op.data))
            files[op.name] += op.data[:cut]
        return files

    def power_failure(
        self,
        k: int,
        *,
        keep: dict[str, int] | None = None,
        persisted: Iterable[int] = (),
    ) -> dict[str, bytes]:
        """Directory contents after a power failure before operation ``k``.

        ``persisted`` selects, by position, which of the directory operations
        issued since the last directory fsync reached the disk anyway (see
        :meth:`pending_directory_ops`). ``keep`` maps a file name to a number
        of unsynced bytes that reached the disk.
        """
        # Model inodes so that a rename carries a file's synced state with it.
        contents: dict[int, bytes] = {}
        synced: dict[int, int] = {}
        names: dict[str, int] = {}
        for inode, (name, data) in enumerate(self.base.items()):
            contents[inode], synced[inode], names[name] = data, len(data), inode
        durable = dict(names)
        pending: list[tuple[str, str, str, int]] = []  # kind, name, target, inode
        for op in self.ops[:k]:
            if op.kind == "create":
                inode = len(contents)
                contents[inode], synced[inode] = b"", 0
                names[op.name] = inode
                pending.append(("create", op.name, "", inode))
            elif op.kind == "write":
                contents[names[op.name]] += op.data
            elif op.kind == "fsync":
                synced[names[op.name]] = len(contents[names[op.name]])
            elif op.kind == "fsync_dir":
                durable = dict(names)
                pending = []
            elif op.kind == "rename":
                inode = names.pop(op.name)
                names[op.target] = inode
                pending.append(("rename", op.name, op.target, inode))
            elif op.kind == "remove":
                pending.append(("remove", op.name, "", names.pop(op.name)))
            elif op.kind == "truncate":  # fileops.truncate fsyncs
                inode = names[op.name]
                contents[inode] = contents[inode][: op.size]
                synced[inode] = op.size
        for position in sorted(persisted):
            kind, name, target, inode = pending[position]
            if kind == "create":
                durable[name] = inode
            elif kind == "rename":
                durable.pop(name, None)
                durable[target] = inode
            else:
                durable.pop(name, None)
        keep = keep or {}
        return {
            name: contents[inode][: synced[inode] + keep.get(name, 0)]
            for name, inode in durable.items()
        }

    def pending_directory_ops(self, k: int) -> int:
        """Directory operations issued since the last directory fsync before ``k``."""
        count = 0
        for op in self.ops[:k]:
            if op.kind == "fsync_dir":
                count = 0
            elif op.kind in ("create", "rename", "remove"):
                count += 1
        return count

    def power_failures(self, k: int) -> Iterator[dict[str, bytes]]:
        """Every power failure state before ``k``: each subset of pending directory ops."""
        pending = self.pending_directory_ops(k)
        assert pending <= 8, "too many unsynced directory operations to enumerate"
        for size in range(pending + 1):
            for subset in combinations(range(pending), size):
                yield self.power_failure(k, persisted=subset)

    def write_ops(self, start: int = 0, end: int | None = None) -> list[int]:
        """Indexes of the write operations in ``ops[start:end]``."""
        stop = len(self.ops) if end is None else end
        return [i for i in range(start, stop) if self.ops[i].kind == "write"]

    def log_records_before(self, k: int) -> int:
        """Number of complete log records among the first ``k`` operations."""
        return sum(1 for op in self.ops[:k] if op.kind == "write" and op.name.endswith(".log"))


def materialize(files: dict[str, bytes], directory: Path) -> str:
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir()
    for name, data in files.items():
        (directory / name).write_bytes(data)
    return str(directory)
