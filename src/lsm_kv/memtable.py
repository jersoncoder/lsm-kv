"""The in-memory write buffer.

Keys live in a sorted Python list (binary search for lookups, ``insort`` for
new keys) and each key maps to the list of its versions in the order they were
written. Keeping every version, not just the latest, is what lets a snapshot
taken before an overwrite still read the old value; versions no snapshot can
see are dropped when the memtable is flushed.

``insort`` is linear in the number of keys, but it is a ``memmove`` in C, and
for a few megabytes of keys it is faster in CPython than a skip list written
in Python.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right, insort
from collections.abc import Iterator
from typing import Final

from .entry import Entry

ENTRY_OVERHEAD: Final = 32
"""Bytes charged per version on top of key and value, for the size estimate."""


class MemTable:
    __slots__ = ("_keys", "_size", "_versions")

    def __init__(self) -> None:
        self._keys: list[bytes] = []
        self._versions: dict[bytes, list[tuple[int, int, bytes]]] = {}
        self._size = 0

    @property
    def approximate_size(self) -> int:
        return self._size

    def __len__(self) -> int:
        """Number of versions held."""
        return sum(len(v) for v in self._versions.values())

    def is_empty(self) -> bool:
        return not self._keys

    def add(self, seq: int, kind: int, key: bytes, value: bytes) -> None:
        """Record a version. Sequence numbers must increase across calls."""
        versions = self._versions.get(key)
        if versions is None:
            self._versions[key] = [(seq, kind, value)]
            insort(self._keys, key)
        else:
            versions.append((seq, kind, value))
        self._size += len(key) + len(value) + ENTRY_OVERHEAD

    def get(self, key: bytes, seq: int) -> tuple[int, bytes] | None:
        """Return ``(kind, value)`` of the newest version at or below ``seq``."""
        versions = self._versions.get(key)
        if versions is None:
            return None
        for version_seq, kind, value in reversed(versions):
            if version_seq <= seq:
                return kind, value
        return None

    def entries(self, start: bytes | None = None, end: bytes | None = None) -> Iterator[Entry]:
        """Yield versions with ``start <= key < end`` in internal order.

        The iterator tolerates keys being added between steps: it finds its
        place again by binary search after each key instead of holding an
        index into the list. Versions added after a key was visited are not
        seen, which is fine because they are newer than any reader of this
        iterator.
        """
        keys = self._keys
        i = 0 if start is None else bisect_left(keys, start)
        while i < len(keys):
            key = keys[i]
            if end is not None and key >= end:
                return
            for seq, kind, value in self._versions[key][::-1]:
                yield key, seq, kind, value
            i = bisect_right(keys, key)
