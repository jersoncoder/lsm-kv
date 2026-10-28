"""The internal representation of one version of a key.

An entry is a plain tuple ``(key, seq, kind, value)``: tuples are the cheapest
record type in CPython and compare without custom code. Entries are ordered by
key ascending, then by sequence number descending, so the newest version of a
key comes first. :func:`internal_order` gives that order as a sort key.
"""

from __future__ import annotations

from typing import Final

KIND_DELETE: Final = 0
"""A tombstone: the key was deleted at this sequence number. Its value is empty."""

KIND_PUT: Final = 1
"""A value written at this sequence number."""

MAX_SEQUENCE: Final = (1 << 64) - 1
"""Sequence numbers are stored as unsigned 64-bit integers."""

Entry = tuple[bytes, int, int, bytes]
"""``(key, seq, kind, value)``."""


def internal_order(entry: Entry) -> tuple[bytes, int]:
    """Sort key giving key ascending, then newest version first."""
    return entry[0], -entry[1]
