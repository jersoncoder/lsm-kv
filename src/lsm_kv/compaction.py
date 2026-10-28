"""Which versions survive a flush or a compaction, and when tiers are merged.

Garbage collection rule
-----------------------
For one key, take its versions newest first, ``v0, v1, ...`` with sequence
numbers ``s0 > s1 > ...``. A reader at sequence number ``r`` (a snapshot, or
the live view at the latest sequence number) sees the newest version with
``s <= r``. So version ``vi`` (``i >= 1``) is visible to some reader only if a
live snapshot ``r`` satisfies ``si <= r < s(i-1)``; otherwise it is shadowed for
everyone and is dropped. ``v0`` is always kept: tables newer than the ones
being merged may hold newer versions, but nothing older than ``v0`` in the
merge can take its place.

A tombstone is still needed while something older than it could reappear
without it. So after the pass above, tombstones at the old end of the kept
list are dropped only if no table older than the merge inputs could contain
the key (checked with each older table's key range and Bloom filter). A
tombstone with an older kept version behind it is never dropped.

Tiering policy
--------------
Flushes create tables in tier 0. When a tier holds ``fanout`` tables, all of
them are merged into one table in the next tier. Because a merge takes a whole
tier and puts its output at the front of the next one, tables stay ordered by
age (tier ascending, then newest first) and their sequence number ranges never
overlap. The read path relies on that order: the first table that has a
version of a key visible to the reader has the right one.
"""

from __future__ import annotations

from bisect import bisect_left
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Sequence

from .entry import KIND_DELETE, Entry


def collect_garbage(
    entries: Iterable[Entry],
    snapshots: Sequence[int],
    older_may_contain: Callable[[bytes], bool],
) -> Iterator[Entry]:
    """Filter entries (in internal order) down to the versions still needed.

    ``snapshots`` is the sorted list of live snapshot sequence numbers.
    ``older_may_contain(key)`` must return True unless no table older than the
    input could hold ``key``.
    """
    group: list[Entry] = []
    for entry in entries:
        if group and entry[0] != group[0][0]:
            yield from _collect_key(group, snapshots, older_may_contain)
            group = []
        group.append(entry)
    if group:
        yield from _collect_key(group, snapshots, older_may_contain)


def _collect_key(
    versions: list[Entry],
    snapshots: Sequence[int],
    older_may_contain: Callable[[bytes], bool],
) -> list[Entry]:
    kept = [versions[0]]
    newer_seq = versions[0][1]
    for entry in versions[1:]:
        seq = entry[1]
        i = bisect_left(snapshots, seq)
        if i < len(snapshots) and snapshots[i] < newer_seq:
            kept.append(entry)
        newer_seq = seq
    if kept[-1][2] == KIND_DELETE and not older_may_contain(versions[0][0]):
        while kept and kept[-1][2] == KIND_DELETE:
            kept.pop()
    return kept


def pick_tier(tiers: Iterable[int], fanout: int) -> int | None:
    """Return the lowest tier holding at least ``fanout`` tables, if any."""
    counts = Counter(tiers)
    for tier in sorted(counts):
        if counts[tier] >= fanout:
            return tier
    return None
