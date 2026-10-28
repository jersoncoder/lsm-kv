from __future__ import annotations

import random

import pytest

from lsm_kv.compaction import collect_garbage, pick_tier
from lsm_kv.entry import KIND_DELETE, KIND_PUT, Entry, internal_order


def put(key: bytes, seq: int) -> Entry:
    return (key, seq, KIND_PUT, f"{key.decode()}@{seq}".encode())


def tomb(key: bytes, seq: int) -> Entry:
    return (key, seq, KIND_DELETE, b"")


def gc(entries: list[Entry], snapshots: list[int], older: bool = False) -> list[Entry]:
    return list(collect_garbage(entries, snapshots, lambda key: older))


def test_without_snapshots_only_the_newest_version_survives() -> None:
    entries = [put(b"a", 9), put(b"a", 5), put(b"a", 2), put(b"b", 7)]
    assert gc(entries, []) == [put(b"a", 9), put(b"b", 7)]


def test_snapshot_keeps_the_version_it_sees() -> None:
    entries = [put(b"a", 9), put(b"a", 5), put(b"a", 2)]
    assert gc(entries, [6]) == [put(b"a", 9), put(b"a", 5)]
    assert gc(entries, [5]) == [put(b"a", 9), put(b"a", 5)]
    assert gc(entries, [4]) == [put(b"a", 9), put(b"a", 2)]
    assert gc(entries, [1]) == [put(b"a", 9)]  # the key did not exist yet


def test_tombstone_is_dropped_only_when_no_older_table_may_hold_the_key() -> None:
    entries = [tomb(b"a", 9), put(b"a", 5)]
    assert gc(entries, []) == []
    assert gc(entries, [], older=True) == [tomb(b"a", 9)]


def test_tombstone_in_front_of_a_kept_version_is_kept() -> None:
    entries = [tomb(b"a", 9), put(b"a", 5)]
    assert gc(entries, [7]) == [tomb(b"a", 9), put(b"a", 5)]


def test_old_tombstones_at_the_end_are_dropped() -> None:
    entries = [put(b"a", 9), tomb(b"a", 5), put(b"a", 2)]
    # The snapshot at 6 sees the tombstone, which hides nothing once the
    # version at 2 is gone, so dropping it changes no reader's result.
    assert gc(entries, [6]) == [put(b"a", 9)]
    assert gc(entries, [6], older=True) == [put(b"a", 9), tomb(b"a", 5)]


def visible(entries: list[Entry], key: bytes, reader: int, older: bool) -> bytes | None:
    """What a reader at sequence number ``reader`` gets for ``key``."""
    for k, seq, kind, value in entries:
        if k == key and seq <= reader:
            return value if kind == KIND_PUT else None
    return b"<older table>" if older else None


@pytest.mark.parametrize("seed", range(20))
def test_no_reader_can_tell_the_difference(seed: int) -> None:
    """Brute-force check of the rule on random histories and snapshots."""
    rng = random.Random(seed)
    keys = [bytes([97 + i]) for i in range(6)]
    entries = []
    for seq in rng.sample(range(1, 200), 60):
        key = rng.choice(keys)
        entries.append(put(key, seq) if rng.random() < 0.7 else tomb(key, seq))
    entries.sort(key=internal_order)
    snapshots = sorted(rng.sample(range(0, 210), 8))
    for older in (False, True):
        out = gc(entries, snapshots, older)
        assert len(out) <= len(entries)
        for reader in [*snapshots, 10**9]:
            for key in keys:
                assert visible(out, key, reader, older) == visible(entries, key, reader, older)


def test_pick_tier_chooses_the_lowest_full_tier() -> None:
    assert pick_tier([0, 0, 0, 1, 1], fanout=4) is None
    assert pick_tier([0, 0, 0, 0, 1, 1, 1, 1], fanout=4) == 0
    assert pick_tier([0, 1, 1, 1, 1], fanout=4) == 1
    assert pick_tier([], fanout=2) is None
