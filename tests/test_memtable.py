from __future__ import annotations

from lsm_kv.entry import KIND_DELETE, KIND_PUT
from lsm_kv.memtable import ENTRY_OVERHEAD, MemTable


def build() -> MemTable:
    mem = MemTable()
    mem.add(1, KIND_PUT, b"b", b"b1")
    mem.add(2, KIND_PUT, b"a", b"a2")
    mem.add(3, KIND_PUT, b"b", b"b3")
    mem.add(4, KIND_DELETE, b"a", b"")
    mem.add(5, KIND_PUT, b"c", b"c5")
    return mem


def test_get_returns_the_newest_version_visible_at_a_sequence_number() -> None:
    mem = build()
    assert mem.get(b"b", 5) == (KIND_PUT, b"b3")
    assert mem.get(b"b", 2) == (KIND_PUT, b"b1")
    assert mem.get(b"a", 5) == (KIND_DELETE, b"")
    assert mem.get(b"a", 3) == (KIND_PUT, b"a2")
    assert mem.get(b"a", 1) is None
    assert mem.get(b"zzz", 5) is None


def test_entries_are_in_internal_order() -> None:
    assert list(build().entries()) == [
        (b"a", 4, KIND_DELETE, b""),
        (b"a", 2, KIND_PUT, b"a2"),
        (b"b", 3, KIND_PUT, b"b3"),
        (b"b", 1, KIND_PUT, b"b1"),
        (b"c", 5, KIND_PUT, b"c5"),
    ]


def test_entries_respect_bounds() -> None:
    mem = build()
    assert [e[0] for e in mem.entries(b"b", b"c")] == [b"b", b"b"]
    assert [e[0] for e in mem.entries(b"a\x00")] == [b"b", b"b", b"c"]
    assert list(mem.entries(b"d")) == []


def test_iteration_survives_inserts_between_steps() -> None:
    mem = build()
    it = mem.entries()
    seen = [next(it)[0]]
    mem.add(6, KIND_PUT, b"0", b"before everything")
    mem.add(7, KIND_PUT, b"bb", b"inserted ahead")
    seen += [e[0] for e in it]
    assert seen == [b"a", b"a", b"b", b"b", b"bb", b"c"]


def test_size_counts_every_version() -> None:
    mem = MemTable()
    assert mem.is_empty()
    mem.add(1, KIND_PUT, b"key", b"value")
    mem.add(2, KIND_PUT, b"key", b"v")
    assert mem.approximate_size == (3 + 5 + ENTRY_OVERHEAD) + (3 + 1 + ENTRY_OVERHEAD)
    assert len(mem) == 2
    assert not mem.is_empty()
