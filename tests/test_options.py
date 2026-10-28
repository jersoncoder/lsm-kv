from __future__ import annotations

import pytest

from lsm_kv import Options, SyncPolicy


def test_defaults_favour_durability() -> None:
    assert Options().sync is SyncPolicy.EVERY_WRITE


@pytest.mark.parametrize(
    "kwargs",
    [
        {"sync_interval": 0},
        {"memtable_size": 0},
        {"block_size": 0},
        {"bloom_bits_per_key": 0},
        {"bloom_bits_per_key": 65},
        {"compaction_fanout": 1},
    ],
)
def test_rejects_invalid_values(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="must"):
        Options(**kwargs)  # type: ignore[arg-type]


def test_rejects_a_sync_policy_that_is_not_the_enum() -> None:
    with pytest.raises(TypeError):
        Options(sync="every_write")  # type: ignore[arg-type]
