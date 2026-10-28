from __future__ import annotations

from pathlib import Path

import pytest

from lsm_kv import fileops


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "db")


@pytest.fixture
def no_fsync(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip real fsyncs in tests that check logic, not durability.

    Durability is checked by the crash tests, which record every fsync and
    replay the writes themselves; this fixture only keeps long randomized runs
    fast on file systems where fsync costs milliseconds.
    """
    monkeypatch.setattr(fileops, "fsync", lambda fd: None)
    monkeypatch.setattr(fileops, "fsync_dir", lambda path: None)
