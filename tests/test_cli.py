from __future__ import annotations

import pytest

from lsm_kv.__main__ import main


def run(capsys: pytest.CaptureFixture[str], *argv: str) -> tuple[int, str, str]:
    code = main(list(argv))
    out, err = capsys.readouterr()
    return code, out, err


def test_put_get_scan_delete(db_path: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert run(capsys, db_path, "put", "b", "2")[0] == 0
    assert run(capsys, db_path, "put", "a", "1")[0] == 0
    assert run(capsys, db_path, "put", "c", "3")[0] == 0
    assert run(capsys, db_path, "get", "a") == (0, "1\n", "")
    assert run(capsys, db_path, "scan") == (0, "a\t1\nb\t2\nc\t3\n", "")
    assert run(capsys, db_path, "scan", "--start", "b", "--limit", "1") == (0, "b\t2\n", "")
    assert run(capsys, db_path, "scan", "--end", "b") == (0, "a\t1\n", "")
    assert run(capsys, db_path, "delete", "a")[0] == 0
    assert run(capsys, db_path, "get", "a") == (1, "", "not found\n")
    assert run(capsys, db_path, "compact")[0] == 0
    assert run(capsys, db_path, "stats") == (0, "tier 0: 1 table(s)\n", "")


def test_hex_mode(db_path: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert run(capsys, db_path, "--hex", "put", "00ff", "0102")[0] == 0
    assert run(capsys, db_path, "--hex", "get", "00ff") == (0, "0102\n", "")
    assert run(capsys, db_path, "get", "\x00\xff")[0] == 1  # text mode encodes as UTF-8


def test_read_commands_do_not_create_a_database(
    db_path: str, capsys: pytest.CaptureFixture[str]
) -> None:
    code, _, err = run(capsys, db_path, "get", "a")
    assert code == 2
    assert "no database" in err
