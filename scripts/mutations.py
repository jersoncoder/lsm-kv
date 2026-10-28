"""Check that the tests notice when a step of the durability protocol is missing.

Each mutation removes one safeguard (an fsync, a directory fsync, a checksum
check, the torn-tail truncation, ...) from a temporary copy of the package,
then runs the log, manifest, crash and corruption tests against that copy. Every
mutation should make them fail; one that survives marks a safeguard the tests
cannot see. Exits with status 1 if any mutation survives.

    python scripts/mutations.py
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTS = [
    "tests/test_wal.py",
    "tests/test_manifest.py",
    "tests/test_crash.py",
    "tests/test_corruption.py",
]

# (what is removed, file, original text, replacement)
MUTATIONS = [
    (
        "fsync of a new table",
        "sstable.py",
        "        fileops.fsync(fd)\n        fileops.close(fd)",
        "        fileops.close(fd)",
    ),
    (
        "directory fsync after writing a table",
        "db.py",
        "            raise\n        fileops.fsync_dir(self._path)\n",
        "            raise\n",
    ),
    (
        "fsync of the new manifest before the rename",
        "manifest.py",
        "        fileops.write(fd, data)\n        fileops.fsync(fd)",
        "        fileops.write(fd, data)",
    ),
    (
        "directory fsync after the manifest rename",
        "manifest.py",
        "    fileops.rename(temp, os.path.join(directory, MANIFEST_NAME))\n"
        "    fileops.fsync_dir(directory)",
        "    fileops.rename(temp, os.path.join(directory, MANIFEST_NAME))",
    ),
    (
        "fsync of the old log when a flush switches logs",
        "db.py",
        "            self._wal.sync()\n            new_number = self._new_file_number()",
        "            new_number = self._new_file_number()",
    ),
    (
        "directory fsync after creating a log on open",
        "db.py",
        "                self._wal = wal.LogWriter.create(os.path.join(path, log_name(number)))\n"
        "                fileops.fsync_dir(path)",
        "                self._wal = wal.LogWriter.create(os.path.join(path, log_name(number)))",
    ),
    (
        "fsync of the log under EVERY_WRITE",
        "db.py",
        "        if policy is SyncPolicy.EVERY_WRITE:\n            self._wal.sync()",
        "        if policy is SyncPolicy.EVERY_WRITE:\n            pass",
    ),
    (
        "truncation of a torn log tail on open",
        "db.py",
        "                if contents.torn:\n",
        "                if False:\n",
    ),
    (
        "dropping a damaged last log record (treated as corruption instead)",
        "wal.py",
        "        return pos + HEADER_SIZE + length >= size",
        "        return pos + HEADER_SIZE + length > size",
    ),
    (
        "replay of every live log, not only the newest",
        "db.py",
        "_replay_logs(path, state, logs_on_disk)",
        "_replay_logs(path, state, logs_on_disk[-1:])",
    ),
    (
        "sequence number continuity check in the log",
        "db.py",
        "            if seq != last_seq + 1:",
        "            if seq < last_seq + 1:",
    ),
    (
        "refusal of a torn log that is not the newest",
        "db.py",
        "        if log.torn and i != len(live) - 1:",
        "        if False:",
    ),
    (
        "table block checksum check",
        "sstable.py",
        "        if zlib.crc32(memoryview(data)[: size - _U32.size]) != stored_crc:",
        "        if False:",
    ),
    (
        "manifest checksum check",
        "manifest.py",
        "    if zlib.crc32(data[8:16] + data[_HEADER.size :]) != stored_crc:",
        "    if False:",
    ),
]


def run_tests(package_parent: Path) -> bool:
    """Run the tests against the package copy; return True if they pass."""
    env = dict(os.environ, PYTHONPATH=str(package_parent))
    check = subprocess.run(
        [sys.executable, "-c", "import lsm_kv; print(lsm_kv.__file__)"],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert check.stdout.startswith(str(package_parent)), check.stdout
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *TESTS],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def main() -> int:
    survivors = []
    with tempfile.TemporaryDirectory() as tmp:
        copy = Path(tmp, "src")
        for description, name, old, new in MUTATIONS:
            shutil.rmtree(copy, ignore_errors=True)
            shutil.copytree(ROOT / "src", copy)
            target = copy / "lsm_kv" / name
            source = target.read_text()
            if source.count(old) != 1:
                print(f"cannot apply: {description}")
                return 2
            target.write_text(source.replace(old, new))
            caught = not run_tests(copy)
            print(f"{'caught  ' if caught else 'SURVIVED'}  {description}")
            if not caught:
                survivors.append(description)
        shutil.rmtree(copy, ignore_errors=True)
        shutil.copytree(ROOT / "src", copy)
        if not run_tests(copy):
            print("the unmodified code fails the tests")
            return 2
    print(f"{len(MUTATIONS) - len(survivors)} of {len(MUTATIONS)} mutations caught")
    return 1 if survivors else 0


if __name__ == "__main__":
    sys.exit(main())
