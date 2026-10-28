"""docs/FORMAT.md must describe the bytes the code actually writes.

The hex examples in the document are parsed (hex bytes up to the first run of
two spaces on each line, annotations after it) and compared with output of
the encoders.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from lsm_kv.batch import encode_batch
from lsm_kv.entry import KIND_DELETE, KIND_PUT
from lsm_kv.manifest import Manifest, TableInfo, encode
from lsm_kv.sstable import TableWriter
from lsm_kv.wal import encode_record

DOC = Path(__file__).resolve().parent.parent / "docs" / "FORMAT.md"


def examples() -> dict[str, bytes]:
    found = {}
    for name, body in re.findall(r"```text\n# example: (\S+)\n(.*?)```", DOC.read_text(), re.S):
        hex_part = " ".join(re.split(r"\s{2,}", line.strip())[0] for line in body.splitlines())
        found[name] = bytes.fromhex(hex_part)
    return found


def test_document_has_the_examples() -> None:
    assert set(examples()) == {"wal-record", "table", "manifest-header"}


def test_log_record_example() -> None:
    payload = encode_batch(7, [(KIND_PUT, b"k", b"v"), (KIND_DELETE, b"x", b"")])
    assert examples()["wal-record"] == encode_record(payload)


def test_table_example(tmp_path: Path) -> None:
    writer = TableWriter(str(tmp_path / "t.sst"), block_size=4096, bloom_bits_per_key=10)
    writer.add(b"k", 8, KIND_PUT, b"v2")
    writer.add(b"k", 7, KIND_PUT, b"v")
    writer.add(b"x", 9, KIND_DELETE, b"")
    writer.finish()
    assert examples()["table"] == (tmp_path / "t.sst").read_bytes()


def test_manifest_example() -> None:
    data = encode(Manifest(5, 4, 9, (TableInfo(3, 0, 201, 3, 7, 9, b"k", b"x"),)))
    assert examples()["manifest-header"] == data[:20]
    (shown,) = re.findall(r"```json\n(.*?)\n```", DOC.read_text(), re.S)
    assert json.loads(shown) == json.loads(data[20:])
    assert shown.encode() == data[20:]
