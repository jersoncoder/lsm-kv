"""A small command line interface: ``python -m lsm_kv PATH COMMAND ...``.

Keys and values are given and printed as UTF-8 text, or as hex with ``--hex``.
"""

from __future__ import annotations

import argparse
import sys

from . import DB, LSMKVError, Options, SyncPolicy


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m lsm_kv", description=__doc__.splitlines()[0])
    parser.add_argument("path", help="database directory")
    parser.add_argument("--hex", action="store_true", help="read and print keys and values as hex")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("get", help="print the value of a key").add_argument("key")
    put = commands.add_parser("put", help="set a key")
    put.add_argument("key")
    put.add_argument("value")
    commands.add_parser("delete", help="delete a key").add_argument("key")
    scan = commands.add_parser("scan", help="print keys and values in key order")
    scan.add_argument("--start", help="first key (inclusive)")
    scan.add_argument("--end", help="last key (exclusive)")
    scan.add_argument("--limit", type=int, help="print at most this many entries")
    commands.add_parser("compact", help="merge every table into one")
    commands.add_parser("stats", help="print table counts per tier")
    args = parser.parse_args(argv)

    def parse(text: str) -> bytes:
        return bytes.fromhex(text) if args.hex else text.encode()

    def bound(text: str | None) -> bytes | None:
        return None if text is None else parse(text)

    def show(data: bytes) -> str:
        return data.hex() if args.hex else data.decode(errors="backslashreplace")

    readonly = args.command in ("get", "scan", "stats")
    options = Options(sync=SyncPolicy.EVERY_WRITE, create_if_missing=not readonly)
    try:
        db = DB.open(args.path, options)
    except LSMKVError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    with db:
        if args.command == "get":
            value = db.get(parse(args.key))
            if value is None:
                print("not found", file=sys.stderr)
                return 1
            print(show(value))
        elif args.command == "put":
            db.put(parse(args.key), parse(args.value))
        elif args.command == "delete":
            db.delete(parse(args.key))
        elif args.command == "scan":
            with db.scan(bound(args.start), bound(args.end)) as entries:
                for count, (key, value) in enumerate(entries):
                    if args.limit is not None and count >= args.limit:
                        break
                    print(f"{show(key)}\t{show(value)}")
        elif args.command == "compact":
            db.compact()
        else:
            for tier, tables in db.stats().tables_per_tier.items():
                print(f"tier {tier}: {tables} table(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
