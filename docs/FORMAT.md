# On-disk format

This document specifies every file lsm-kv writes, byte by byte. All integers
are unsigned and little-endian. Every checksum is CRC-32 with the IEEE 802.3
polynomial, as computed by Python's `zlib.crc32`. The hex examples are
produced by the code: `tests/test_format_doc.py` rebuilds each one and fails
if this document and the implementation disagree.

A database is a directory:

| File | Purpose |
| --- | --- |
| `MANIFEST` | The live tables and log positions. Replaced atomically. |
| `NNNNNN.log` | Write-ahead logs. Only logs numbered at or above the manifest's `log_number` are live. |
| `NNNNNN.sst` | Tables. Only tables listed in the manifest are live. |
| `LOCK` | Empty file locked with `flock` while an instance has the database open. |
| `MANIFEST.tmp` | Exists only while a new manifest is being written. |

File numbers come from one counter (`next_file_number`). On open it is raised
past every number found in the directory, so a new file never takes the name of
an existing one (the number of a leftover deleted on open can be handed out
again later).
On open, leftovers of an interrupted flush or compaction (`MANIFEST.tmp`,
tables not in the manifest, logs below `log_number`) are deleted, but only
after the manifest, every live table and every live log have been verified.

## Entries and sequence numbers

Every write gets a sequence number, a 64-bit counter that increases by one
per operation. A version of a key is the tuple `(key, seq, kind, value)` where
`kind` is `1` for a value and `0` for a deletion (a tombstone, whose value is
empty). Versions are ordered by key (bytewise), then by sequence number
descending, so the newest version of a key comes first. Tables store versions
in that order.

## Write-ahead log

A log file is a sequence of records with no file header. Each record is one
write batch.

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 4 | `length`: payload length in bytes |
| 4 | 4 | `length_crc`: CRC-32 of the four `length` bytes |
| 8 | 4 | `payload_crc`: CRC-32 of the payload |
| 12 | `length` | payload |

The payload is a batch:

| Size | Field |
| --- | --- |
| 8 | `seq`: sequence number of the first operation |
| 4 | `count`: number of operations, at least 1 |
| ... | `count` operations |

Each operation is `kind:u8 key_len:u32 value_len:u32 key value`, with kind `1`
(put) or `0` (delete, `value_len` must be 0). Operation `i` of the batch has
sequence number `seq + i`.

Example: a batch at sequence number 7 that puts `k = v` and deletes `x`.

```text
# example: wal-record
21 00 00 00                 length = 33
47 17 ca 39                 length_crc
bb 7a e0 38                 payload_crc
07 00 00 00 00 00 00 00     seq = 7
02 00 00 00                 count = 2
01 01 00 00 00 01 00 00 00  put, key_len = 1, value_len = 1
6b 76                       "k", "v"
00 01 00 00 00 00 00 00 00  delete, key_len = 1, value_len = 0
78                          "x"
```

### Reading a log during recovery

Records are read in order. A record is intact when at least 12 bytes remain,
`length_crc` matches, the payload fits in the file and `payload_crc` matches.
For the first record that is not intact, at offset `p`:

* fewer than 12 bytes remain: torn tail;
* `length_crc` matches and the record extends past the end of the file, or
  ends exactly at it with a bad `payload_crc`: torn tail;
* `length_crc` matches, `payload_crc` does not, and bytes follow the record:
  corruption;
* `length_crc` does not match: corruption if an intact record starts at any
  offset after `p`, torn tail otherwise (zero-filled or garbage tails).

A torn tail is cut off (the file is truncated to `p` and fsynced) and is
allowed only in the newest live log. Corruption raises `CorruptionError` with
the file and offset. Each record's `seq` must also be exactly one more than
the last sequence number before it (the manifest's `last_sequence` for the
first record), otherwise the log is corrupt.

## Tables (`.sst`)

```text
+--------------+-----+------------------+--------------+------------------+-------------+--------+
| data block 0 | ... | data block n - 1 | filter block | properties block | index block | footer |
+--------------+-----+------------------+--------------+------------------+-------------+--------+
```

Blocks are contiguous and in this order; the first data block starts at
offset 0 and the footer is the last 52 bytes of the file.

### Block framing

| Size | Field |
| --- | --- |
| 1 | `type`: 1 data, 2 filter, 3 properties, 4 index |
| ... | body |
| 4 | `crc`: CRC-32 of the type byte and the body |

Block offsets and sizes (in the index and the footer) cover the framing. A
reader checks the CRC and the type of every block it reads, every time it
reads it, so damaged data raises `CorruptionError` instead of being returned.

### Data block

The body is a run of entries in internal order, nothing else:

| Size | Field |
| --- | --- |
| 4 | `key_len` |
| 4 | `value_len` |
| 8 | `seq` |
| 1 | `kind`: 1 value, 0 tombstone |
| `key_len` | key |
| `value_len` | value |

The writer closes a block once its body reaches `Options.block_size` bytes, so
every block except the last has at least that many bytes of entries (a single
large entry makes a large block). Keys are stored in full (no prefix
compression) and blocks are not compressed.

### Filter block

A Bloom filter over the table's distinct keys:

| Size | Field |
| --- | --- |
| 1 | `num_probes` |
| 4 | `num_bits` |
| `ceil(num_bits / 8)` | bit array; bit `b` is bit `b % 8` (least significant first) of byte `b // 8` |

For a key, `h` is the first 8 bytes of BLAKE2b(key, digest size 8) read as a
little-endian integer, `h1 = h & 0xffffffff` and `h2 = (h >> 32) | 1`. Probe
`i` (for `i` in `0 .. num_probes - 1`) tests bit `(h1 + i * h2) mod num_bits`.
The writer uses `num_bits = max(64, distinct_keys * bits_per_key)` and
`num_probes = round(bits_per_key * ln 2)`, clamped to 1..30.

### Properties block

| Size | Field |
| --- | --- |
| 8 | `entry_count` |
| 8 | `min_seq` |
| 8 | `max_seq` |
| 4 + n | `smallest` key, length-prefixed |
| 4 + n | `largest` key, length-prefixed |

### Index block

One entry per data block, in order. It is sparse: it holds the last version
in each block, not every key.

| Size | Field |
| --- | --- |
| 4 | `key_len` |
| 8 | `seq` of the block's last entry |
| 8 | block offset |
| 4 | block size, including framing |
| `key_len` | key of the block's last entry |

To find the newest version of key `K` visible at sequence number `S`, the
reader binary-searches the index for the first block whose last entry is at
or after `(K, S)` in internal order, then binary-searches inside that block.

### Footer

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | filter block offset |
| 8 | 4 | filter block size |
| 12 | 8 | properties block offset |
| 20 | 4 | properties block size |
| 24 | 8 | index block offset |
| 32 | 4 | index block size |
| 36 | 4 | format version, currently 1 |
| 40 | 4 | CRC-32 of bytes 0 to 39 |
| 44 | 8 | magic `LSMKVSST` |

On open the reader checks, in order: the file is at least 52 bytes, the magic,
the footer CRC, the format version (a different one raises
`UnsupportedFormatError`), that the metadata blocks and footer are contiguous,
the filter, properties and index blocks (CRC and type), that the index covers
the data blocks exactly, and that the last index key is the properties'
`largest`. When reading a data block it also checks that its last entry is the
one the index recorded for it.

### Example

A table holding `put k = v2` at 8, `put k = v` at 7 and `delete x` at 9 (one
data block; `bits_per_key = 10`).

```text
# example: table
01                          data block
01 00 00 00 02 00 00 00     key_len = 1, value_len = 2
08 00 00 00 00 00 00 00 01  seq = 8, put
6b 76 32                    "k", "v2"
01 00 00 00 01 00 00 00     key_len = 1, value_len = 1
07 00 00 00 00 00 00 00 01  seq = 7, put
6b 76                       "k", "v"
01 00 00 00 00 00 00 00     key_len = 1, value_len = 0
09 00 00 00 00 00 00 00 00  seq = 9, delete
78                          "x"
6a 25 3f 6a                 crc
02                          filter block
07 40 00 00 00              7 probes, 64 bits
0c 06 81 40 a1 50 20 10     bit array
95 d7 fd d5                 crc
03                          properties block
03 00 00 00 00 00 00 00     entry_count = 3
07 00 00 00 00 00 00 00     min_seq = 7
09 00 00 00 00 00 00 00     max_seq = 9
01 00 00 00 6b              smallest = "k"
01 00 00 00 78              largest = "x"
a6 bd 22 21                 crc
04                          index block
01 00 00 00                 key_len = 1
09 00 00 00 00 00 00 00     seq = 9
00 00 00 00 00 00 00 00     offset = 0
3e 00 00 00                 size = 62
78                          "x"
e3 cd 14 da                 crc
3e 00 00 00 00 00 00 00     footer: filter offset = 62
12 00 00 00                 filter size = 18
50 00 00 00 00 00 00 00     properties offset = 80
27 00 00 00                 properties size = 39
77 00 00 00 00 00 00 00     index offset = 119
1e 00 00 00                 index size = 30
01 00 00 00                 format version = 1
f9 d3 a8 5b                 footer crc
4c 53 4d 4b 56 53 53 54     "LSMKVSST"
```

## Manifest

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | magic `LSMKVMAN` |
| 8 | 4 | format version, currently 1 |
| 12 | 4 | payload length |
| 16 | 4 | CRC-32 of bytes 8 to 15 followed by the payload |
| 20 | length | payload: UTF-8 JSON |

The payload is an object with sorted keys and no whitespace:

| Field | Meaning |
| --- | --- |
| `next_file_number` | Lower bound for the next file number. |
| `log_number` | Logs numbered below this are fully contained in tables. |
| `last_sequence` | Highest sequence number stored in tables. |
| `tables` | Live tables, newest first, each with `number`, `tier`, `size` (bytes), `entry_count`, `min_seq`, `max_seq`, and `smallest` and `largest` keys in hex. |

Open checks that the tables are ordered by tier, then newest first, with
disjoint sequence number ranges; that each table's file exists, has the
recorded size and agrees with its own properties block.

The manifest is never edited in place. A new one is written to
`MANIFEST.tmp`, fsynced, renamed over `MANIFEST`, and the directory is
fsynced. Example, for a database with one table:

```text
# example: manifest-header
4c 53 4d 4b 56 4d 41 4e     "LSMKVMAN"
01 00 00 00                 format version = 1
aa 00 00 00                 payload length = 170
d8 0b 6a a7                 crc
```

```json
{"last_sequence":9,"log_number":4,"next_file_number":5,"tables":[{"entry_count":3,"largest":"78","max_seq":9,"min_seq":7,"number":3,"size":201,"smallest":"6b","tier":0}]}
```
