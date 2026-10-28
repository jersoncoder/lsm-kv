"""A Bloom filter over the distinct user keys of one SSTable.

Each key is hashed once with BLAKE2b to 64 bits. The two 32-bit halves drive
double hashing (Kirsch and Mitzenmacher): probe ``i`` sets bit
``(h1 + i * h2) mod num_bits``. BLAKE2b is used because Python's built-in
``hash`` is randomized per process and so cannot be stored on disk.

Encoding: ``num_probes:u8  num_bits:u32  bits[ceil(num_bits / 8)]``, bit ``b``
being bit ``b % 8`` (least significant first) of byte ``b // 8``.
"""

from __future__ import annotations

import hashlib
import math
import struct
from collections.abc import Iterable
from typing import Final

_HEADER: Final = struct.Struct("<BI")
_MIN_BITS: Final = 64


def key_hash(key: bytes) -> int:
    """The 64-bit hash stored for a key while a table is being written."""
    return int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "little")


class BloomFilter:
    __slots__ = ("_bits", "_num_bits", "_num_probes")

    def __init__(self, num_bits: int, num_probes: int, bits: bytes | bytearray):
        if num_bits <= 0 or num_probes <= 0 or len(bits) != (num_bits + 7) // 8:
            raise ValueError("inconsistent Bloom filter parameters")
        self._num_bits = num_bits
        self._num_probes = num_probes
        self._bits = bytes(bits)

    @classmethod
    def build(cls, hashes: Iterable[int], bits_per_key: int) -> BloomFilter:
        hash_list = list(hashes)
        num_bits = max(_MIN_BITS, len(hash_list) * bits_per_key)
        # The false positive rate is lowest with bits_per_key * ln 2 probes.
        num_probes = min(30, max(1, round(bits_per_key * math.log(2))))
        bits = bytearray((num_bits + 7) // 8)
        for h in hash_list:
            h1, h2 = h & 0xFFFFFFFF, (h >> 32) | 1
            for i in range(num_probes):
                bit = (h1 + i * h2) % num_bits
                bits[bit >> 3] |= 1 << (bit & 7)
        return cls(num_bits, num_probes, bits)

    def may_contain(self, key: bytes) -> bool:
        """False means the key is certainly absent; True means it may be present."""
        h = key_hash(key)
        h1, h2 = h & 0xFFFFFFFF, (h >> 32) | 1
        num_bits, bits = self._num_bits, self._bits
        for i in range(self._num_probes):
            bit = (h1 + i * h2) % num_bits
            if not bits[bit >> 3] & (1 << (bit & 7)):
                return False
        return True

    def encode(self) -> bytes:
        return _HEADER.pack(self._num_probes, self._num_bits) + self._bits

    @classmethod
    def decode(cls, data: bytes) -> BloomFilter:
        """Parse an encoded filter. Raises ``ValueError`` if it is inconsistent."""
        if len(data) < _HEADER.size:
            raise ValueError("Bloom filter shorter than its header")
        num_probes, num_bits = _HEADER.unpack_from(data, 0)
        return cls(num_bits, num_probes, data[_HEADER.size :])
