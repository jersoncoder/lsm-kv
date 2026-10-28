from __future__ import annotations

import pytest

from lsm_kv.bloom import BloomFilter, key_hash


def keys(prefix: str, n: int) -> list[bytes]:
    return [f"{prefix}{i}".encode() for i in range(n)]


def test_no_false_negatives() -> None:
    present = keys("key", 5000)
    bloom = BloomFilter.build((key_hash(k) for k in present), bits_per_key=10)
    assert all(bloom.may_contain(k) for k in present)


def test_false_positive_rate_matches_the_bits_per_key() -> None:
    # Theory gives about 0.8% at 10 bits per key with 7 probes.
    bloom = BloomFilter.build((key_hash(k) for k in keys("in", 10000)), bits_per_key=10)
    absent = keys("out", 20000)
    rate = sum(bloom.may_contain(k) for k in absent) / len(absent)
    assert rate < 0.02


def test_encoding_round_trip() -> None:
    bloom = BloomFilter.build((key_hash(k) for k in keys("k", 100)), bits_per_key=8)
    decoded = BloomFilter.decode(bloom.encode())
    assert decoded.encode() == bloom.encode()
    assert all(decoded.may_contain(k) for k in keys("k", 100))


def test_empty_filter_rejects_everything() -> None:
    bloom = BloomFilter.build([], bits_per_key=10)
    assert not any(bloom.may_contain(k) for k in keys("x", 100))


@pytest.mark.parametrize("data", [b"", b"\x07", b"\x07\x40\x00\x00\x00" + b"\x00" * 7])
def test_decoding_rejects_inconsistent_filters(data: bytes) -> None:
    with pytest.raises(ValueError, match="Bloom filter"):
        BloomFilter.decode(data)


def test_hash_is_a_fixed_function_of_the_key() -> None:
    # Filters are stored on disk, so the hash must not depend on the process
    # (Python's built-in hash of bytes is randomized per process).
    assert key_hash(b"lsm") == 0x2511765B50F448D
