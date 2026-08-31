"""
ZenithDB Bloom Filter Implementation
High-performance probabilistic set membership filter with Kirsch-Mitzenmacher double-hashing.
"""

import hashlib
import math
import struct
from typing import Optional, Union


class BloomFilter:
    """
    Space-efficient probabilistic set filter.
    
    Uses Kirsch-Mitzenmacher optimization to generate k independent hash locations
    from a single SHA-256 digest: g_i(x) = (h1 + i * h2) mod m.
    """

    def __init__(
        self,
        capacity: int = 10000,
        error_rate: float = 0.01,
        bit_count: Optional[int] = None,
        hash_count: Optional[int] = None,
        bit_array: Optional[bytearray] = None,
    ) -> None:
        self.capacity = max(1, capacity)
        self.error_rate = error_rate

        if bit_count is not None and hash_count is not None:
            self.m = bit_count
            self.k = hash_count
        else:
            # Optimal m = - (n * ln(p)) / (ln(2)^2)
            ln2_sq = (math.log(2)) ** 2
            self.m = int(math.ceil(- (self.capacity * math.log(self.error_rate)) / ln2_sq))
            self.m = max(8, self.m)  # At least 1 byte
            # Optimal k = (m / n) * ln(2)
            self.k = int(round((self.m / self.capacity) * math.log(2)))
            self.k = max(1, min(30, self.k))

        self.byte_count = (self.m + 7) // 8
        self.m = self.byte_count * 8  # Align to whole byte boundary

        if bit_array is not None:
            self.bits = bit_array
        else:
            self.bits = bytearray(self.byte_count)

        self.count = 0

    def _hashes(self, key: bytes):
        """Generates k hash positions using Kirsch-Mitzenmacher double hashing."""
        digest = hashlib.sha256(key).digest()
        h1, h2 = struct.unpack(">QQ", digest[:16])
        for i in range(self.k):
            yield (h1 + i * h2) % self.m

    def add(self, key: Union[bytes, str]) -> None:
        """Adds a key to the filter."""
        if isinstance(key, str):
            key = key.encode("utf-8")

        for pos in self._hashes(key):
            byte_idx = pos // 8
            bit_idx = pos % 8
            self.bits[byte_idx] |= 1 << bit_idx
        self.count += 1

    def contains(self, key: Union[bytes, str]) -> bool:
        """
        Returns True if the key might be in the set, False if definitely NOT in the set.
        """
        if isinstance(key, str):
            key = key.encode("utf-8")

        for pos in self._hashes(key):
            byte_idx = pos // 8
            bit_idx = pos % 8
            if not (self.bits[byte_idx] & (1 << bit_idx)):
                return False
        return True

    def __contains__(self, key: Union[bytes, str]) -> bool:
        return self.contains(key)

    def to_bytes(self) -> bytes:
        """
        Serializes filter to binary format:
        [Magic: 4B ("ZBLM")][Capacity: 4B][ErrorRate: 4B float][m: 4B][k: 2B][Bits: byte_count bytes]
        """
        header = struct.pack(
            ">4sIfIH",
            b"ZBLM",
            self.capacity,
            self.error_rate,
            self.m,
            self.k,
        )
        return header + bytes(self.bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        """Deserializes filter from binary format."""
        header_format = ">4sIfIH"
        header_size = struct.calcsize(header_format)
        if len(data) < header_size:
            raise ValueError("Invalid Bloom filter binary payload")

        magic, capacity, error_rate, m, k = struct.unpack(
            header_format, data[:header_size]
        )
        if magic != b"ZBLM":
            raise ValueError(f"Invalid Bloom filter magic: {magic}")

        bit_bytes = bytearray(data[header_size:])
        bf = cls(
            capacity=capacity,
            error_rate=error_rate,
            bit_count=m,
            hash_count=k,
            bit_array=bit_bytes,
        )
        return bf
