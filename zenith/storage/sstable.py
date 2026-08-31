"""
ZenithDB SSTable (Sorted String Table) Implementation
Immutable on-disk table with binary sparse indexing and Bloom filter pruning.
"""

import bisect
import os
import struct
from typing import Generator, List, Optional, Tuple
from zenith.storage.bloom import BloomFilter

TOMBSTONE = b"__ZENITH_TOMBSTONE__"


class SSTableWriter:
    """
    Writes sorted key-value pairs into an immutable SSTable file.
    
    File Structure:
    [Header: 4B b"ZSST" + 1B Version]
    [Data Block: Sequence of (KeyLen, ValLen, Key, Value)]
    [Index Block: Sequence of (KeyLen, DataOffset, Key)]
    [Bloom Filter Block: Serialized BloomFilter]
    [Footer Block: Metadata, Index/Bloom Offsets, Min/Max Keys]
    [Trailer: 8B FooterOffset + 8B b"ZSSTFOOT"]
    """

    MAGIC = b"ZSST"
    VERSION = 1
    TRAILER_MAGIC = b"ZSSTFOOT"

    def __init__(self, file_path: str, index_interval: int = 16) -> None:
        self.file_path = file_path
        self.index_interval = index_interval
        self._temp_path = file_path + ".tmp"
        self._file = open(self._temp_path, "wb")
        self._entry_count = 0
        self._index_entries: List[Tuple[bytes, int]] = []
        self._bloom = BloomFilter(capacity=10000, error_rate=0.01)
        self._min_key: Optional[bytes] = None
        self._max_key: Optional[bytes] = None
        self._closed = False

        # Write header (5 bytes)
        self._file.write(self.MAGIC + bytes([self.VERSION]))

    def write_entry(self, key: bytes, value: bytes) -> None:
        """Writes a single (key, value) entry. Keys MUST be written in ascending sorted order."""
        if len(key) > 65535:
            raise ValueError("Key size exceeds maximum allowable 64KB limit")

        if self._min_key is None:
            self._min_key = key
        self._max_key = key

        offset = self._file.tell()
        if self._entry_count % self.index_interval == 0:
            self._index_entries.append((key, offset))

        self._bloom.add(key)

        # Write data frame: KeyLen(2B), ValLen(4B), Key, Value
        frame_hdr = struct.pack(">HI", len(key), len(value))
        self._file.write(frame_hdr + key + value)
        self._entry_count += 1

    def finish(self) -> None:
        """Finalizes SSTable file by writing index, bloom, footer and trailer."""
        if self._closed:
            return

        if self._entry_count == 0:
            self._file.close()
            if os.path.exists(self._temp_path):
                os.remove(self._temp_path)
            self._closed = True
            return

        # 1. Write Index Block
        index_offset = self._file.tell()
        index_buf = bytearray()
        index_buf.extend(struct.pack(">I", len(self._index_entries)))
        for k, off in self._index_entries:
            index_buf.extend(struct.pack(">HQ", len(k), off) + k)
        self._file.write(index_buf)
        index_size = len(index_buf)

        # 2. Write Bloom Filter Block
        bloom_offset = self._file.tell()
        bloom_bytes = self._bloom.to_bytes()
        self._file.write(bloom_bytes)
        bloom_size = len(bloom_bytes)

        # 3. Write Footer Block
        footer_offset = self._file.tell()
        min_k = self._min_key or b""
        max_k = self._max_key or b""

        footer_hdr = struct.pack(
            ">QQQQ I H",
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            self._entry_count,
            len(min_k),
        )
        footer_payload = footer_hdr + min_k + struct.pack(">H", len(max_k)) + max_k
        self._file.write(footer_payload)

        # 4. Write Trailer (16 bytes: FooterOffset + TRAILER_MAGIC)
        trailer = struct.pack(">Q8s", footer_offset, self.TRAILER_MAGIC)
        self._file.write(trailer)

        self._file.flush()
        self._file.close()
        self._closed = True

        # Atomic rename temp file to target path
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
        os.replace(self._temp_path, self.file_path)


class SSTableReader:
    """Fast binary reader for an SSTable file."""

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self._file = open(file_path, "rb")
        self._file_size = os.path.getsize(file_path)
        self._index_keys: List[bytes] = []
        self._index_offsets: List[int] = []
        self._bloom: Optional[BloomFilter] = None
        self.entry_count = 0
        self.min_key: bytes = b""
        self.max_key: bytes = b""
        self._index_offset = 0
        self._load_metadata()

    def _load_metadata(self) -> None:
        """Reads trailer, footer, sparse index, and Bloom filter into memory."""
        if self._file_size < 16:
            raise ValueError(f"SSTable file too small: {self.file_path}")

        # Read trailer (last 16 bytes)
        self._file.seek(self._file_size - 16)
        trailer_bytes = self._file.read(16)
        footer_offset, magic = struct.unpack(">Q8s", trailer_bytes)
        if magic != SSTableWriter.TRAILER_MAGIC:
            raise ValueError(f"Invalid SSTable trailer magic in {self.file_path}")

        # Read footer
        self._file.seek(footer_offset)
        hdr_fmt = ">QQQQ I H"
        hdr_size = struct.calcsize(hdr_fmt)
        hdr_data = self._file.read(hdr_size)
        (
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            self.entry_count,
            min_k_len,
        ) = struct.unpack(hdr_fmt, hdr_data)

        self._index_offset = index_offset
        self.min_key = self._file.read(min_k_len)
        max_k_len_data = self._file.read(2)
        max_k_len = struct.unpack(">H", max_k_len_data)[0]
        self.max_key = self._file.read(max_k_len)

        # Read Index Block
        self._file.seek(index_offset)
        idx_count_data = self._file.read(4)
        idx_count = struct.unpack(">I", idx_count_data)[0]
        for _ in range(idx_count):
            entry_hdr = self._file.read(10)  # KeyLen(2) + Offset(8)
            k_len, off = struct.unpack(">HQ", entry_hdr)
            k = self._file.read(k_len)
            self._index_keys.append(k)
            self._index_offsets.append(off)

        # Read Bloom Filter
        self._file.seek(bloom_offset)
        bloom_data = self._file.read(bloom_size)
        self._bloom = BloomFilter.from_bytes(bloom_data)

    def get(self, key: bytes) -> Optional[bytes]:
        """
        Retrieves value for key, or None if key is absent.
        Returns TOMBSTONE if key was deleted.
        """
        # 1. Range pruning
        if not self.min_key or key < self.min_key or key > self.max_key:
            return None

        # 2. Bloom filter pruning
        if self._bloom and key not in self._bloom:
            return None

        # 3. Binary search in sparse index
        if not self._index_keys:
            return None

        idx = bisect.bisect_right(self._index_keys, key) - 1
        idx = max(0, idx)
        start_offset = self._index_offsets[idx]

        # 4. Scan data block from start_offset up to index_offset
        self._file.seek(start_offset)
        while self._file.tell() < self._index_offset:
            hdr_bytes = self._file.read(6)
            if len(hdr_bytes) < 6:
                break
            klen, vlen = struct.unpack(">HI", hdr_bytes)
            curr_key = self._file.read(klen)
            curr_val = self._file.read(vlen)

            if curr_key == key:
                return curr_val
            elif curr_key > key:
                # Key passed, not present
                break

        return None

    def scan(
        self, start_key: Optional[bytes] = None, end_key: Optional[bytes] = None
    ) -> Generator[Tuple[bytes, bytes], None, None]:
        """Scans entries in range [start_key, end_key]."""
        if self.entry_count == 0:
            return

        if start_key is not None and start_key > self.max_key:
            return
        if end_key is not None and end_key < self.min_key:
            return

        if start_key is not None and self._index_keys:
            idx = bisect.bisect_right(self._index_keys, start_key) - 1
            idx = max(0, idx)
            offset = self._index_offsets[idx]
        else:
            offset = 5  # Skip 5-byte header

        self._file.seek(offset)
        for _ in range(self.entry_count):
            if self._file.tell() >= self._index_offset:
                break
            hdr_bytes = self._file.read(6)
            if len(hdr_bytes) < 6:
                break
            klen, vlen = struct.unpack(">HI", hdr_bytes)
            curr_key = self._file.read(klen)
            curr_val = self._file.read(vlen)

            if start_key is not None and curr_key < start_key:
                continue
            if end_key is not None and curr_key > end_key:
                break

            yield (curr_key, curr_val)

    def iter_all(self) -> Generator[Tuple[bytes, bytes], None, None]:
        """Iterates over all entries in the SSTable."""
        return self.scan()

    def close(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()
