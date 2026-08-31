"""
ZenithDB LSM-Tree (Log-Structured Merge Tree) Engine
Coordinates MemTable, WAL durability, multi-level SSTables, and compaction.
"""

import heapq
import os
import threading
import time
from typing import Dict, Generator, List, Optional, Tuple

from zenith.storage.sstable import SSTableReader, SSTableWriter, TOMBSTONE
from zenith.storage.wal import WALOpType, WriteAheadLog


class MemTable:
    """In-memory write buffer backed by a Python dict with size tracking."""

    def __init__(self, max_size_bytes: int = 4 * 1024 * 1024) -> None:
        self.max_size_bytes = max_size_bytes
        self._data: Dict[bytes, bytes] = {}
        self._byte_size = 0

    def put(self, key: bytes, value: bytes) -> None:
        old_val = self._data.get(key)
        if old_val is not None:
            self._byte_size -= len(key) + len(old_val)
        self._data[key] = value
        self._byte_size += len(key) + len(value) + 16

    def get(self, key: bytes) -> Optional[bytes]:
        return self._data.get(key)

    def delete(self, key: bytes) -> None:
        self.put(key, TOMBSTONE)

    def is_full(self) -> bool:
        return self._byte_size >= self.max_size_bytes

    def get_sorted_entries(self) -> List[Tuple[bytes, bytes]]:
        return sorted(self._data.items(), key=lambda item: item[0])

    def __len__(self) -> int:
        return len(self._data)

    @property
    def byte_size(self) -> int:
        return self._byte_size

    def clear(self) -> None:
        self._data.clear()
        self._byte_size = 0


class LSMTree:
    """
    Log-Structured Merge Tree storage engine.
    
    Provides ACID durability, high-throughput ingestion, multi-level SSTable hierarchy,
    and automatic background / on-demand compaction.
    """

    def __init__(
        self,
        data_dir: str,
        memtable_size_bytes: int = 4 * 1024 * 1024,
        wal_sync_mode: str = "every_sec",
    ) -> None:
        self.data_dir = data_dir
        self.memtable_size_bytes = memtable_size_bytes
        self.wal_dir = os.path.join(data_dir, "wal")
        self.sst_dir = os.path.join(data_dir, "sstables")
        self._lock = threading.RLock()

        os.makedirs(self.wal_dir, exist_ok=True)
        os.makedirs(self.sst_dir, exist_ok=True)

        self.wal = WriteAheadLog(self.wal_dir, sync_mode=wal_sync_mode)
        self.memtable = MemTable(memtable_size_bytes)
        self.immutable_memtable: Optional[MemTable] = None

        self._sst_readers: List[SSTableReader] = []
        self._next_sst_id = 1

        self._load_existing_sstables()
        self._recover_from_wal()

    def _get_sst_path(self, sst_id: int) -> str:
        return os.path.join(self.sst_dir, f"sst_{sst_id:08d}.sst")

    def _load_existing_sstables(self) -> None:
        """Loads and sorts all existing SSTable files from disk."""
        files = sorted(
            [
                f
                for f in os.listdir(self.sst_dir)
                if f.startswith("sst_") and f.endswith(".sst")
            ]
        )
        self._sst_readers = []
        max_id = 0
        for f in files:
            try:
                sst_id = int(f[4:12])
                if sst_id > max_id:
                    max_id = sst_id
                reader = SSTableReader(os.path.join(self.sst_dir, f))
                self._sst_readers.append(reader)
            except Exception:
                pass
        self._next_sst_id = max_id + 1

    def _recover_from_wal(self) -> None:
        """Replays committed WAL frames into MemTable."""
        frames = self.wal.recover()
        for frame in frames:
            if frame.op_type == WALOpType.SET:
                self.memtable.put(frame.key, frame.value)
            elif frame.op_type == WALOpType.DEL:
                self.memtable.delete(frame.key)

    def put(self, key: bytes, value: bytes) -> None:
        """Writes key-value pair to WAL and MemTable."""
        with self._lock:
            self.wal.append(WALOpType.SET, key, value)
            self.memtable.put(key, value)
            if self.memtable.is_full():
                self._flush_memtable()

    def delete(self, key: bytes) -> None:
        """Appends deletion tombstone to WAL and MemTable."""
        with self._lock:
            self.wal.append(WALOpType.DEL, key, TOMBSTONE)
            self.memtable.delete(key)
            if self.memtable.is_full():
                self._flush_memtable()

    def get(self, key: bytes) -> Optional[bytes]:
        """
        Reads a key with hierarchical lookup:
        Active MemTable -> Immutable MemTable -> SSTables (newest to oldest).
        """
        with self._lock:
            # 1. Active MemTable
            val = self.memtable.get(key)
            if val is not None:
                return None if val == TOMBSTONE else val

            # 2. Immutable MemTable
            if self.immutable_memtable:
                val = self.immutable_memtable.get(key)
                if val is not None:
                    return None if val == TOMBSTONE else val

            # 3. SSTables in reverse chronological order (newest first)
            for reader in reversed(self._sst_readers):
                val = reader.get(key)
                if val is not None:
                    return None if val == TOMBSTONE else val

            return None

    def _flush_memtable(self) -> None:
        """Flushes in-memory data to a new SSTable on disk."""
        if len(self.memtable) == 0:
            return

        self.immutable_memtable = self.memtable
        self.memtable = MemTable(self.memtable_size_bytes)

        entries = self.immutable_memtable.get_sorted_entries()
        sst_path = self._get_sst_path(self._next_sst_id)
        self._next_sst_id += 1

        writer = SSTableWriter(sst_path)
        for k, v in entries:
            writer.write_entry(k, v)
        writer.finish()

        reader = SSTableReader(sst_path)
        self._sst_readers.append(reader)

        # Rotate WAL
        self.wal.rotate()
        self.immutable_memtable = None

        # Check if compaction is needed (e.g. >= 4 SSTables)
        if len(self._sst_readers) >= 4:
            self.compact()

    def compact(self) -> None:
        """
        Performs K-way merge compaction of all SSTables.
        Purges duplicate keys and tombstone markers.
        """
        with self._lock:
            if len(self._sst_readers) < 2:
                return

            iterators = [reader.iter_all() for reader in self._sst_readers]

            # Merge all sorted streams
            # Key deduplication: keep only the latest version from the newest SSTable
            # Each item: (key, value, reader_index)
            heap = []
            for i, it in enumerate(iterators):
                try:
                    k, v = next(it)
                    heapq.heappush(heap, (k, -i, v, it))  # -i ensures newest reader wins on tie
                except StopIteration:
                    pass

            compact_sst_path = self._get_sst_path(self._next_sst_id)
            self._next_sst_id += 1
            writer = SSTableWriter(compact_sst_path)

            last_key: Optional[bytes] = None
            while heap:
                key, neg_idx, val, it = heapq.heappop(heap)
                # Advance iterator
                try:
                    next_k, next_v = next(it)
                    heapq.heappush(heap, (next_k, neg_idx, next_v, it))
                except StopIteration:
                    pass

                # If duplicate key from older sstable, skip
                if key == last_key:
                    continue
                last_key = key

                # Skip tombstones during compaction
                if val != TOMBSTONE:
                    writer.write_entry(key, val)

            writer.finish()

            # Close and remove old SSTable readers
            old_readers = self._sst_readers
            self._sst_readers = []
            for r in old_readers:
                r.close()
                try:
                    if os.path.exists(r.file_path):
                        os.remove(r.file_path)
                except OSError:
                    pass

            # Open compacted SSTable
            if os.path.exists(compact_sst_path):
                self._sst_readers.append(SSTableReader(compact_sst_path))

            # Purge obsolete WAL files
            self.wal.purge_before(self.wal._current_seq)

    def scan(
        self, start_key: Optional[bytes] = None, end_key: Optional[bytes] = None
    ) -> Generator[Tuple[bytes, bytes], None, None]:
        """
        K-way merged range scan across MemTable and all SSTables.
        """
        with self._lock:
            # Collect all active key-value pairs
            combined: Dict[bytes, bytes] = {}

            # 1. SSTables from oldest to newest
            for reader in self._sst_readers:
                for k, v in reader.scan(start_key, end_key):
                    combined[k] = v

            # 2. Immutable MemTable
            if self.immutable_memtable:
                for k, v in self.immutable_memtable.get_sorted_entries():
                    if (start_key is None or k >= start_key) and (
                        end_key is None or k <= end_key
                    ):
                        combined[k] = v

            # 3. Active MemTable
            for k, v in self.memtable.get_sorted_entries():
                if (start_key is None or k >= start_key) and (
                    end_key is None or k <= end_key
                ):
                    combined[k] = v

            # Yield sorted and non-tombstone pairs
            for k in sorted(combined.keys()):
                v = combined[k]
                if v != TOMBSTONE:
                    yield (k, v)

    def flush(self) -> None:
        """Forces MemTable flush and WAL sync to disk."""
        with self._lock:
            self._flush_memtable()
            self.wal.sync()

    def close(self) -> None:
        """Closes all SSTable handles and the active WAL."""
        with self._lock:
            self.flush()
            for reader in self._sst_readers:
                reader.close()
            self.wal.close()
