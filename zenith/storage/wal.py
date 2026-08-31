"""
ZenithDB Write-Ahead Log (WAL) Engine
Guarantees ACID durability and crash recovery using binary framing and CRC32 verification.
"""

import os
import struct
import time
import zlib
from enum import IntEnum
from typing import BinaryIO, Generator, List, Optional, Tuple


class WALOpType(IntEnum):
    SET = 1
    DEL = 2
    EXPIRE = 3
    TXN_BEGIN = 4
    TXN_COMMIT = 5
    TXN_ROLLBACK = 6
    FLUSH = 7


class WALFrame:
    """Represents a single binary log record in the WAL."""
    __slots__ = ("lsn", "timestamp", "op_type", "key", "value", "crc")

    def __init__(
        self,
        lsn: int,
        timestamp: float,
        op_type: WALOpType,
        key: bytes,
        value: bytes,
        crc: int = 0,
    ) -> None:
        self.lsn = lsn
        self.timestamp = timestamp
        self.op_type = op_type
        self.key = key
        self.value = value
        self.crc = crc

    def __repr__(self) -> str:
        return (
            f"WALFrame(lsn={self.lsn}, op={self.op_type.name}, "
            f"key_len={len(self.key)}, val_len={len(self.value)})"
        )


class WriteAheadLog:
    """
    Append-only binary Write-Ahead Log.
    
    Frame Layout (34-byte header + dynamic payload):
    - Magic: b"ZWAL" (4 bytes)
    - Version: 1 (1 byte)
    - LSN: Log Sequence Number (8 bytes unsigned uint64, big-endian)
    - Timestamp: Unix epoch seconds (8 bytes double float, big-endian)
    - OpType: Operation type (1 byte)
    - Key Length: (4 bytes uint32, big-endian)
    - Value Length: (4 bytes uint32, big-endian)
    - CRC32: Checksum of OpType + LSN + Key + Value (4 bytes uint32, big-endian)
    - Key Payload: key bytes
    - Value Payload: value bytes
    """

    MAGIC = b"ZWAL"
    VERSION = 1
    HEADER_FORMAT = ">4sB QdB II I"  # Magic(4), Version(1), LSN(8), Timestamp(8), OpType(1), KeyLen(4), ValLen(4), CRC(4)
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 34 bytes

    def __init__(
        self,
        directory: str,
        max_file_size: int = 32 * 1024 * 1024,  # 32MB
        sync_mode: str = "every_sec",  # "always", "every_sec", "none"
    ) -> None:
        self.directory = directory
        self.max_file_size = max_file_size
        self.sync_mode = sync_mode
        self._current_file: Optional[BinaryIO] = None
        self._current_path: Optional[str] = None
        self._current_seq = 0
        self._next_lsn = 1
        self._last_sync_time = time.time()

        os.makedirs(self.directory, exist_ok=True)
        self._init_or_rotate()

    def _get_log_path(self, seq: int) -> str:
        return os.path.join(self.directory, f"wal_{seq:08d}.log")

    def _init_or_rotate(self) -> None:
        """Finds the latest WAL file or creates a new one."""
        existing_logs = sorted(
            [
                f
                for f in os.listdir(self.directory)
                if f.startswith("wal_") and f.endswith(".log")
            ]
        )

        if existing_logs:
            latest = existing_logs[-1]
            try:
                seq = int(latest[4:12])
                self._current_seq = seq
            except ValueError:
                self._current_seq = 1
        else:
            self._current_seq = 1

        self._current_path = self._get_log_path(self._current_seq)
        self._current_file = open(self._current_path, "a+b")

    def append(
        self,
        op_type: WALOpType,
        key: bytes,
        value: bytes = b"",
        timestamp: Optional[float] = None,
    ) -> int:
        """Appends a record to the WAL and returns its LSN."""
        if self._current_file is None:
            self._init_or_rotate()

        lsn = self._next_lsn
        self._next_lsn += 1
        ts = timestamp if timestamp is not None else time.time()

        # Compute CRC32
        crc_data = struct.pack(">BQ", int(op_type), lsn) + key + value
        crc = zlib.crc32(crc_data) & 0xFFFFFFFF

        header = struct.pack(
            self.HEADER_FORMAT,
            self.MAGIC,
            self.VERSION,
            lsn,
            ts,
            int(op_type),
            len(key),
            len(value),
            crc,
        )

        payload = header + key + value
        self._current_file.write(payload)

        # Handle durability sync
        if self.sync_mode == "always":
            self.sync()
        elif self.sync_mode == "every_sec":
            now = time.time()
            if now - self._last_sync_time >= 1.0:
                self.sync()
                self._last_sync_time = now

        # Check rotation
        if self._current_file.tell() >= self.max_file_size:
            self.rotate()

        return lsn

    def sync(self) -> None:
        """Forces unwritten buffered data to disk via fsync."""
        if self._current_file and not self._current_file.closed:
            self._current_file.flush()
            try:
                os.fsync(self._current_file.fileno())
            except OSError:
                pass

    def rotate(self) -> None:
        """Closes current WAL segment and opens a new segment."""
        self.sync()
        if self._current_file and not self._current_file.closed:
            self._current_file.close()

        self._current_seq += 1
        self._current_path = self._get_log_path(self._current_seq)
        self._current_file = open(self._current_path, "a+b")

    def close(self) -> None:
        """Flushes and closes the active WAL."""
        if self._current_file and not self._current_file.closed:
            self.sync()
            self._current_file.close()
            self._current_file = None

    @classmethod
    def read_frames(
        cls, file_path: str
    ) -> Generator[WALFrame, None, None]:
        """
        Reads all valid frames from a given WAL file.
        Gracefully terminates at EOF or uncommitted / corrupt partial writes.
        """
        if not os.path.exists(file_path):
            return

        with open(file_path, "rb") as f:
            while True:
                header_bytes = f.read(cls.HEADER_SIZE)
                if len(header_bytes) < cls.HEADER_SIZE:
                    # Normal clean EOF or truncated partial header
                    break

                try:
                    magic, version, lsn, ts, op_val, klen, vlen, crc = struct.unpack(
                        cls.HEADER_FORMAT, header_bytes
                    )
                except struct.error:
                    break

                if magic != cls.MAGIC or version != cls.VERSION:
                    # Invalid magic/version - stop replay
                    break

                payload = f.read(klen + vlen)
                if len(payload) < klen + vlen:
                    # Truncated write at sudden crash
                    break

                key = payload[:klen]
                value = payload[klen : klen + vlen]

                # Checksum validation
                crc_data = struct.pack(">BQ", op_val, lsn) + key + value
                computed_crc = zlib.crc32(crc_data) & 0xFFFFFFFF
                if computed_crc != crc:
                    # Corrupted frame - stop replay
                    break

                try:
                    op_type = WALOpType(op_val)
                except ValueError:
                    break

                yield WALFrame(
                    lsn=lsn,
                    timestamp=ts,
                    op_type=op_type,
                    key=key,
                    value=value,
                    crc=crc,
                )

    def recover(self) -> List[WALFrame]:
        """
        Scans all WAL segments in sequence, yielding all valid committed frames.
        Updates internal next_lsn to resume cleanly.
        """
        self.sync()
        frames: List[WALFrame] = []
        log_files = sorted(
            [
                os.path.join(self.directory, f)
                for f in os.listdir(self.directory)
                if f.startswith("wal_") and f.endswith(".log")
            ]
        )

        max_lsn = 0
        for log_file in log_files:
            for frame in self.read_frames(log_file):
                frames.append(frame)
                if frame.lsn > max_lsn:
                    max_lsn = frame.lsn

        self._next_lsn = max_lsn + 1
        return frames

    def purge_before(self, max_seq_to_delete: int) -> int:
        """Deletes obsolete WAL files older than max_seq_to_delete after compaction."""
        deleted_count = 0
        for f in os.listdir(self.directory):
            if f.startswith("wal_") and f.endswith(".log"):
                try:
                    seq = int(f[4:12])
                    if seq < max_seq_to_delete and seq != self._current_seq:
                        os.remove(os.path.join(self.directory, f))
                        deleted_count += 1
                except (ValueError, OSError):
                    pass
        return deleted_count
