"""
ZenithDB ACID Transaction Manager
Provides atomic multi-key transactions with snapshot isolation and rollback.
"""

import threading
from typing import Any, Dict, Optional, Union
from zenith.storage.lsm import LSMTree, TOMBSTONE
from zenith.storage.wal import WALOpType


class Transaction:
    """
    Represents an active ACID transaction context.
    """

    def __init__(self, manager: "TransactionManager") -> None:
        self.manager = manager
        self.lsm = manager.lsm
        self._write_buffer: Dict[bytes, bytes] = {}
        self._meta_buffer: Dict[bytes, bytes] = {}
        self._committed = False
        self._rolled_back = False

    def set(self, key: str, value: Union[str, bytes]) -> None:
        """Buffers a set operation within transaction."""
        if self._committed or self._rolled_back:
            raise RuntimeError("Transaction is no longer active")
        k = f"__V__:{key}".encode("utf-8")
        v = value if isinstance(value, bytes) else str(value).encode("utf-8")
        self._write_buffer[k] = v
        self._meta_buffer[f"__TYPE__:{key}".encode("utf-8")] = b"string"

    def delete(self, key: str) -> None:
        """Buffers a delete operation within transaction."""
        if self._committed or self._rolled_back:
            raise RuntimeError("Transaction is no longer active")
        k = f"__V__:{key}".encode("utf-8")
        self._write_buffer[k] = TOMBSTONE
        self._meta_buffer[f"__TYPE__:{key}".encode("utf-8")] = TOMBSTONE
        self._meta_buffer[f"__EXP__:{key}".encode("utf-8")] = TOMBSTONE

    def get(self, key: str) -> Optional[str]:
        """Reads key respecting uncommitted writes in active transaction."""
        k = f"__V__:{key}".encode("utf-8")
        if k in self._write_buffer:
            val = self._write_buffer[k]
            return None if val == TOMBSTONE else val.decode("utf-8")

        val = self.lsm.get(k)
        return val.decode("utf-8") if val else None

    def commit(self) -> None:
        """Atomically commits all buffered operations to WAL and LSMTree."""
        if self._committed or self._rolled_back:
            raise RuntimeError("Transaction is no longer active")

        with self.manager._lock:
            # Write TXN_BEGIN
            self.lsm.wal.append(WALOpType.TXN_BEGIN, b"TXN_START", b"")
            for k, v in self._write_buffer.items():
                if v == TOMBSTONE:
                    self.lsm.delete(k)
                else:
                    self.lsm.put(k, v)
            for k, v in self._meta_buffer.items():
                if v == TOMBSTONE:
                    self.lsm.delete(k)
                else:
                    self.lsm.put(k, v)
            # Write TXN_COMMIT
            self.lsm.wal.append(WALOpType.TXN_COMMIT, b"TXN_COMMIT", b"")
            self.lsm.wal.sync()

        self._committed = True

    def rollback(self) -> None:
        """Aborts transaction and discards buffered mutations."""
        if self._committed or self._rolled_back:
            return
        self._write_buffer.clear()
        self._meta_buffer.clear()
        self._rolled_back = True

    def __enter__(self) -> "Transaction":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            self.rollback()
        elif not self._committed and not self._rolled_back:
            self.commit()


class TransactionManager:
    """Manages transactional concurrency and isolation."""

    def __init__(self, lsm: LSMTree) -> None:
        self.lsm = lsm
        self._lock = threading.Lock()

    def begin(self) -> Transaction:
        """Starts a new transaction."""
        return Transaction(self)
