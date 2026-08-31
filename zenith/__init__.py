"""
ZenithDB - Zero-Dependency Multi-Model Storage Engine
Built 100% with the Python Standard Library.

Components:
- LSM-Tree Key-Value Store with Write-Ahead Logging (WAL) & SSTables
- Full-Text Search Engine with BM25 Probabilistic Ranking & Porter Stemmer
- Vector Similarity Index with Cosine/Euclidean Distance & Partitioning
- Document Store with JSONPath Filtering & Secondary Indexes
- ACID Transactions with Snapshot Isolation
- Redis RESP2 Protocol TCP Server & HTTP REST API with Live Web Dashboard
- ANSI Terminal UI (Tables, Progress Bars, Spinners, REPL)
"""

__version__ = "1.0.0"
__author__ = "ZenithDB Team"
__license__ = "MIT"

from zenith.storage.wal import WriteAheadLog, WALFrame, WALOpType
from zenith.storage.bloom import BloomFilter
from zenith.storage.sstable import SSTableWriter, SSTableReader
from zenith.storage.lsm import LSMTree
from zenith.engine.kv import KeyValueEngine
from zenith.engine.doc import DocumentStore
from zenith.engine.text import FullTextIndex
from zenith.engine.vector import VectorIndex
from zenith.engine.txn import TransactionManager, Transaction

__all__ = [
    "WriteAheadLog",
    "WALFrame",
    "WALOpType",
    "BloomFilter",
    "SSTableWriter",
    "SSTableReader",
    "LSMTree",
    "KeyValueEngine",
    "DocumentStore",
    "FullTextIndex",
    "VectorIndex",
    "TransactionManager",
    "Transaction",
    "__version__",
]
