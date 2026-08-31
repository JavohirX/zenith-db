# Zenith Package Architecture (`architecture.md`)

> **Directory**: `/zenith`  
> **Purpose**: Core Python package for ZenithDB, organizing modular subsystems into specialized namespaces.

---

## 🤖 AI Agent Maintenance Instructions
Whenever ANY file is created, modified, renamed, or deleted in this directory or its subdirectories:
1. Update this `architecture.md` and the relevant subdirectory `architecture.md`.
2. Ensure all exported symbols in `__init__.py` match the underlying implementations.
3. Re-run `python tools/bundle.py` to regenerate the standalone `zenith.py`.
4. Run `python tests/run_all.py` to verify test coverage.

---

## 📄 File Inventory & Technical Specifications

### `__init__.py`
- **Role**: Package root exposing the public programmatic API of ZenithDB.
- **Exports**:
  - `WriteAheadLog`, `WALFrame`, `WALOpType` (from `zenith.storage.wal`)
  - `BloomFilter` (from `zenith.storage.bloom`)
  - `SSTableWriter`, `SSTableReader` (from `zenith.storage.sstable`)
  - `LSMTree` (from `zenith.storage.lsm`)
  - `KeyValueEngine` (from `zenith.engine.kv`)
  - `DocumentStore` (from `zenith.engine.doc`)
  - `FullTextIndex` (from `zenith.engine.text`)
  - `VectorIndex` (from `zenith.engine.vector`)
  - `TransactionManager`, `Transaction` (from `zenith.engine.txn`)
- **Metadata**: `__version__ = "1.0.0"`, `__license__ = "MIT"`.

---

## 📁 Subdirectory Summaries (Abstract Tree)

- **[`storage/`](file:///D:/source/hack1/zenith/storage/architecture.md)**: Low-level storage subsystem implementing the LSM-Tree, Write-Ahead Log (WAL), binary SSTables, and Bloom filters.
- **[`engine/`](file:///D:/source/hack1/zenith/engine/architecture.md)**: Multi-model database query engines: Key-Value (Strings, Hashes, Lists, Sets, ZSets, TTL), JSON Document Store, Full-Text BM25 Search, Vector Similarity Index, and ACID Transactions.
- **[`protocol/`](file:///D:/source/hack1/zenith/protocol/architecture.md)**: Redis Serialization Protocol (RESP2/RESP3) streaming parser and serializer.
- **[`server/`](file:///D:/source/hack1/zenith/server/architecture.md)**: Network servers: Asyncio TCP RESP server (`:6379`) and multi-threaded HTTP/1.1 REST/Dashboard server (`:8080`).
- **[`cli/`](file:///D:/source/hack1/zenith/cli/architecture.md)**: Terminal UI rendering engine (tables, progress bars, ANSI styling), interactive REPL, multi-threaded benchmarking suite, and CLI dispatching.
