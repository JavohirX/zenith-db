# Storage Subsystem Architecture (`architecture.md`)

> **Directory**: `/zenith/storage`  
> **Purpose**: Physical storage layer providing ACID durability, append-only write-ahead logging, immutable Sorted String Tables (SSTables), Bloom filter indexing, and Log-Structured Merge Tree (LSM-Tree) compaction.

---

## 🤖 AI Agent Maintenance Instructions
Whenever ANY file is created, modified, renamed, or deleted in this directory:
1. Update this `architecture.md` immediately with exact binary layout changes, offsets, or framing adjustments.
2. Verify binary backward compatibility and crash recovery resilience tests in `tests/test_storage.py`.
3. Re-run `python tools/bundle.py` and `python tests/run_all.py`.

---

## 📄 File Inventory & Technical Specifications

### `wal.py` (Write-Ahead Log)
- **Role**: Append-only log recording mutations prior to memory state modifications, guaranteeing zero data loss.
- **Binary Frame Layout** (34 bytes header + dynamic payload):
  - `Magic`: `b"ZWAL"` (4 bytes)
  - `Version`: `1` (1 byte)
  - `LSN`: Log Sequence Number (8 bytes unsigned uint64, `>Q`)
  - `Timestamp`: Epoch seconds (8 bytes double float, `>d`)
  - `OpType`: Enum uint8 (`1=SET`, `2=DEL`, `3=EXPIRE`, `4=TXN_BEGIN`, `5=TXN_COMMIT`, `6=TXN_ROLLBACK`, `7=FLUSH`)
  - `KeyLen`: `>I` (4 bytes uint32)
  - `ValLen`: `>I` (4 bytes uint32)
  - `CRC32`: Checksum computed over `OpType + LSN + Key + Value` (4 bytes uint32, `>I`)
  - `KeyPayload`: Raw UTF-8 or binary key bytes
  - `ValPayload`: Raw value bytes
- **Classes**:
  - `WALOpType`: Integer enumeration of operation types.
  - `WALFrame`: In-memory container representing a single parsed log frame.
  - `WriteAheadLog`: Manages active log segment file, appending, `os.fsync` synchronization (`always`, `every_sec`, `none`), segment rotation, recovery replay, and obsolete segment purging.
- **Crash Recovery Algorithm**: Reads sequentially. If header or CRC mismatch occurs (e.g., from power loss mid-write), cleanly stops at the last committed frame and isolates valid state.

### `bloom.py` (Bloom Filter)
- **Role**: Probabilistic membership filter preventing unnecessary disk seeks for non-existent keys ($<1\%$ false-positive rate).
- **Algorithm**: Kirsch-Mitzenmacher double-hashing technique:
  $$g_i(x) = (h_1(x) + i \cdot h_2(x)) \pmod m$$
  where $h_1, h_2$ are two 64-bit integers extracted from a single `hashlib.sha256(key).digest()`.
- **Classes**:
  - `BloomFilter`: Bit array backed by `bytearray`. Methods: `add(key)`, `contains(key)`, `to_bytes()`, `from_bytes()`.
- **Binary Serialization Format**:
  - `[Magic: 4B ("ZBLM")][Capacity: 4B][ErrorRate: 4B float][m: 4B][k: 2B][Bits: byte_count bytes]`.

### `sstable.py` (Sorted String Table)
- **Role**: Immutable on-disk sorted table storing data records with sparse index and Bloom filter.
- **File Structure**:
  1. **Header**: `b"ZSST"` (4 bytes) + `Version: 1B`.
  2. **Data Block**: Continuous sequence of records: `[KeyLen: 2B (>H)][ValLen: 4B (>I)][KeyBytes][ValBytes]`.
  3. **Index Block**: Sparse index entries recording byte offset every $N$ keys (default: 16): `[IndexCount: 4B]` + repeated `[KeyLen: 2B][Offset: 8B (>Q)][KeyBytes]`.
  4. **Bloom Filter Block**: Serialized BloomFilter byte buffer.
  5. **Footer Block**: `[IndexOffset: 8B][IndexSize: 8B][BloomOffset: 8B][BloomSize: 8B][EntryCount: 4B][MinKeyLen: 2B][MinKey][MaxKeyLen: 2B][MaxKey]`.
  6. **Trailer**: `[FooterOffset: 8B][TrailerMagic: 8B ("ZSSTFOOT")]`.
- **Classes**:
  - `SSTableWriter`: Streams sorted key-value pairs to temp file, generates index, bloom, footer, and atomically renames.
  - `SSTableReader`: Reads trailer and footer on open, caches sparse index and Bloom filter in memory. Uses `bisect` binary search for $O(\log N)$ point lookups, and supports sequential range scans.

### `lsm.py` (Log-Structured Merge Tree Coordinator)
- **Role**: Central coordinator managing in-memory MemTables, WAL persistence, multi-level SSTables, and background/foreground compaction.
- **Classes**:
  - `MemTable`: In-memory sorted write buffer with byte size accounting.
  - `LSMTree`: Thread-safe coordinator (`threading.RLock`). Coordinates write path (WAL -> MemTable -> flush to L0 SSTable) and read path (Active MemTable -> Immutable MemTable -> SSTables newest to oldest).
  - `compact()`: Performs $K$-way merge sort across SSTables using `heapq`, purges deleted tombstones (`__ZENITH_TOMBSTONE__`), generates consolidated SSTables, and deletes obsolete WAL segments.

---

## 📁 Subdirectory Summaries (Abstract Tree)
*(No further subdirectories in `zenith/storage`)*
