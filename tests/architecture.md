# Test Suite Architecture (`architecture.md`)

> **Directory**: `/tests`  
> **Purpose**: Automated test suite and custom ANSI test runner providing 100% standard library testing without pytest.

---

## 🤖 AI Agent Maintenance Instructions
Whenever ANY test file is created, modified, renamed, or deleted in this directory:
1. Update this `architecture.md` with new test case names, coverage areas, and assertion invariants.
2. Run `python tests/run_all.py` to ensure all tests pass with 0 failures and 0 errors.
3. Update `deps-proof.txt` and `README.md` if the total test count changes.

---

## 📄 File Inventory & Technical Specifications

### `run_all.py` (Unified ANSI Test Runner)
- **Role**: Discovers and executes all unit tests using standard library `unittest`, rendering a real-time ANSI status table with microsecond execution durations.
- **Classes**:
  - `ZenithTestResult`: Custom `unittest.TextTestResult` tracking per-test execution timestamps (`time.perf_counter`) and status.
  - `run_tests()`: Discovers `test_*.py` files, executes test suite, outputs formatted results table, and returns exit code 0 on success or 1 on failure.

### `test_storage.py` (Storage Layer Tests)
- **Role**: Validates low-level durability and storage mechanisms.
- **Test Cases**:
  - `TestBloomFilter.test_membership_and_false_positives`: Verifies true positives and tests false-positive rate threshold ($<5\%$).
  - `TestBloomFilter.test_serialization_roundtrip`: Verifies binary `to_bytes()` and `from_bytes()`.
  - `TestWriteAheadLog.test_wal_append_and_recovery`: Verifies binary log appending, LSN ordering, and recovery replay.
  - `TestWriteAheadLog.test_wal_crash_truncation_resilience`: Simulates sudden power failure mid-write with corrupted trailing bytes; verifies clean isolation of valid committed frames.
  - `TestSSTable.test_sstable_write_and_binary_search`: Tests immutable SSTable writing, sparse index binary search, and range scanning.
  - `TestLSMTree.test_lsm_flush_and_compaction`: Verifies MemTable flush thresholds, $K$-way merge sort compaction, and tombstone collection.

### `test_engines.py` (Multi-Model Engine Tests)
- **Role**: Validates KV, Document, Text, Vector, and Transaction logic.
- **Test Cases**:
  - `TestKeyValueEngine.test_strings_and_increments`: Tests `set`, `get`, `incrby`, `decrby`, `strlen`, `append`, `setnx`.
  - `TestKeyValueEngine.test_hashes`: Tests `hset`, `hget`, `hdel`, `hgetall`, `hincrby`, `hexists`.
  - `TestKeyValueEngine.test_lists`: Tests `rpush`, `lrange`, `lindex`, `lset`, `ltrim`, `lpop`, `rpop`.
  - `TestKeyValueEngine.test_sets`: Tests `sadd`, `srem`, `smembers`, `sismember`, `sunion`, `sinter`, `sdiff`.
  - `TestKeyValueEngine.test_sorted_sets`: Tests `zadd`, `zscore`, `zincrby`, `zrank`, `zrange`.
  - `TestKeyValueEngine.test_ttl_expiration`: Verifies TTL expiration and automatic lazy purging after timeout.
  - `TestDocumentStore.test_document_crud_and_queries`: Tests document insertion, retrieval, JSONPath filtering (`$gte`, `$contains`), and deletion.
  - `TestFullTextIndex.test_porter_stemmer`: Tests English inflectional rule transformations.
  - `TestFullTextIndex.test_bm25_search_relevance`: Tests Okapi BM25 relevance scoring and snippet highlight generation.
  - `TestVectorIndex.test_vector_math_and_search`: Tests cosine similarity, Euclidean distance, and nearest-neighbor search.
  - `TestTransactions.test_transaction_commit_and_rollback`: Tests atomic commit and rollback on exception.

### `test_protocol_server.py` (Protocol & Server Tests)
- **Role**: Validates RESP parsing, serialization, and TCP/HTTP server request dispatching.
- **Test Cases**:
  - `TestRESPProtocol.test_serializer`: Verifies simple strings, errors, integers, bulk strings, and arrays.
  - `TestRESPProtocol.test_parser_single_packet`: Verifies parsing of complete RESP packets.
  - `TestRESPProtocol.test_parser_fragmented_packet`: Simulates TCP packet chunking and fragmented streaming reads.
  - `TestServerCommandExecution.test_ping_and_echo`: Tests server handshake commands.
  - `TestServerCommandExecution.test_set_get_del_flow`: Tests standard KV request-response flow.
  - `TestServerCommandExecution.test_extended_redis_commands`: Tests `SETNX`, `APPEND`, `STRLEN`, `HINCRBY`, `LINDEX`, `LTRIM`.
  - `TestServerCommandExecution.test_document_and_search_commands`: Tests `DOC.INSERT`, `DOC.GET`, `DOC.QUERY`, `DOC.DELETE`.

---

## 📁 Subdirectory Summaries (Abstract Tree)
*(No further subdirectories in `tests`)*
