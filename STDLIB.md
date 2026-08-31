# ZenithDB — Standard Library Substitutions (`STDLIB.md`)

ZenithDB was constructed under the strict **Zero-Dependency constraint**: 100% of runtime functionality is implemented using only the Python standard library. No packages were installed via `pip`, and no third-party libraries are vendored into the source tree.

---

## Zero-Dependency Craft Manifest

| # | Package Normally Reached For | Python Standard Library Substitution | Architectural & Design Rationale |
|---|---|---|---|
| **1** | **`redis` / `redis-py`** | `asyncio.start_server`, `socket`, `struct` | Full RESP2/RESP3 streaming parser and serializer capable of handling fragmented TCP packets and pipelining, supporting Redis clients (`redis-cli`, `redis-py`, `ioredis`). |
| **2** | **`rocksdb` / `leveldb`** | `os`, `struct`, `zlib.crc32`, `hashlib.sha256`, `bisect`, `heapq` | Complete Log-Structured Merge Tree (LSM-Tree) engine with append-only Write-Ahead Log (WAL), binary SSTables, Kirsch-Mitzenmacher Bloom filters, and multi-way merge compaction. |
| **3** | **`sqlite3` / `duckdb` (Doc Engine)** | `json`, `collections.defaultdict`, `re`, `fnmatch` | Schema-free JSON document collection engine with nested JSONPath queries (`$.user.age`), MongoDB-style filtering operators (`$gt`, `$in`, `$contains`), and secondary indexes. |
| **4** | **`whoosh` / `elasticsearch`** | `re`, `math.log`, `collections.Counter`, `unicodedata` | Inverted index full-text search with Robertson-Spärck Jones Okapi BM25 probabilistic ranking ($k_1=1.5, b=0.75$), Porter stemming algorithm, and text snippet highlighting. |
| **5** | **`chromadb` / `faiss`** | `math.sqrt`, `struct.pack('>f')`, `heapq.nlargest`, `random` | Dense vector embedding store with binary IEEE 754 float packing, Exact & Inverted File (IVF) K-Means clustering, Cosine similarity, Euclidean distance, and Dot product metrics. |
| **6** | **`rich` / `colorama`** | ANSI escape sequences (`\033[...]`), `sys.stdout` | 24-bit TrueColor and 16-color terminal styling, auto-wrapping borders, bold, underline, and cross-platform UTF-8 terminal encoding configuration. |
| **7** | **`tabulate`** | `sys.stdout`, string formatting, `str.ljust` | Dynamic table formatter with automatic column width calculation, ANSI length awareness, right/left alignment, and ASCII/Unicode border rendering. |
| **8** | **`tqdm`** | `time.perf_counter`, `sys.stdout.write('\r...')`, `sys.stdout.flush` | Dynamic in-place terminal progress bar with real-time operations/second calculation, percentage rendering, glyph filling, and ETA prediction. |
| **9** | **`click` / `typer`** | `argparse` | Robust command-line interface with nested subcommands (`server`, `repl`, `bench`, `verify-deps`), flag parsing, typed type-casting, and auto-generated help documentation. |
| **10** | **`fastapi` / `flask`** | `http.server.HTTPServer`, `socketserver.ThreadingMixIn`, `urllib.parse` | Multi-threaded HTTP/1.1 REST server with JSON request/response handling, CORS headers, URL routing, query parameter parsing, and live dashboard rendering. |
| **11** | **`pydantic`** | `dataclasses`, `typing`, `json.loads`, `json.dumps` | Lightweight schema validation, serialization, type checking, and boundary parsing for API payloads and document records. |
| **12** | **`python-dotenv`** | `os.environ`, `os.path`, string parsing | Zero-dependency environment and configuration loader with fallback defaults and CLI flag overrides. |
| **13** | **`pytest`** | `unittest`, `unittest.TextTestResult`, `time.perf_counter` | Custom test runner with test discovery, microsecond duration tracking, status table rendering, and failure diff extraction. |

---

## Detailed Substitution Deep-Dives

### 1. Reimplementing RocksDB / LevelDB Storage Engine (`zenith.storage`)
* **Traditional Dependency**: `rocksdb-python`, `plyvel`, or C++ bindings.
* **ZenithDB Implementation**:
  - **Write-Ahead Log (WAL)**: Formatted as a binary frame:
    $$\text{Frame} = [\text{Magic: 4B}][\text{Ver: 1B}][\text{LSN: 8B}][\text{Timestamp: 8B}][\text{Op: 1B}][\text{KeyLen: 4B}][\text{ValLen: 4B}][\text{CRC32: 4B}][\text{Key}][\text{Value}]$$
    Durability is enforced via `os.fsync` with selectable policies (`always`, `every_sec`, `none`). Partial trailing corruptions from power-cuts are isolated and safely discarded during replay.
  - **Bloom Filters**: Uses Kirsch-Mitzenmacher double-hashing $g_i(x) = (h_1 + i \cdot h_2) \pmod m$ derived from `hashlib.sha256`, reducing disk reads for non-existent keys to $<1\%$.
  - **SSTable & Sparse Index**: Data entries are sorted lexicographically. An index block stores byte offsets every 16 keys, enabling $O(\log N)$ binary search in memory via `bisect` before seeking to the target data block.
  - **Compaction**: $K$-way merge sort via `heapq.heappop` consolidates overlapping SSTables and prunes deleted tombstones.

### 2. Reimplementing Redis Wire Protocol (`zenith.protocol.resp`)
* **Traditional Dependency**: `redis`, `hiredis`, `redis-py`.
* **ZenithDB Implementation**:
  - A zero-dependency streaming state machine in `zenith/protocol/resp.py` parses RESP2/RESP3 framing (`+`, `-`, `:`, `$`, `*`) across arbitrary TCP chunk boundaries.
  - Seamlessly handles connection handshakes from standard tools: `redis-cli -p 6379 ping`, `redis-benchmark`, and language drivers (`redis-py`, `ioredis`).

### 3. Reimplementing Whoosh / Elasticsearch BM25 Search (`zenith.engine.text`)
* **Traditional Dependency**: `whoosh`, `elasticsearch`, `tantivy`.
* **ZenithDB Implementation**:
  - **Porter Stemming**: Full English inflection stemmer implemented with phonological vowel-consonant measure sequences $m$ in $[C](VC)^m[V]$.
  - **BM25 Probabilistic Ranking**: Computes Robertson-Spärck Jones IDF and non-linear term frequency saturation ($k_1=1.5, b=0.75$).
  - **Snippet Generator**: Generates context windows around matching tokens with dynamic HTML/ANSI highlight tagging.

### 4. Reimplementing ChromaDB / FAISS Vector Search (`zenith.engine.vector`)
* **Traditional Dependency**: `chromadb`, `faiss-cpu`, `numpy`, `scipy`.
* **ZenithDB Implementation**:
  - Packed IEEE-754 single-precision float binary vectors (`struct.pack('>Nf')`).
  - Cosine similarity, Euclidean $L_2$ distance, and Dot product implemented in pure standard library.
  - K-Means Inverted File (IVF) centroid clustering partitions vectors into Voronoi cells, reducing top-$k$ search space from $O(N)$ to $O(\sqrt{N})$.

---

## Disclosed Development & Test Dependencies

**None.**  
The test framework used is Python's standard `unittest` module. No dev-only third-party testing packages (such as `pytest`, `tox`, or `coverage`) are required to build, test, benchmark, or run the project.

---

## Non-Hackathon Code Disclosure

**None.**  
All code in this repository was written from scratch during the Zero Dependency Hackathon build window.
