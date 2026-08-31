# Query Engines Architecture (`architecture.md`)

> **Directory**: `/zenith/engine`  
> **Purpose**: Multi-model query execution layer providing Key-Value data structures, Document storage with JSONPath filtering, Full-Text BM25 ranking, Vector similarity search, and ACID transactions.

---

## 🤖 AI Agent Maintenance Instructions
Whenever ANY file is created, modified, renamed, or deleted in this directory:
1. Update this `architecture.md` with new method signatures, query operators, scoring formulas, or indexing rules.
2. Ensure mathematical operations (distance metrics, BM25 formulas, stemmer rules) remain 100% standard library without numpy/scipy.
3. Update corresponding unit tests in `tests/test_engines.py`.
4. Re-run `python tools/bundle.py` and `python tests/run_all.py`.

---

## 📄 File Inventory & Technical Specifications

### `kv.py` (Multi-Type Key-Value Engine)
- **Role**: Implements Redis-compatible typed data structures on top of the underlying LSM-Tree.
- **Key Prefix Encoding**:
  - String Value: `__V__:{key}`
  - Type Tag: `__TYPE__:{key}` (`string`, `hash`, `list`, `set`, `zset`)
  - Expiration TTL: `__EXP__:{key}` (Unix timestamp)
  - Hash Fields: `__H__:{key}:{field}`
  - List Elements: `__L__:{key}:{index}` and `__LMETA__:{key}` (`head:tail`)
  - Set Members: `__S__:{key}:{member}`
  - Sorted Set Members: `__Z__:{key}:{member}`
- **Operations Supported**:
  - Strings: `set`, `setnx`, `get`, `mget`, `mset`, `incrby`, `decrby`, `append`, `strlen`.
  - Hashes: `hset`, `hget`, `hdel`, `hgetall`, `hkeys`, `hvals`, `hexists`, `hlen`, `hincrby`.
  - Lists: `lpush`, `rpush`, `lpop`, `rpop`, `lrange`, `llen`, `lindex`, `lset`, `ltrim`.
  - Sets: `sadd`, `srem`, `smembers`, `sismember`, `scard`, `sunion`, `sinter`, `sdiff`.
  - Sorted Sets: `zadd`, `zrem`, `zscore`, `zincrby`, `zrank`, `zrange`, `zcard`.
  - Keys & Expiration: `expire`, `ttl`, `persist`, `exists`, `type`, `delete`, `keys`, `dbsize`, `flushdb`.
- **TTL Strategy**: Hybrid active expiration (min-heap `_ttl_heap`) and passive on-access expiration with non-recursive purge.

### `doc.py` (JSON Document Store)
- **Role**: Document collection database with JSONPath extraction and secondary indexes.
- **Classes & Functions**:
  - `get_nested_field(doc, path)`: Extracts nested attributes using dot-notation (e.g. `user.profile.age`).
  - `DocumentStore`: Manages collection namespaces (`__DOC__:{coll}:{id}`).
- **Query & Filtering Operators**:
  - Equality: `$eq`, `$ne`
  - Range: `$gt`, `$gte`, `$lt`, `$lte`
  - Set Membership: `$in`, `$nin`
  - Containment: `$contains` (lists, strings, sets)
  - Pattern Matching: `$regex`
- **Secondary Indexing**: `create_index(coll, field_path)` builds inverted lookup keys `__DIDX__:{coll}:{field}:{val}:{id}` for accelerated lookups.

### `text.py` (Full-Text Search Engine with BM25)
- **Role**: Inverted index full-text search with stemming and probabilistic relevance scoring.
- **Classes**:
  - `PorterStemmer`: Complete zero-dependency implementation of the Porter Stemming algorithm using vowel-consonant measure sequences $m$ in $[C](VC)^m[V]$.
  - `FullTextIndex`: Builds inverted index postings (`__FT_POST__:{ns}:{term}:{doc_id}`).
- **BM25 Relevance Formula**:
  $$\text{Score}(D, Q) = \sum_{q \in Q} \text{IDF}(q) \cdot \frac{f(q, D) \cdot (k_1 + 1)}{f(q, D) + k_1 \cdot \left(1 - b + b \cdot \frac{|D|}{\text{avgdl}}\right)}$$
  where $\text{IDF}(q) = \ln\left(1 + \frac{N - n(q) + 0.5}{n(q) + 0.5}\right)$, $k_1 = 1.5$, $b = 0.75$.
- **Snippet Generator**: Scans document text, identifies densest keyword match window, and injects HTML/ANSI highlight tags (`<b>...</b>`).

### `vector.py` (Dense Vector Similarity Engine)
- **Role**: High-dimensional vector embedding storage and nearest-neighbor search.
- **Storage Format**: Packed IEEE-754 binary floats (`struct.pack(f'>{dim}f', *vec)`).
- **Distance Metrics**:
  - Cosine Similarity: $\frac{u \cdot v}{\|u\|_2 \|v\|_2} \in [-1.0, 1.0]$
  - Euclidean Distance: $\sqrt{\sum (u_i - v_i)^2} \ge 0$
  - Dot Product: $\sum u_i v_i$
- **Index Types**:
  - Exact Brute-Force: Flat scan with pure Python list optimizations.
  - Inverted File (IVF) Clustering: K-Means centroid clustering (`train_ivf()`) partitions vector space into Voronoi cells, querying only the $n\_probe$ closest centroids for $10\times$ speedup on large datasets.

### `txn.py` (ACID Transaction Manager)
- **Role**: Provides atomic multi-key transactions with snapshot isolation and rollback.
- **Classes**:
  - `Transaction`: Buffers mutations in local `_write_buffer` and `_meta_buffer`. Reads own uncommitted writes.
  - `commit()`: Atomically emits `WALOpType.TXN_BEGIN` -> writes all mutations -> emits `WALOpType.TXN_COMMIT` under global mutex lock.
  - `rollback()`: Clears buffers without persisting. Supports Python context manager (`with txn_mgr.begin() as tx:`).

---

## 📁 Subdirectory Summaries (Abstract Tree)
*(No further subdirectories in `zenith/engine`)*
