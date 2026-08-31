# Server Subsystem Architecture (`architecture.md`)

> **Directory**: `/zenith/server`  
> **Purpose**: Network layer exposing ZenithDB via concurrent Asyncio TCP (Redis-compatible) and Multi-Threaded HTTP/1.1 REST API + Real-time Web Control Plane.

---

## 🤖 AI Agent Maintenance Instructions
Whenever ANY file is created, modified, renamed, or deleted in this directory:
1. Update this `architecture.md` with new REST routes, RESP command dispatches, or dashboard features.
2. Verify that NO third-party frameworks (such as FastAPI, Flask, Tornado, or uvloop) are imported; use only stdlib `asyncio` and `http.server`.
3. Update network integration tests in `tests/test_protocol_server.py`.
4. Re-run `python tools/bundle.py` and `python tests/run_all.py`.

---

## 📄 File Inventory & Technical Specifications

### `tcp.py` (Asyncio Redis TCP Server)
- **Role**: Non-blocking TCP socket server handling Redis client connections concurrently.
- **Port**: Default `:6379`.
- **Classes**:
  - `TCPServer`: Uses `asyncio.start_server` to accept client streams.
  - `handle_client(reader, writer)`: Feeds incoming byte chunks into `RESPParser`, executes commands via `execute_command()`, and sends back RESP responses.
- **Command Router**:
  - Server Management: `PING`, `ECHO`, `COMMAND`, `INFO`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `COMPACT`.
  - Strings: `GET`, `SET` (with `EX`, `NX`, `XX`), `SETNX`, `SETEX`, `MGET`, `MSET`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `APPEND`, `STRLEN`.
  - Keys: `DEL`, `EXISTS`, `EXPIRE`, `TTL`, `PERSIST`, `TYPE`, `KEYS`.
  - Hashes: `HSET`, `HGET`, `HDEL`, `HGETALL`, `HKEYS`, `HVALS`, `HEXISTS`, `HLEN`, `HINCRBY`.
  - Lists: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LTRIM`.
  - Sets: `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SUNION`, `SINTER`, `SDIFF`.
  - Sorted Sets: `ZADD`, `ZREM`, `ZSCORE`, `ZINCRBY`, `ZRANK`, `ZRANGE`, `ZCARD`.
  - Zenith Extensions: `SEARCH.BM25`, `VECTOR.SEARCH`, `DOC.INSERT`, `DOC.GET`, `DOC.QUERY`, `DOC.DELETE`.

### `http.py` (REST API & Live Web Control Plane)
- **Role**: Multi-threaded HTTP/1.1 server serving JSON REST endpoints and an embedded interactive HTML5/CSS/SVG dashboard.
- **Port**: Default `:8080`.
- **Classes**:
  - `ThreadedHTTPServer`: Subclasses `socketserver.ThreadingMixIn` and `http.server.HTTPServer` for concurrent request handling.
  - `ZenithHTTPHandler`: Request dispatcher handling CORS, routing, and JSON serialization.
  - `HTTPServerWrapper`: Lifecycle manager for starting and shutting down the server.
- **REST Endpoints**:
  - `GET /` & `GET /dashboard`: Self-contained HTML5 web control plane (real-time metrics, live key browser, and query console with 0 CDN dependencies).
  - `GET /health`: JSON health status and engine version.
  - `GET /metrics`: Real-time key counts, operations processed, SSTable count, and uptime.
  - `GET /api/v1/keys`: List stored keys matching pattern.
  - `GET /api/v1/kv/:key`: Point lookup with value, type, and remaining TTL.
  - `POST /api/v1/kv`: Set key with optional `ex` expiration.
  - `DELETE /api/v1/kv/:key`: Delete key.
  - `POST /api/v1/search`: BM25 full-text search.
  - `POST /api/v1/vector/search`: Vector similarity search.
  - `POST /api/v1/vector/insert`: Insert dense vector embedding with metadata.
  - `POST /api/v1/doc/:collection`: Insert JSON document.
  - `GET /api/v1/doc/:collection/:id`: Retrieve JSON document.
  - `POST /api/v1/doc/:collection/query`: Query JSON documents with filter operators.
  - `DELETE /api/v1/doc/:collection/:id`: Delete JSON document.
  - `POST /api/v1/compact`: Trigger on-demand LSM compaction.

---

## 📁 Subdirectory Summaries (Abstract Tree)
*(No further subdirectories in `zenith/server`)*
