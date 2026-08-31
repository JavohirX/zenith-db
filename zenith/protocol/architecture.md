# Protocol Subsystem Architecture (`architecture.md`)

> **Directory**: `/zenith/protocol`  
> **Purpose**: Redis Serialization Protocol (RESP2/RESP3) parser and serializer, allowing ZenithDB to speak native Redis wire protocol to external clients.

---

## 🤖 AI Agent Maintenance Instructions
Whenever ANY file is created, modified, renamed, or deleted in this directory:
1. Update this `architecture.md` with any new RESP data types or parser state machine modifications.
2. Verify packet fragmentation and pipelining tests in `tests/test_protocol_server.py`.
3. Re-run `python tools/bundle.py` and `python tests/run_all.py`.

---

## 📄 File Inventory & Technical Specifications

### `resp.py` (Redis Serialization Protocol Parser & Serializer)
- **Role**: Streaming parser and binary serializer for RESP2 and RESP3 wire protocols.
- **Protocol Data Types Handled**:
  - **Simple Strings** (`+`): E.g., `+OK\r\n`, `+PONG\r\n`
  - **Errors** (`-`): E.g., `-ERR unknown command\r\n`
  - **Integers** (`:`): E.g., `:1000\r\n`
  - **Bulk Strings** (`$`): E.g., `$6\r\nfoobar\r\n` (and Null bulk string `$-1\r\n`)
  - **Arrays** (`*`): E.g., `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` (and Null array `*-1\r\n`)
  - **Inline Commands**: Fallback plain text commands without prefix (e.g. `PING\r\n`).
- **Classes**:
  - `RESPParser`: Streaming state machine backed by `bytearray`. Handles arbitrary TCP packet fragmentation where frames arrive across multiple chunks or socket reads.
    - `feed(data: bytes)`: Appends incoming socket buffer bytes.
    - `get_next()`: Parses next complete command/object, advancing buffer by consumed bytes, or returns `None` if data is incomplete.
  - `RESPSerializer`: High-speed binary serializer converting Python objects to RESP format.
    - `ok()`, `ping()`, `simple_string(s)`, `error(msg)`, `integer(val)`, `bulk_string(s)`, `array(items)`, `encode(val)`.

---

## 📁 Subdirectory Summaries (Abstract Tree)
*(No further subdirectories in `zenith/protocol`)*
