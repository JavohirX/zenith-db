# CLI & Terminal UI Architecture (`architecture.md`)

> **Directory**: `/zenith/cli`  
> **Purpose**: Terminal UI rendering engine (Rich/Colorama/Tabulate/Tqdm replacement), interactive REPL shell, performance benchmarking suite, and unified command-line entrypoint.

---

## 🤖 AI Agent Maintenance Instructions
Whenever ANY file is created, modified, renamed, or deleted in this directory:
1. Update this `architecture.md` with new subcommands, flags, terminal styling capabilities, or benchmark metrics.
2. Ensure terminal UI code remains resilient to character encoding limitations (Windows cp1252 / UTF-8 safe).
3. Re-run `python tools/bundle.py` and `python tests/run_all.py`.

---

## 📄 File Inventory & Technical Specifications

### `terminal.py` (Zero-Dependency Terminal UI Engine)
- **Role**: Replaces `rich`, `colorama`, `tabulate`, and `tqdm` using pure ANSI escape sequences and standard library utilities.
- **Components**:
  - `Colors`: ANSI color codes (Bold, Dim, Italic, Underline, 16 standard and bright colors, Reset).
  - `format_table(headers, rows, style)`: Computes column widths dynamically with ANSI code stripping for accurate length calculation, and renders formatted borders.
  - `ProgressBar`: In-place carriage return (`\r`) progress bar rendering progress percentage, filled/empty blocks, elapsed time, and real-time operations/second throughput without flicker.
  - `print_banner()`: Cross-platform ASCII banner display.
  - Cross-platform safe UTF-8 stdout reconfiguration (`sys.stdout.reconfigure(encoding='utf-8')`).

### `bench.py` (Performance Benchmarking Engine)
- **Role**: Multi-threaded workload generator measuring latency percentiles and throughput.
- **Classes**:
  - `BenchmarkEngine`: Orchestrates concurrent worker threads against LSM-Tree, KeyValue, BM25 text, and Vector search engines.
  - Computes exact metrics: Total ops, duration, throughput (ops/sec), min latency, average latency, p50, p95, p99, and max latency.
  - `run_full_suite(total_ops, concurrency)`: Executes standard benchmark matrix (SET, GET, HSET, BM25 Search, Vector Search) and outputs a formatted results table.

### `main.py` (Unified CLI Entrypoint)
- **Role**: Command-line dispatcher powered by standard library `argparse`.
- **Subcommands**:
  - `server`: Starts both TCP RESP (`:6379`) and HTTP REST/Dashboard (`:8080`) servers.
  - `repl`: Launches interactive ANSI shell with auto-formatting.
  - `bench`: Runs the multi-threaded performance benchmark.
  - `verify-deps`: Traverses project files using AST analysis and verifies 100% standard library compliance with 0 third-party packages.

---

## 📁 Subdirectory Summaries (Abstract Tree)
*(No further subdirectories in `zenith/cli`)*
