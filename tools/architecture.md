# Tooling Architecture (`architecture.md`)

> **Directory**: `/tools`  
> **Purpose**: Developer utilities for build automation, single-file compilation, and reproducible build verification.

---

## 🤖 AI Agent Maintenance Instructions
Whenever ANY file is created, modified, renamed, or deleted in this directory:
1. Update this `architecture.md` with tool descriptions and hashing methods.
2. Run `python tools/bundle.py` to ensure build determinism and byte-identical hashes.

---

## 📄 File Inventory & Technical Specifications

### `bundle.py` (Single-File Deterministic Bundler)
- **Role**: Compiles modular `zenith/` components into the standalone zero-dependency executable [`zenith.py`](file:///D:/source/hack1/zenith.py) (+5 Single File Bonus, +5 Reproducible Build Bonus).
- **Functions**:
  - `clean_source(code, prefixes_to_strip)`: Strips internal cross-module imports while preserving code indentation, docstrings, and logic.
  - `bundle()`: Concatenates module files in topological dependency order (`bloom` -> `wal` -> `sstable` -> `lsm` -> `kv` -> `doc` -> `text` -> `vector` -> `txn` -> `resp` -> `tcp` -> `http` -> `terminal` -> `bench` -> `main`).
  - `main()`: Executes two consecutive builds, computes SHA-256 hashes for both, verifies byte-identical equality, and outputs the verification receipt.

---

## 📁 Subdirectory Summaries (Abstract Tree)
*(No further subdirectories in `tools`)*
