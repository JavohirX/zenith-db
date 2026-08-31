# ZenithDB Root Architecture (`architecture.md`)

> **Directory**: `/` (Project Root)  
> **Purpose**: Repository root containing top-level entrypoints, configuration manifests, build scripts, documentation, and the modular source tree.

---

## 🤖 AI Agent Maintenance Instructions
Whenever ANY file is created, modified, renamed, or deleted in this directory or any subdirectories:
1. Update this `architecture.md` (and the corresponding sub-directory `architecture.md`) immediately.
2. Ensure descriptions of textual files (code, configs, scripts) remain exhaustive and detailed.
3. Keep subdirectory descriptions concise as abstract summaries to maintain a clean recursive tree structure.
4. Run `python tools/bundle.py` if python source files change to keep the single-file distribution `zenith.py` synchronized.
5. Re-run `python tests/run_all.py` and `python zenith.py verify-deps` to guarantee 100% test pass rate and zero-dependency compliance.

---

## 📄 File Inventory & Technical Specifications

### `zenith.py`
- **Role**: Standalone single-file executable distribution of the entire ZenithDB engine (+5 Single File Bonus).
- **Line Count**: 4,400+ lines.
- **Dependencies**: 100% Python Standard Library (zero imports from third parties).
- **Contents**: Self-contained inlined bundle of storage core, query engines, protocol serializers, TCP/HTTP servers, terminal UI, benchmarks, REPL, and self-testing harness.
- **Entry Points**:
  - `python zenith.py server`: Starts TCP RESP server on `:6379` and HTTP REST/Dashboard on `:8080`.
  - `python zenith.py repl`: Launches interactive ANSI shell.
  - `python zenith.py bench`: Runs multi-threaded load generator.
  - `python zenith.py verify-deps`: Runs AST import auditor verifying zero external dependencies.

### `.zero-dep.toml`
- **Role**: Hackathon track declaration and bonus tracking manifest.
- **Format**: TOML.
- **Metadata**:
  - `track = "D"` (Data & Storage)
  - `bonuses = ["Single File", "Reproducible Build", "Package Killer", "STDLIB Log"]`
  - `substitutions`: Lists the 13 distinct package killers.

### `requirements.txt`
- **Role**: Standard Python package dependency manifest.
- **Constraint**: Completely empty (0 bytes / comment only), proving zero third-party runtime dependencies.

### `deps-proof.txt`
- **Role**: Cryptographic and AST audit receipt verifying 0 foreign dependencies, reproducible build hashes, and 100% test passing metrics.

### `STDLIB.md`
- **Role**: Detailed documentation of 13 package-to-stdlib substitutions with deep technical rationale, architectural comparisons, and design trade-offs.

### `README.md`
- **Role**: Comprehensive user and judge documentation, featuring Mermaid architecture diagrams, quickstart guides, Redis client interoperability examples, benchmarks, and crash resilience guarantees.

### `DEMO_SCRIPT.md`
- **Role**: Ready-made 5-minute hackathon video demo script with exact presenter dialogue, timing markers, live terminal commands, and camera actions.

### `LICENSE`
- **Role**: OSI-approved MIT License.

### `Makefile`
- **Role**: Single-command automation for POSIX/make environments (`make test`, `make bench`, `make bundle`, `make verify`, `make server`).

### `run.bat` & `run.sh`
- **Role**: Cross-platform one-command launcher wrappers for Windows (`run.bat`) and Linux/macOS (`run.sh`).

### `.gitignore`
- **Role**: Excludes temporary files, bytecode cache (`__pycache__`, `*.pyc`), and runtime data directories (`data/`, `data_bench/`).

---

## 📁 Subdirectory Summaries (Abstract Tree)

- **[`zenith/`](file:///D:/source/hack1/zenith/architecture.md)**: Primary modular Python package containing storage, engine, protocol, server, and CLI components.
- **[`tests/`](file:///D:/source/hack1/tests/architecture.md)**: Comprehensive automated test suite with custom ANSI test runner covering unit, integration, protocol, and crash recovery tests.
- **[`tools/`](file:///D:/source/hack1/tools/architecture.md)**: Developer tooling, including the deterministic single-file bundler and reproducible build verifier.
