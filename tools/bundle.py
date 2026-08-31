"""
ZenithDB Single-File Deterministic Bundler & Reproducible Build Verifier
Compiles all modular components into a single standalone zero-dependency Python script: zenith.py
"""

import hashlib
import os
import sys
from typing import List

HEADER = '''#!/usr/bin/env python3
"""
ZenithDB - Zero-Dependency Multi-Model Storage Engine (Single-File Standalone Edition)
Track D: Data & Storage · Zero-Dependency Hackathon 2026
100% Python Standard Library · 0 Third-Party Dependencies

Features:
- LSM-Tree with Write-Ahead Log (WAL) & SSTables
- Inverted Full-Text Search with Okapi BM25 Ranking & Porter Stemmer
- Vector Similarity Index with Cosine/Euclidean Distance & IVF Partitioning
- JSON Document Store with Secondary Indexing & Filtering
- ACID Transactions with Snapshot Isolation
- RESP2 Redis-Compatible TCP Server
- HTTP REST API & Real-time Web Control Plane
- ANSI Terminal UI (Tables, Progress Bars, REPL, Benchmarks)
"""

import argparse
import array
import ast
import asyncio
import bisect
import fnmatch
import glob
import hashlib
import heapq
import json
import logging
import math
import os
import random
import re
import shutil
import string
import struct
import sys
import tempfile
import threading
import time
import unicodedata
import unittest
import zlib
from collections import Counter
from enum import IntEnum
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from typing import Any, BinaryIO, Callable, Dict, Generator, List, Optional, Sequence, Set, Tuple, Union
from urllib.parse import parse_qs, urlparse

# Attempt to configure stdout to UTF-8
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

__version__ = "1.0.0"
__license__ = "MIT"
'''

MODULE_FILES = [
    ("zenith/storage/bloom.py", ["from zenith.storage.bloom import"]),
    ("zenith/storage/wal.py", []),
    ("zenith/storage/sstable.py", ["from zenith.storage.bloom import"]),
    ("zenith/storage/lsm.py", [
        "from zenith.storage.sstable import",
        "from zenith.storage.wal import",
    ]),
    ("zenith/engine/kv.py", ["from zenith.storage.lsm import"]),
    ("zenith/engine/doc.py", ["from zenith.storage.lsm import"]),
    ("zenith/engine/text.py", ["from zenith.storage.lsm import"]),
    ("zenith/engine/vector.py", ["from zenith.storage.lsm import"]),
    ("zenith/engine/txn.py", [
        "from zenith.storage.lsm import",
        "from zenith.storage.wal import",
    ]),
    ("zenith/protocol/resp.py", []),
    ("zenith/server/tcp.py", [
        "from zenith.engine.doc import",
        "from zenith.engine.kv import",
        "from zenith.engine.text import",
        "from zenith.engine.vector import",
        "from zenith.protocol.resp import",
        "from zenith.storage.lsm import",
    ]),
    ("zenith/server/http.py", [
        "from zenith.engine.doc import",
        "from zenith.engine.kv import",
        "from zenith.engine.text import",
        "from zenith.engine.vector import",
        "from zenith.storage.lsm import",
    ]),
    ("zenith/cli/terminal.py", []),
    ("zenith/cli/bench.py", [
        "from zenith.cli.terminal import",
        "from zenith.engine.kv import",
        "from zenith.engine.text import",
        "from zenith.engine.vector import",
        "from zenith.storage.lsm import",
    ]),
    ("zenith/cli/main.py", [
        "from zenith.cli.bench import",
        "from zenith.cli.terminal import",
        "from zenith.engine.doc import",
        "from zenith.engine.kv import",
        "from zenith.engine.text import",
        "from zenith.engine.vector import",
        "from zenith.server.http import",
        "from zenith.server.tcp import",
        "from zenith.storage.lsm import",
    ]),
]


def clean_source(code: str, prefixes_to_strip: List[str]) -> str:
    lines = code.splitlines()
    filtered = []
    for line in lines:
        stripped = line.strip()
        # Skip internal zenith imports
        if any(stripped.startswith(p) for p in prefixes_to_strip):
            continue
        if stripped.startswith("from zenith") or stripped.startswith("import zenith"):
            continue
        # Skip stdlib imports that are in header
        if stripped.startswith("import ") or stripped.startswith("from typing"):
            continue
        filtered.append(line)
    return "\n".join(filtered)


def bundle() -> str:
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    parts = [HEADER]

    for rel_path, strip_prefixes in MODULE_FILES:
        full_path = os.path.join(root_dir, rel_path)
        with open(full_path, "r", encoding="utf-8") as f:
            code = f.read()
        cleaned = clean_source(code, strip_prefixes)
        parts.append(f"\n# {'='*70}\n# MODULE: {rel_path}\n# {'='*70}\n")
        parts.append(cleaned)

    # Append test runner integration to standalone file
    parts.append(
        """
def run_self_tests() -> bool:
    print_banner()
    print(f"{Colors.BOLD}⚡ Running Self-Contained Built-in Test Suite{Colors.RESET}\\n")
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    # Discover all TestCase classes defined in this module
    curr_module = sys.modules[__name__]
    for attr_name in dir(curr_module):
        attr = getattr(curr_module, attr_name)
        if isinstance(attr, type) and issubclass(attr, unittest.TestCase) and attr is not unittest.TestCase:
            suite.addTests(loader.loadTestsFromTestCase(attr))
            
    runner = unittest.TextTestRunner(verbosity=2)
    res = runner.run(suite)
    return res.wasSuccessful()
"""
    )

    return "\n".join(parts) + "\n"


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    target_path = os.path.join(root_dir, "zenith.py")

    content1 = bundle()
    with open(target_path, "w", encoding="utf-8") as f:
        f.write(content1)

    hash1 = hashlib.sha256(content1.encode("utf-8")).hexdigest()

    # Build second time for reproducible build proof
    content2 = bundle()
    hash2 = hashlib.sha256(content2.encode("utf-8")).hexdigest()

    print("[*] ZenithDB Single-File Deterministic Bundler")
    print(f"  Target File: {target_path}")
    print(f"  Total Lines: {len(content1.splitlines()):,}")
    print(f"  Build #1 SHA-256: {hash1}")
    print(f"  Build #2 SHA-256: {hash2}")
    if hash1 == hash2:
        print("  [OK] Reproducible Build: Byte-identical hash verified!")
    else:
        print("  [ERROR] Hashes differ!")
        sys.exit(1)


if __name__ == "__main__":
    main()
