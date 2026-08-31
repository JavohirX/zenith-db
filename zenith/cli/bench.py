"""
ZenithDB Performance Benchmarking Suite
High-concurrency load generator measuring latency percentiles (p50/p95/p99) and ops/sec.
"""

import math
import random
import string
import threading
import time
from typing import Callable, List, Tuple

from zenith.cli.terminal import Colors, ProgressBar, format_table
from zenith.engine.kv import KeyValueEngine
from zenith.engine.text import FullTextIndex
from zenith.engine.vector import VectorIndex
from zenith.storage.lsm import LSMTree


def random_string(length: int = 16) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


class BenchmarkEngine:
    """Runs multi-threaded workload benchmarks against ZenithDB engines."""

    def __init__(self, lsm: LSMTree) -> None:
        self.lsm = lsm
        self.kv = KeyValueEngine(lsm)
        self.text_idx = FullTextIndex(lsm, "bench_text")
        self.vector_idx = VectorIndex(lsm, "bench_vec", dimension=64)

    def run_benchmark(
        self,
        name: str,
        total_ops: int,
        concurrency: int,
        op_factory: Callable[[int], Callable[[], None]],
    ) -> dict:
        """
        Executes concurrent load test and calculates statistical percentiles.
        """
        latencies_ms: List[float] = []
        lat_lock = threading.Lock()

        ops_per_thread = total_ops // concurrency
        threads: List[threading.Thread] = []

        progress = ProgressBar(total_ops, description=f"Benchmarking {name}")
        start_time = time.time()

        def worker(thread_id: int):
            op_fn = op_factory(thread_id)
            local_lats = []
            for _ in range(ops_per_thread):
                t0 = time.perf_counter()
                op_fn()
                t1 = time.perf_counter()
                local_lats.append((t1 - t0) * 1000.0)  # ms
                progress.update(1)

            with lat_lock:
                latencies_ms.extend(local_lats)

        for tid in range(concurrency):
            t = threading.Thread(target=worker, args=(tid,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        total_time = time.time() - start_time
        latencies_ms.sort()

        if not latencies_ms:
            return {}

        def percentile(p: float) -> float:
            k = (len(latencies_ms) - 1) * p
            f = math.floor(k)
            c = math.ceil(k)
            if f == c:
                return latencies_ms[int(k)]
            d0 = latencies_ms[int(f)] * (c - k)
            d1 = latencies_ms[int(c)] * (k - f)
            return d0 + d1

        ops_sec = total_ops / total_time if total_time > 0 else 0

        return {
            "name": name,
            "ops": total_ops,
            "concurrency": concurrency,
            "total_time_s": round(total_time, 3),
            "ops_sec": round(ops_sec, 1),
            "min_ms": round(latencies_ms[0], 3),
            "avg_ms": round(sum(latencies_ms) / len(latencies_ms), 3),
            "p50_ms": round(percentile(0.50), 3),
            "p95_ms": round(percentile(0.95), 3),
            "p99_ms": round(percentile(0.99), 3),
            "max_ms": round(latencies_ms[-1], 3),
        }

    def run_full_suite(
        self, total_ops: int = 10000, concurrency: int = 8
    ) -> None:
        """Runs the standard ZenithDB performance suite."""
        print(f"\n{Colors.BOLD}⚡ ZenithDB High-Performance Benchmark Suite{Colors.RESET}")
        print(
            f"{Colors.DIM}Target operations: {total_ops:,} | Concurrency: {concurrency} threads{Colors.RESET}\n"
        )

        results = []

        # 1. SET Key-Value
        def set_factory(tid: int):
            return lambda: self.kv.set(
                f"bench_k_{tid}_{random.randint(0, 10000)}", random_string(32)
            )

        res_set = self.run_benchmark("SET (KV)", total_ops, concurrency, set_factory)
        results.append(res_set)

        # 2. GET Key-Value
        def get_factory(tid: int):
            return lambda: self.kv.get(
                f"bench_k_{tid}_{random.randint(0, 10000)}"
            )

        res_get = self.run_benchmark("GET (KV)", total_ops, concurrency, get_factory)
        results.append(res_get)

        # 3. Hash HSET
        def hset_factory(tid: int):
            return lambda: self.kv.hset(
                f"bench_hash_{tid}",
                f"field_{random.randint(0, 100)}",
                random_string(16),
            )

        res_hset = self.run_benchmark("HSET (Hash)", total_ops, concurrency, hset_factory)
        results.append(res_hset)

        # 4. BM25 Text Search
        # Prepopulate corpus
        corpus_words = [
            "database", "acid", "transactions", "lsm", "engine", "storage",
            "search", "vector", "python", "standard", "library", "bloom",
        ]
        for i in range(100):
            sample_text = " ".join(random.choices(corpus_words, k=12))
            self.text_idx.index_document(f"doc_{i}", sample_text)

        def search_factory(tid: int):
            return lambda: self.text_idx.search(
                random.choice(corpus_words), limit=5
            )

        search_ops = min(total_ops // 2, 2000)
        res_search = self.run_benchmark(
            "BM25 Search", search_ops, concurrency, search_factory
        )
        results.append(res_search)

        # 5. Vector Cosine Search
        # Prepopulate vectors (64-dim)
        for i in range(100):
            vec = [random.uniform(-1.0, 1.0) for _ in range(64)]
            self.vector_idx.insert(f"vec_{i}", vec)

        def vec_factory(tid: int):
            qvec = [random.uniform(-1.0, 1.0) for _ in range(64)]
            return lambda: self.vector_idx.search(qvec, top_k=5, metric="cosine")

        vec_ops = min(total_ops // 2, 2000)
        res_vec = self.run_benchmark(
            "Vector Search", vec_ops, concurrency, vec_factory
        )
        results.append(res_vec)

        # Print summary table
        headers = [
            "Workload", "Operations", "Throughput", "Avg Latency", "p50", "p95", "p99", "Max Latency"
        ]
        rows = []
        for r in results:
            rows.append(
                [
                    r["name"],
                    f"{r['ops']:,}",
                    f"{r['ops_sec']:,.1f} ops/s",
                    f"{r['avg_ms']:.3f} ms",
                    f"{r['p50_ms']:.3f} ms",
                    f"{r['p95_ms']:.3f} ms",
                    f"{r['p99_ms']:.3f} ms",
                    f"{r['max_ms']:.3f} ms",
                ]
            )

        print("\n" + format_table(headers, rows))
        print(
            f"\n{Colors.GREEN}✓ Benchmark completed successfully. Zero third-party packages used.{Colors.RESET}\n"
        )
