"""
ZenithDB Vector Similarity Engine
Compact binary vector storage, exact and IVF partitioned nearest-neighbor search with Cosine, Euclidean, and Dot metrics.
"""

import array
import json
import math
import random
import struct
from typing import Any, Dict, List, Optional, Tuple, Union

from zenith.storage.lsm import LSMTree


def vector_dot(u: List[float], v: List[float]) -> float:
    """Computes dot product of two vectors."""
    return sum(a * b for a, b in zip(u, v))


def vector_norm(u: List[float]) -> float:
    """Computes Euclidean L2 norm of a vector."""
    return math.sqrt(sum(a * a for a in u))


def cosine_similarity(u: List[float], v: List[float]) -> float:
    """Computes cosine similarity between [-1.0, 1.0]. Higher is more similar."""
    norm_u = vector_norm(u)
    norm_v = vector_norm(v)
    if norm_u == 0.0 or norm_v == 0.0:
        return 0.0
    return vector_dot(u, v) / (norm_u * norm_v)


def euclidean_distance(u: List[float], v: List[float]) -> float:
    """Computes Euclidean distance. Lower is closer."""
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(u, v)))


class VectorIndex:
    """
    High-performance vector index engine built on pure standard library primitives.
    """

    def __init__(
        self,
        lsm: LSMTree,
        namespace: str = "default",
        dimension: Optional[int] = None,
    ) -> None:
        self.lsm = lsm
        self.namespace = namespace
        self.dimension = dimension
        self._centroids: List[List[float]] = []  # IVF centroids
        self._centroid_clusters: Dict[int, List[str]] = {}  # centroid_idx -> [vector_ids]

    def _vec_key(self, vec_id: str) -> bytes:
        return f"__VEC__:{self.namespace}:{vec_id}".encode("utf-8")

    def _meta_key(self, vec_id: str) -> bytes:
        return f"__VMETA__:{self.namespace}:{vec_id}".encode("utf-8")

    def insert(
        self,
        vector_id: str,
        vector: List[float],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Inserts or updates a dense vector embedding."""
        if self.dimension is None:
            self.dimension = len(vector)
        elif len(vector) != self.dimension:
            raise ValueError(
                f"Vector dimension mismatch: expected {self.dimension}, got {len(vector)}"
            )

        # Pack vector floats into compact binary representation
        vec_bytes = struct.pack(f">{len(vector)}f", *vector)
        self.lsm.put(self._vec_key(vector_id), vec_bytes)

        if metadata:
            meta_bytes = json.dumps(metadata).encode("utf-8")
            self.lsm.put(self._meta_key(vector_id), meta_bytes)

    def get(
        self, vector_id: str
    ) -> Optional[Tuple[List[float], Dict[str, Any]]]:
        """Retrieves vector floats and metadata by ID."""
        vec_raw = self.lsm.get(self._vec_key(vector_id))
        if vec_raw is None:
            return None

        dim = len(vec_raw) // 4
        vector = list(struct.unpack(f">{dim}f", vec_raw))

        meta_raw = self.lsm.get(self._meta_key(vector_id))
        metadata = json.loads(meta_raw.decode("utf-8")) if meta_raw else {}

        return (vector, metadata)

    def delete(self, vector_id: str) -> bool:
        """Deletes a vector and its metadata."""
        if self.lsm.get(self._vec_key(vector_id)) is None:
            return False
        self.lsm.delete(self._vec_key(vector_id))
        self.lsm.delete(self._meta_key(vector_id))
        return True

    def train_ivf(self, n_clusters: int = 16, max_iters: int = 10) -> None:
        """Trains K-Means centroids for fast Inverted File (IVF) index acceleration."""
        all_vectors: List[Tuple[str, List[float]]] = []
        prefix = f"__VEC__:{self.namespace}:".encode("utf-8")
        for k, v in self.lsm.scan(start_key=prefix):
            if not k.startswith(prefix):
                break
            vec_id = k[len(prefix) :].decode("utf-8")
            dim = len(v) // 4
            vec = list(struct.unpack(f">{dim}f", v))
            all_vectors.append((vec_id, vec))

        if len(all_vectors) < n_clusters:
            return

        # Random centroid initialization
        sample_centroids = random.sample(
            [v for _, v in all_vectors], n_clusters
        )
        centroids = [list(c) for c in sample_centroids]
        dim = len(centroids[0])

        for _ in range(max_iters):
            clusters: Dict[int, List[List[float]]] = {
                i: [] for i in range(n_clusters)
            }
            for _, v in all_vectors:
                # Find nearest centroid by cosine
                best_c = max(
                    range(n_clusters),
                    key=lambda idx: cosine_similarity(v, centroids[idx]),
                )
                clusters[best_c].append(v)

            # Update centroids
            for idx in range(n_clusters):
                pts = clusters[idx]
                if pts:
                    new_c = [sum(p[d] for p in pts) / len(pts) for d in range(dim)]
                    centroids[idx] = new_c

        self._centroids = centroids
        self._centroid_clusters = {i: [] for i in range(n_clusters)}
        for vec_id, v in all_vectors:
            best_c = max(
                range(n_clusters),
                key=lambda idx: cosine_similarity(v, self._centroids[idx]),
            )
            self._centroid_clusters[best_c].append(vec_id)

    def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
        metric: str = "cosine",  # "cosine", "euclidean", "dot"
        filter_dict: Optional[Dict[str, Any]] = None,
        n_probe: int = 4,
    ) -> List[Dict[str, Any]]:
        """
        Searches for top_k most similar vectors.
        """
        prefix = f"__VEC__:{self.namespace}:".encode("utf-8")
        candidates: List[Tuple[float, str, Dict[str, Any]]] = []

        # Check if IVF acceleration is available
        if self._centroids and len(self._centroids) >= n_probe:
            # Find closest n_probe centroids
            scored_centroids = [
                (cosine_similarity(query_vector, c), idx)
                for idx, c in enumerate(self._centroids)
            ]
            scored_centroids.sort(key=lambda x: x[0], reverse=True)
            target_ids: Set[str] = set()
            for _, c_idx in scored_centroids[:n_probe]:
                target_ids.update(self._centroid_clusters.get(c_idx, []))

            # Scan only target IDs
            for vec_id in target_ids:
                ret = self.get(vec_id)
                if ret is None:
                    continue
                v, meta = ret
                if filter_dict and not self._matches_meta(meta, filter_dict):
                    continue

                score = self._calc_score(query_vector, v, metric)
                candidates.append((score, vec_id, meta))
        else:
            # Exact brute-force scan
            for k, v_bytes in self.lsm.scan(start_key=prefix):
                if not k.startswith(prefix):
                    break
                vec_id = k[len(prefix) :].decode("utf-8")
                dim = len(v_bytes) // 4
                v = list(struct.unpack(f">{dim}f", v_bytes))

                meta_raw = self.lsm.get(self._meta_key(vec_id))
                meta = json.loads(meta_raw.decode("utf-8")) if meta_raw else {}

                if filter_dict and not self._matches_meta(meta, filter_dict):
                    continue

                score = self._calc_score(query_vector, v, metric)
                candidates.append((score, vec_id, meta))

        # Sort candidates
        # For cosine/dot: higher is better (reverse=True)
        # For euclidean: lower is better (reverse=False)
        reverse = metric in ("cosine", "dot")
        candidates.sort(key=lambda x: x[0], reverse=reverse)

        results = []
        for score, vec_id, meta in candidates[:top_k]:
            results.append(
                {
                    "vector_id": vec_id,
                    "score": round(score, 6),
                    "metadata": meta,
                }
            )
        return results

    def _calc_score(
        self, u: List[float], v: List[float], metric: str
    ) -> float:
        if metric == "cosine":
            return cosine_similarity(u, v)
        elif metric == "euclidean":
            return euclidean_distance(u, v)
        elif metric == "dot":
            return vector_dot(u, v)
        else:
            raise ValueError(f"Unknown metric: {metric}")

    def _matches_meta(
        self, meta: Dict[str, Any], filter_dict: Dict[str, Any]
    ) -> bool:
        for k, v in filter_dict.items():
            if meta.get(k) != v:
                return False
        return True
