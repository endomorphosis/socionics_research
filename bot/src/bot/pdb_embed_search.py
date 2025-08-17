from __future__ import annotations

import hashlib
import os
from typing import Iterable, List, Tuple

import numpy as np


def _get_embedder():
    light = os.getenv("SOCIONICS_LIGHTWEIGHT_EMBEDDINGS", "1").lower() in ("1", "true", "yes")
    if light:
        class _Lite:
            def encode(self, text: str):
                buckets = [0.0] * 64
                for tok in (text or "").split():
                    h = int(hashlib.sha256(tok.lower().encode()).hexdigest(), 16)
                    buckets[h % 64] += 1
                norm = float(np.linalg.norm(buckets)) or 1.0
                return np.array([v / norm for v in buckets], dtype=float)
        return _Lite()
    else:
        from sentence_transformers import SentenceTransformer

        model_name = os.getenv("SOCIONICS_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
        return SentenceTransformer(model_name)


def embed_texts(texts: Iterable[str]) -> List[List[float]]:
    model = _get_embedder()
    vecs: List[List[float]] = []
    for t in texts:
        v = model.encode(t)
        if hasattr(v, "tolist"):
            v = v.tolist()
        vecs.append(v)
    return vecs


def cosine_topk(matrix: np.ndarray, query: np.ndarray, k: int) -> Tuple[np.ndarray, np.ndarray]:
    denom = (np.linalg.norm(matrix, axis=1) * (np.linalg.norm(query) + 1e-9)) + 1e-9
    scores = (matrix @ query) / denom
    idx = np.argsort(-scores)[:k]
    return idx, scores[idx]
