from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd


@dataclass
class KLResult:
    question: str
    type_a: str
    type_b: str
    kl_ab: float
    kl_ba: float
    jsd: float


def _load_frame(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    if p.suffix.lower() == ".parquet":
        return pd.read_parquet(p)
    return pd.read_csv(p)


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    # expect columns: subject_id, type_label, question_id, answer_value
    required = {"subject_id", "type_label", "question_id", "answer_value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    return df


def _distributions(df: pd.DataFrame, smoothing: float = 1e-6) -> Dict[Tuple[str, str], np.ndarray]:
    # Build categorical distribution P(answer | type, question) with Laplace smoothing
    dists: Dict[Tuple[str, str], np.ndarray] = {}
    # Determine answer support per question (assumes integer/ordinal answers)
    support: Dict[str, List[int]] = {}
    for q, sub in df.groupby("question_id"):
        vals = sorted(set(int(v) for v in sub["answer_value"].dropna().tolist()))
        support[str(q)] = vals
    for (t, q), sub in df.groupby(["type_label", "question_id"]):
        q_str = str(q)
        vals = support[q_str]
        idx_map = {v: i for i, v in enumerate(vals)}
        counts = np.full(len(vals), smoothing, dtype=float)
        for v in sub["answer_value"].dropna().astype(int).tolist():
            if v in idx_map:
                counts[idx_map[v]] += 1.0
        probs = counts / counts.sum()
        dists[(str(t), q_str)] = probs
    return dists


def kl_divergence(p: np.ndarray, q: np.ndarray, eps: float = 1e-12) -> float:
    p = np.clip(p, eps, 1.0)
    q = np.clip(q, eps, 1.0)
    return float(np.sum(p * np.log(p / q)))


def jensen_shannon(p: np.ndarray, q: np.ndarray, eps: float = 1e-12) -> float:
    m = 0.5 * (p + q)
    return 0.5 * kl_divergence(p, m, eps) + 0.5 * kl_divergence(q, m, eps)


def analyze_kl(path: str | Path, top_k: int = 20, smoothing: float = 1e-6) -> pd.DataFrame:
    df = _ensure_columns(_load_frame(path))
    dmap = _distributions(df, smoothing=smoothing)
    types = sorted({t for (t, _q) in dmap.keys()})
    questions = sorted({q for (_t, q) in dmap.keys()})
    rows: List[Dict[str, object]] = []
    for q in questions:
        # compute all pairwise divergences for this question
        for i in range(len(types)):
            for j in range(i + 1, len(types)):
                ta, tb = types[i], types[j]
                pa = dmap.get((ta, q))
                pb = dmap.get((tb, q))
                if pa is None or pb is None or pa.shape != pb.shape:
                    continue
                kl_ab = kl_divergence(pa, pb)
                kl_ba = kl_divergence(pb, pa)
                jsd = jensen_shannon(pa, pb)
                rows.append({
                    "question": q,
                    "type_a": ta,
                    "type_b": tb,
                    "kl_ab": kl_ab,
                    "kl_ba": kl_ba,
                    "jsd": jsd,
                })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    # Rank by Jensen-Shannon (symmetric) descending as default tie-breaker by max(kl_ab, kl_ba)
    out = out.sort_values(by=["jsd", "kl_ab", "kl_ba"], ascending=[False, False, False]).reset_index(drop=True)
    return out.head(top_k)
