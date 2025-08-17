from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from .pdb_storage import _ensure_dir  # reuse data dir helper


EDGES_PARQUET = "pdb_profile_edges.parquet"


def _load_parquet(path: Path, columns: list[str]) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame(columns=columns)


@dataclass
class PdbEdgesStorage:
    def __post_init__(self) -> None:
        base = _ensure_dir()
        self.edges_path = base / EDGES_PARQUET

    def upsert_edges(self, edges: Iterable[dict]) -> int:
        """
        Upsert edges (from_pid -> to_pid) with optional relation/source.
        Dedupes by (from_pid,to_pid,relation) and only appends new edges.
        Returns number of edges written.
        """
        df = _load_parquet(self.edges_path, ["from_pid", "to_pid", "relation", "source"])
        existing_keys = set()
        if not df.empty:
            for _, row in df.iterrows():
                try:
                    existing_keys.add((int(row.get("from_pid")), int(row.get("to_pid")), str(row.get("relation") or "")))
                except Exception:
                    pass
        rows = []
        added = 0
        for e in edges:
            try:
                a = int(e.get("from_pid"))
                b = int(e.get("to_pid"))
            except Exception:
                continue
            relation = str(e.get("relation") or "")
            source = str(e.get("source") or "")
            key = (a, b, relation)
            if key in existing_keys:
                continue
            existing_keys.add(key)
            rows.append({"from_pid": a, "to_pid": b, "relation": relation, "source": source})
            added += 1
        if rows:
            new_df = pd.DataFrame(rows)
            if df.empty:
                df = new_df
            else:
                df = pd.concat([df, new_df], ignore_index=True, copy=False)
            df.to_parquet(self.edges_path, index=False)
        return added
