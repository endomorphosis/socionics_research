from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd

from .pdb_cid import cid_from_object, canonical_json_bytes


RAW_PARQUET = "pdb_profiles.parquet"
VEC_PARQUET = "pdb_profile_vectors.parquet"


def _ensure_dir() -> Path:
    try:
        from .config import settings as _settings  # type: ignore
        data_dir = _settings.data_dir
    except Exception:
        data_dir = "data/bot_store"
    p = Path(data_dir)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _load_parquet(path: Path, columns: List[str]) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame(columns=columns)


@dataclass
class PdbStorage:
    def __post_init__(self) -> None:
        base = _ensure_dir()
        self.raw_path = base / RAW_PARQUET
        self.vec_path = base / VEC_PARQUET

    def upsert_raw(self, records: Iterable[dict]) -> Tuple[int, int]:
        df = _load_parquet(self.raw_path, ["cid", "payload_bytes"])
        existing = set(df["cid"].astype(str)) if not df.empty else set()
        rows = []
        new = 0
        updated = 0
        for r in records:
            # Compute CID from content without ephemeral provenance keys (those starting with '_')
            base_obj = r
            if isinstance(r, dict):
                try:
                    base_obj = {k: v for k, v in r.items() if not (isinstance(k, str) and k.startswith("_"))}
                except Exception:
                    base_obj = r
            cid = cid_from_object(base_obj)
            # Store full annotated payload for analysis/debugging
            payload = canonical_json_bytes(r)
            if cid in existing:
                # replace existing row by cid
                df.loc[df.cid == cid, "payload_bytes"] = [payload]
                updated += 1
            else:
                rows.append({"cid": cid, "payload_bytes": payload})
                new += 1
        if rows:
            new_df = pd.DataFrame(rows)
            if df.empty:
                df = new_df
            else:
                df = pd.concat([df, new_df], ignore_index=True, copy=False)
        if new or updated:
            df.to_parquet(self.raw_path, index=False)
        return new, updated

    def upsert_vectors(self, items: Iterable[tuple[str, list[float]]]) -> Tuple[int, int]:
        df = _load_parquet(self.vec_path, ["cid", "vector"])
        existing = set(df["cid"].astype(str)) if not df.empty else set()
        rows = []
        new = 0
        updated = 0
        for cid, vec in items:
            if cid in existing:
                df.loc[df.cid == cid, "vector"] = [vec]
                updated += 1
            else:
                rows.append({"cid": cid, "vector": vec})
                new += 1
        if rows:
            new_df = pd.DataFrame(rows)
            if df.empty:
                df = new_df
            else:
                df = pd.concat([df, new_df], ignore_index=True, copy=False)
        if new or updated:
            df.to_parquet(self.vec_path, index=False)
        return new, updated

    def load_joined(self) -> pd.DataFrame:
        raw = _load_parquet(self.raw_path, ["cid", "payload_bytes"])
        vec = _load_parquet(self.vec_path, ["cid", "vector"])
        if raw.empty:
            return pd.DataFrame(columns=["cid", "payload_bytes", "vector"])
        return raw.merge(vec, on="cid", how="left")
