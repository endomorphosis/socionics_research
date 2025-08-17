from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd
import os
import time
import fcntl

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
    if not path.exists():
        return pd.DataFrame(columns=columns)
    try:
        return pd.read_parquet(path)
    except Exception:
        try:
            ts = int(time.time())
            bad = path.with_suffix(path.suffix + f".corrupt.{ts}")
            os.replace(path, bad)
        except Exception:
            pass
        return pd.DataFrame(columns=columns)


def _atomic_write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Use a unique tmp filename in the same directory for atomic replace
    ts = int(time.time() * 1000)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}.{ts}")
    try:
        df.to_parquet(tmp, index=False)
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


class _FileLock:
    def __init__(self, lock_path: Path) -> None:
        self._lock_path = lock_path
        self._fh = None

    def __enter__(self):
        self._fh = open(self._lock_path, "w")
        fcntl.flock(self._fh, fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._fh is not None:
                fcntl.flock(self._fh, fcntl.LOCK_UN)
                self._fh.close()
        finally:
            self._fh = None


@dataclass
class PdbStorage:
    def __post_init__(self) -> None:
        base = _ensure_dir()
        self.raw_path = base / RAW_PARQUET
        self.vec_path = base / VEC_PARQUET

    def upsert_raw(self, records: Iterable[dict]) -> Tuple[int, int]:
        lock = self.raw_path.with_suffix(self.raw_path.suffix + ".lock")
        with _FileLock(lock):
            df = _load_parquet(self.raw_path, ["cid", "payload_bytes"])
            existing = set(df["cid"].astype(str)) if not df.empty else set()
            # Build a small lookup for existing payloads to avoid redundant writes
            existing_payload: dict[str, bytes] = {}
            if not df.empty:
                for _, row in df.iterrows():
                    try:
                        existing_payload[str(row.get("cid"))] = row.get("payload_bytes")
                    except Exception:
                        pass
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
                scid = str(cid)
                if scid in existing:
                    # Only write if payload actually differs
                    prev = existing_payload.get(scid)
                    if isinstance(prev, (bytes, bytearray)) and prev == payload:
                        continue
                    df.loc[df.cid == scid, "payload_bytes"] = [payload]
                    updated += 1
                else:
                    rows.append({"cid": scid, "payload_bytes": payload})
                    new += 1
            if rows:
                new_df = pd.DataFrame(rows)
                if df.empty:
                    df = new_df
                else:
                    df = pd.concat([df, new_df], ignore_index=True, copy=False)
            if new or updated:
                _atomic_write_parquet(df, self.raw_path)
            return new, updated

    def upsert_vectors(self, items: Iterable[tuple[str, list[float]]]) -> Tuple[int, int]:
        lock = self.vec_path.with_suffix(self.vec_path.suffix + ".lock")
        with _FileLock(lock):
            df = _load_parquet(self.vec_path, ["cid", "vector"])
            existing = set(df["cid"].astype(str)) if not df.empty else set()
            existing_vec: dict[str, list] = {}
            if not df.empty:
                for _, row in df.iterrows():
                    try:
                        existing_vec[str(row.get("cid"))] = row.get("vector")
                    except Exception:
                        pass
            rows = []
            new = 0
            updated = 0
            for cid, vec in items:
                scid = str(cid)
                if scid in existing:
                    prev = existing_vec.get(scid)
                    if isinstance(prev, list) and prev == vec:
                        continue
                    df.loc[df.cid == scid, "vector"] = [vec]
                    updated += 1
                else:
                    rows.append({"cid": scid, "vector": vec})
                    new += 1
            if rows:
                new_df = pd.DataFrame(rows)
                if df.empty:
                    df = new_df
                else:
                    df = pd.concat([df, new_df], ignore_index=True, copy=False)
            if new or updated:
                _atomic_write_parquet(df, self.vec_path)
            return new, updated

    def load_joined(self) -> pd.DataFrame:
        raw = _load_parquet(self.raw_path, ["cid", "payload_bytes"])
        vec = _load_parquet(self.vec_path, ["cid", "vector"])
        if raw.empty:
            return pd.DataFrame(columns=["cid", "payload_bytes", "vector"])
        return raw.merge(vec, on="cid", how="left")
