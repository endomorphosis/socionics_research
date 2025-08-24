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


def _validate_record(record: dict) -> bool:
    """
    Validate that a record is worth storing (not empty/useless).
    
    Returns True if the record should be stored, False if it should be skipped.
    """
    if not record or not isinstance(record, dict):
        return False
    
    # Skip records that are just provenance/metadata without content
    content_keys = [k for k in record.keys() if not (isinstance(k, str) and k.startswith("_"))]
    if not content_keys:
        return False
    
    # Check if there's any meaningful content beyond just ID fields
    id_like_keys = {'id', 'cid', 'pid', 'uuid', 'uid'}
    meaningful_keys = [k for k in content_keys if k.lower() not in id_like_keys]
    
    if not meaningful_keys:
        return False
    
    # Check if meaningful fields have non-empty values
    for key in meaningful_keys:
        value = record.get(key)
        if value is not None:
            if isinstance(value, str) and value.strip():
                return True
            elif not isinstance(value, str) and value:
                return True
    
    return False


def _validate_vector(cid: str, vector) -> bool:
    """
    Validate that a vector is worth storing.
    
    Returns True if the vector should be stored, False if it should be skipped.
    """
    if not cid or not isinstance(cid, str) or not cid.strip():
        return False
    
    if vector is None:
        return False
    
    # Handle both lists and numpy arrays
    if hasattr(vector, '__len__'):
        if len(vector) == 0:
            return False
    else:
        return False
    
    # Check if vector is all zeros (usually indicates empty/invalid embedding)
    try:
        # Handle both list and numpy array
        if hasattr(vector, 'all'):  # numpy array
            if (abs(vector) < 1e-10).all():
                return False
        else:  # regular list
            if all(abs(v) < 1e-10 for v in vector):
                return False
    except Exception:
        # If we can't validate the values, assume it's valid
        pass
    
    return True


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
            skipped = 0
            for r in records:
                # Validate record before processing
                if not _validate_record(r):
                    skipped += 1
                    continue
                    
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
            if skipped > 0:
                print(f"Skipped {skipped} invalid/empty records during upsert")
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
            skipped = 0
            for cid, vec in items:
                # Validate vector before processing
                if not _validate_vector(str(cid), vec):
                    skipped += 1
                    continue
                    
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
            if skipped > 0:
                print(f"Skipped {skipped} invalid/empty vectors during upsert")
            return new, updated

    def load_joined(self) -> pd.DataFrame:
        raw = _load_parquet(self.raw_path, ["cid", "payload_bytes"])
        vec = _load_parquet(self.vec_path, ["cid", "vector"])
        if raw.empty:
            return pd.DataFrame(columns=["cid", "payload_bytes", "vector"])
        return raw.merge(vec, on="cid", how="left")
    
    def cleanup_storage(self, dry_run: bool = False) -> dict:
        """
        Clean up storage files by removing duplicates and invalid entries.
        
        Args:
            dry_run: If True, only report what would be cleaned without making changes
            
        Returns:
            Dictionary with cleanup results
        """
        results = {
            'raw_file': {'duplicates': 0, 'empty': 0, 'invalid': 0},
            'vector_file': {'duplicates': 0, 'empty': 0, 'invalid': 0}
        }
        
        # Clean raw profiles
        if self.raw_path.exists():
            df = _load_parquet(self.raw_path, ["cid", "payload_bytes"])
            original_count = len(df)
            
            # Remove duplicates based on CID
            duplicates = df.duplicated(subset=['cid'])
            duplicate_count = duplicates.sum()
            
            # Remove empty rows
            empty_rows = df.isnull().all(axis=1)
            empty_count = empty_rows.sum()
            
            if not dry_run and (duplicate_count > 0 or empty_count > 0):
                df_cleaned = df[~duplicates & ~empty_rows]
                if len(df_cleaned) < original_count:
                    # Create backup
                    import shutil
                    backup_path = self.raw_path.with_suffix(self.raw_path.suffix + '.cleanup_backup')
                    shutil.copy2(self.raw_path, backup_path)
                    
                    # Save cleaned data
                    _atomic_write_parquet(df_cleaned, self.raw_path)
            
            results['raw_file'] = {
                'duplicates': duplicate_count,
                'empty': empty_count,
                'invalid': 0,
                'original_count': original_count,
                'final_count': original_count - duplicate_count - empty_count
            }
        
        # Clean vectors
        if self.vec_path.exists():
            df = _load_parquet(self.vec_path, ["cid", "vector"])
            original_count = len(df)
            
            # Remove duplicates based on CID
            duplicates = df.duplicated(subset=['cid'])
            duplicate_count = duplicates.sum()
            
            # Remove empty rows
            empty_rows = df.isnull().all(axis=1)
            empty_count = empty_rows.sum()
            
            # Remove invalid vectors (all zeros)
            invalid_vectors = 0
            if len(df) > 0:
                def is_invalid_vector(vec):
                    if vec is None:
                        return True
                    # Handle both lists and numpy arrays
                    if not hasattr(vec, '__len__'):
                        return True
                    if len(vec) == 0:
                        return True
                    # Check if vector is all zeros
                    try:
                        if hasattr(vec, 'all'):  # numpy array
                            if (abs(vec) < 1e-10).all():
                                return True
                        else:  # regular list
                            if all(abs(v) < 1e-10 for v in vec):
                                return True
                    except Exception:
                        # If we can't validate the values, assume it's valid
                        pass
                    return False
                
                invalid_mask = df['vector'].apply(is_invalid_vector)
                invalid_vectors = invalid_mask.sum()
                
                if not dry_run and (duplicate_count > 0 or empty_count > 0 or invalid_vectors > 0):
                    df_cleaned = df[~duplicates & ~empty_rows & ~invalid_mask]
                    if len(df_cleaned) < original_count:
                        # Create backup
                        import shutil
                        backup_path = self.vec_path.with_suffix(self.vec_path.suffix + '.cleanup_backup')
                        shutil.copy2(self.vec_path, backup_path)
                        
                        # Save cleaned data
                        _atomic_write_parquet(df_cleaned, self.vec_path)
            
            results['vector_file'] = {
                'duplicates': duplicate_count,
                'empty': empty_count,
                'invalid': invalid_vectors,
                'original_count': original_count,
                'final_count': original_count - duplicate_count - empty_count - invalid_vectors
            }
        
        return results
