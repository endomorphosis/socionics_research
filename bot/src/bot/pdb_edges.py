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


def _validate_edge(edge: dict) -> bool:
    """
    Validate that an edge is worth storing.
    
    Returns True if the edge should be stored, False if it should be skipped.
    """
    if not edge or not isinstance(edge, dict):
        return False
    
    # Must have from_pid and to_pid
    try:
        from_pid = int(edge.get("from_pid"))
        to_pid = int(edge.get("to_pid"))
    except (ValueError, TypeError):
        return False
    
    # PIDs must be positive
    if from_pid <= 0 or to_pid <= 0:
        return False
    
    # Self-edges are generally not useful
    if from_pid == to_pid:
        return False
    
    return True


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
        skipped = 0
        for e in edges:
            # Validate edge before processing
            if not _validate_edge(e):
                skipped += 1
                continue
                
            try:
                a = int(e.get("from_pid"))
                b = int(e.get("to_pid"))
            except Exception:
                skipped += 1
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
        if skipped > 0:
            print(f"Skipped {skipped} invalid edges during upsert")
        return added
    
    def cleanup_edges(self, dry_run: bool = False) -> dict:
        """
        Clean up edges by removing duplicates and invalid entries.
        
        Args:
            dry_run: If True, only report what would be cleaned without making changes
            
        Returns:
            Dictionary with cleanup results
        """
        if not self.edges_path.exists():
            return {'error': 'Edges file does not exist'}
            
        df = _load_parquet(self.edges_path, ["from_pid", "to_pid", "relation", "source"])
        original_count = len(df)
        
        # Remove duplicates based on (from_pid, to_pid, relation)
        duplicates = df.duplicated(subset=['from_pid', 'to_pid', 'relation'])
        duplicate_count = duplicates.sum()
        
        # Remove empty rows
        empty_rows = df.isnull().all(axis=1)
        empty_count = empty_rows.sum()
        
        # Remove invalid edges (negative PIDs, self-edges, etc.)
        invalid_mask = pd.Series([False] * len(df))
        for i, row in df.iterrows():
            try:
                from_pid = int(row.get('from_pid', 0))
                to_pid = int(row.get('to_pid', 0))
                if from_pid <= 0 or to_pid <= 0 or from_pid == to_pid:
                    invalid_mask.iloc[i] = True
            except (ValueError, TypeError):
                invalid_mask.iloc[i] = True
        
        invalid_count = invalid_mask.sum()
        
        results = {
            'duplicates': duplicate_count,
            'empty': empty_count,  
            'invalid': invalid_count,
            'original_count': original_count,
            'final_count': original_count - duplicate_count - empty_count - invalid_count
        }
        
        if not dry_run and (duplicate_count > 0 or empty_count > 0 or invalid_count > 0):
            df_cleaned = df[~duplicates & ~empty_rows & ~invalid_mask]
            if len(df_cleaned) < original_count:
                # Create backup
                import shutil
                backup_path = self.edges_path.with_suffix(self.edges_path.suffix + '.cleanup_backup')
                shutil.copy2(self.edges_path, backup_path)
                
                # Save cleaned data
                df_cleaned.to_parquet(self.edges_path, index=False)
        
        return results
