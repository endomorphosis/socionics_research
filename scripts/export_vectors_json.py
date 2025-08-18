#!/usr/bin/env python3
"""
Export vectors JSON from Parquet for client-side KNN.

Input: data/bot_store/pdb_profile_vectors.parquet with columns [cid, vector]
Output: compass/public/pdb_profile_vectors.json as list of {cid, vector}
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
PARQUET_IN = ROOT / "data/bot_store/pdb_profile_vectors.parquet"
OUT_JSON = ROOT / "compass/public/pdb_profile_vectors.json"


def main() -> None:
    if not PARQUET_IN.exists():
        raise SystemExit(f"Missing input Parquet: {PARQUET_IN}")

    df = pd.read_parquet(PARQUET_IN, columns=["cid", "vector"])  # type: ignore[arg-type]
    records: list[dict] = []
    for _, r in df.iterrows():
        cid = str(r.get("cid", ""))
        vec = r.get("vector")
        if not cid or vec is None:
            continue
        # Accept list/tuple/np.ndarray
        if isinstance(vec, np.ndarray):
            arr = vec.astype(float).tolist()
        else:
            try:
                arr = [float(x) for x in vec]
            except Exception:
                continue
        records.append({"cid": cid, "vector": arr})

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)
    print(f"Wrote {OUT_JSON} with {len(records)} vectors")


if __name__ == "__main__":
    main()
