#!/usr/bin/env python3
"""
Export compact JSON for client-side KNN from existing PDB data.

Inputs:
  - data/bot_store/pdb_profiles_flat.csv (flattened payload fields incl. name, mbti, socionics, description, big5)
  - data/bot_store/pdb_profile_vectors.parquet (optional; not required here)

Output:
  - compass/public/pdb_profiles.json: list of dicts with minimal fields for UI + text blob for KNN

Notes:
  - We intentionally avoid using Parquet from Node due to prior reader incompatibilities.
  - The browser will compute simple hashing-based vectors over the exported 'text' field.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CSV_IN = ROOT / "data/bot_store/pdb_profiles_flat.csv"
OUT_JSON = ROOT / "compass/public/pdb_profiles.json"


def main() -> None:
    if not CSV_IN.exists():
        raise SystemExit(f"Missing input CSV: {CSV_IN}")

    df = pd.read_csv(CSV_IN)

    # Prefer common name fields; keep minimal columns for UI/search
    cols = [
        "cid",
        "name",
        "profile_name",
        "title",
        "description",
        "mbti",
        "socionics",
        "big5",
    ]
    present = [c for c in cols if c in df.columns]
    sdf = df[present].copy()

    def best_name(row) -> str:
        for c in ("name", "profile_name", "title"):
            v = str(row.get(c, "")).strip()
            if v and v.lower() != "nan":
                return v
        cid = str(row.get("cid", ""))
        return cid if cid else "Unknown"

    def norm(v: object) -> str:
        s = str(v) if v is not None else ""
        s = s.strip()
        return "" if s.lower() == "nan" else s

    records: list[dict] = []
    for _, r in sdf.iterrows():
        name = best_name(r)
        mbti = norm(r.get("mbti"))
        soc = norm(r.get("socionics"))
        desc = norm(r.get("description"))
        big5 = norm(r.get("big5"))
        text = " ".join(x for x in [name, mbti, soc, big5, desc] if x)
        records.append(
            {
                "cid": norm(r.get("cid")),
                "name": name,
                "mbti": mbti,
                "socionics": soc,
                "big5": big5,
                "description": desc,
                "text": text,
            }
        )

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)
    print(f"Wrote {OUT_JSON} with {len(records)} records")


if __name__ == "__main__":
    main()
