from __future__ import annotations

import asyncio
import json
from typing import Optional

import numpy as np
import orjson

from .pdb_client import PdbClient
from .pdb_storage import PdbStorage
from .pdb_embed_search import embed_texts, cosine_topk
from .pdb_analysis import analyze_kl
from .pdb_normalize import normalize_profile


def _text_from_profile(p: dict) -> str:
    parts = []
    for key in ("name", "title", "username", "display_name"):
        v = p.get(key)
        if isinstance(v, str):
            parts.append(v)
    for key in ("description", "bio", "biography", "about"):
        v = p.get(key)
        if isinstance(v, str):
            parts.append(v)
    if not parts:
        try:
            parts.append(json.dumps(p, ensure_ascii=False)[:2000])
        except Exception:
            parts.append(str(p))
    return "\n".join(parts)


async def cmd_dump(
    cid: int,
    pid: int,
    max_records: Optional[int] = None,
    start_offset: int = 0,
    client: Optional[PdbClient] = None,
) -> None:
    client = client or PdbClient()
    store = PdbStorage()
    batch: list[dict] = []
    count = 0
    async for item in client.iter_profiles(cid=cid, pid=pid, start_offset=start_offset):
        batch.append(item)
        if len(batch) >= 100:
            store.upsert_raw(batch)
            batch.clear()
        count += 1
        if max_records and count >= max_records:
            break
    if batch:
        store.upsert_raw(batch)
    print(f"Dumped {count} profiles (cid={cid}, pid={pid}).")


def cmd_embed() -> None:
    store = PdbStorage()
    df = store.load_joined()
    if df.empty:
        print("No data to embed.")
        return
    to_embed: list[str] = []
    cids: list[str] = []
    for _, row in df.iterrows():
        if row.get("vector") is not None and isinstance(row["vector"], list):
            continue
        payload = row["payload_bytes"]
        try:
            obj = orjson.loads(payload) if isinstance(payload, (bytes, bytearray)) else payload
            if not isinstance(obj, dict):
                # last resort, try stdlib if payload is a string
                obj = json.loads(payload) if isinstance(payload, str) else {}
        except Exception:
            continue
        cids.append(row["cid"])
        to_embed.append(_text_from_profile(obj))
    if not to_embed:
        print("All vectors present.")
        return
    vecs = embed_texts(to_embed)
    store.upsert_vectors(zip(cids, vecs))
    print(f"Embedded {len(vecs)} profiles.")


def cmd_search(query: str, top_k: int = 5) -> None:
    store = PdbStorage()
    df = store.load_joined()
    if df.empty:
        print("No data.")
        return
    rows = df.dropna(subset=["vector"]).reset_index(drop=True)
    if rows.empty:
        print("No vectors found. Run embed first.")
        return
    mat = np.vstack(rows["vector"].to_list())
    qv = embed_texts([query])[0]
    q = np.array(qv)
    idx, scores = cosine_topk(mat, q, top_k)
    for rank, (i, s) in enumerate(zip(idx, scores), start=1):
        r = rows.iloc[int(i)]
        pb = r.get("payload_bytes")
        obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else pb
        name = obj.get("name") or obj.get("title") or obj.get("username") or "(unknown)"
        print(f"{rank}. cid={r['cid'][:12]} score={s:.4f} name={name}")


def main():
    import argparse
    try:
        # Optional: load variables from a local .env if present
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass

    parser = argparse.ArgumentParser(description="Personality DB dumper & search")
    # Global tuning flags (override env if provided)
    parser.add_argument("--rpm", type=int, default=None, help="Max requests per minute (overrides PDB_RPM/env)")
    parser.add_argument("--concurrency", type=int, default=None, help="Parallel request concurrency (overrides PDB_CONCURRENCY/env)")
    parser.add_argument("--timeout", type=float, default=None, help="HTTP timeout seconds (overrides PDB_TIMEOUT_S/env)")
    parser.add_argument("--base-url", type=str, default=None, help="API base URL (overrides PDB_API_BASE_URL)")
    parser.add_argument("--headers", type=str, default=None, help='Extra headers as JSON (merged last, overrides PDB_API_HEADERS fields)')
    sub = parser.add_subparsers(dest="cmd", required=True)

    def _make_client(args) -> PdbClient:
        kwargs = {}
        if getattr(args, "rpm", None) is not None:
            kwargs["rate_per_minute"] = args.rpm
        if getattr(args, "concurrency", None) is not None:
            kwargs["concurrency"] = args.concurrency
        if getattr(args, "timeout", None) is not None:
            kwargs["timeout_s"] = args.timeout
        if getattr(args, "base_url", None):
            kwargs["base_url"] = args.base_url
        if getattr(args, "headers", None):
            try:
                import json as _json
                kwargs["headers"] = _json.loads(args.headers)
            except Exception:
                # fallback to None on parse error
                kwargs["headers"] = None
        return PdbClient(**kwargs)

    p_dump = sub.add_parser("dump", help="Dump profiles to parquet")
    p_dump.add_argument("--cid", type=int, required=True, help="category id (cid)")
    p_dump.add_argument("--pid", type=int, required=True, help="property id (pid)")
    p_dump.add_argument("--max", type=int, default=None, help="optional max records")
    p_dump.add_argument("--start-offset", type=int, default=0, help="start offset for pagination")

    sub.add_parser("embed", help="Generate embeddings parquet")

    p_dump_any = sub.add_parser("dump-any", help="Dump profiles without cid/pid filters")
    p_dump_any.add_argument("--max", type=int, default=None, help="optional max records")
    p_dump_any.add_argument("--start-offset", type=int, default=0, help="start offset for pagination")

    p_search = sub.add_parser("search", help="Search vectors and show top matches")
    p_search.add_argument("query", type=str)
    p_search.add_argument("--top", type=int, default=5)

    p_idx = sub.add_parser("index", help="Build a FAISS index for fast search")
    p_idx.add_argument("--out", type=str, default="data/bot_store/pdb_faiss.index")

    p_sfaiss = sub.add_parser("search-faiss", help="Search using FAISS index")
    p_sfaiss.add_argument("query", type=str)
    p_sfaiss.add_argument("--top", type=int, default=5)
    p_sfaiss.add_argument("--index", type=str, default="data/bot_store/pdb_faiss.index")

    p_sum = sub.add_parser("summarize", help="Summarize current dataset sizes and type distributions")
    p_sum.add_argument("--normalized", type=str, default="data/bot_store/pdb_profiles_normalized.parquet")

    sub.add_parser("cache-clear", help="Clear local API GET cache if enabled")

    p_an = sub.add_parser("analyze", help="Rank questions by inter-type divergence")
    p_an.add_argument("--file", type=str, required=True, help="CSV or Parquet with columns: subject_id,type_label,question_id,answer_value")
    p_an.add_argument("--top", type=int, default=20)
    p_an.add_argument("--smoothing", type=float, default=1e-6)
    p_an.add_argument("--format", type=str, choices=["table", "csv"], default="table")

    p_exp = sub.add_parser("export", help="Export normalized profiles with cid to Parquet")
    p_exp.add_argument("--out", type=str, default="data/bot_store/pdb_profiles_normalized.parquet")

    p_peek = sub.add_parser("peek", help="Peek an API path and print a summary")
    p_peek.add_argument("path", type=str, help="Relative API path, e.g., profiles")
    p_peek.add_argument("--params", type=str, default=None, help='JSON-encoded params, e.g., "{\"limit\":10}"')

    p_get = sub.add_parser("get-profile", help="Fetch a single profile by id (v1)")
    p_get.add_argument("id", type=int, help="Profile ID")
    p_get.add_argument("--include-related", action="store_true", help="Also upsert items from 'related_profiles' if present")
    p_get.add_argument("--embed", action="store_true", help="Run embedding after upserting")

    p_gets = sub.add_parser("get-profiles", help="Fetch multiple v1 profiles by comma-separated IDs")
    p_gets.add_argument("--ids", type=str, required=True, help="Comma-separated profile IDs")
    p_gets.add_argument("--include-related", action="store_true", help="Also upsert items from 'related_profiles' if present")
    p_gets.add_argument("--embed", action="store_true", help="Run embedding after upserting")

    p_disc = sub.add_parser("discover", help="Discover frequent values of fields to guide filters")
    p_disc.add_argument("path", type=str, help="API path, e.g., profiles")
    p_disc.add_argument("--params", type=str, default=None, help='JSON params, e.g., "{\"limit\":200}"')
    p_disc.add_argument("--keys", type=str, default="cid,pid,cat_id,property_id,category_id,type", help="Comma-separated fields to count")

    p_rel = sub.add_parser("related", help="Fetch related profiles (v2) for given profile IDs")
    p_rel.add_argument("--ids", type=str, required=True, help="Comma-separated profile IDs, e.g., 498239,12345")

    sub.add_parser("hot-queries", help="Fetch trending search hot queries (v2) and store raw")

    p_fh = sub.add_parser("follow-hot", help="Resolve stored hot queries via v2 search/top; supports pagination and auto index")
    p_fh.add_argument("--limit", type=int, default=10, help="Max results per page per query")
    p_fh.add_argument("--max-keys", type=int, default=10, help="Max number of hot query keys to follow")
    p_fh.add_argument("--pages", type=int, default=1, help="Number of pages to fetch via nextCursor for each key")
    p_fh.add_argument("--until-empty", action="store_true", help="Keep paging per key until an empty page")
    p_fh.add_argument("--next-cursor", type=int, default=0, help="Starting nextCursor value for paging")
    p_fh.add_argument("--auto-embed", action="store_true", help="Run embedding after ingestion")
    p_fh.add_argument("--auto-index", action="store_true", help="Rebuild FAISS index after ingestion (implies --auto-embed)")
    p_fh.add_argument("--index-out", type=str, default="data/bot_store/pdb_faiss.index", help="Index output path for --auto-index")
    p_fh.add_argument("--lists", type=str, default=None, help="Comma-separated list names to upsert (e.g., profiles,boards)")
    p_fh.add_argument("--only-profiles", action="store_true", help="Shortcut for --lists profiles")
    p_fh.add_argument("--dry-run", action="store_true", help="Preview results without writing/upserting or embedding/indexing")

    p_st = sub.add_parser("search-top", help="Call v2 search/top and upsert list results")
    p_st.add_argument("--query", type=str, default="", help="Query string (passes as 'keyword' if empty fallbacks apply)")
    p_st.add_argument("--keyword", type=str, default=None, help="Explicit 'keyword' param; if set, overrides query")
    p_st.add_argument("--limit", type=int, default=20)
    p_st.add_argument("--next-cursor", type=int, default=0)
    p_st.add_argument("--encoded", action="store_true", help="Treat --query as already URL-encoded (e.g., Elon%%2520Musk)")
    p_st.add_argument("--pages", type=int, default=1, help="Number of pages to fetch via nextCursor")
    p_st.add_argument("--until-empty", action="store_true", help="Keep paging until an empty page")
    p_st.add_argument("--auto-embed", action="store_true", help="Run embedding after ingestion")
    p_st.add_argument("--auto-index", action="store_true", help="Rebuild FAISS index after ingestion (implies --auto-embed)")
    p_st.add_argument("--index-out", type=str, default="data/bot_store/pdb_faiss.index", help="Index output path for --auto-index")
    p_st.add_argument("--lists", type=str, default=None, help="Comma-separated list names to upsert (e.g., profiles,boards)")
    p_st.add_argument("--only-profiles", action="store_true", help="Shortcut for --lists profiles")
    p_st.add_argument("--dry-run", action="store_true", help="Preview results without writing/upserting or embedding/indexing")

    p_dc = sub.add_parser("discover-cidpid", help="Probe cid/pid or cat_id/property_id pairs for non-empty results")
    p_dc.add_argument("--path", type=str, default="profiles", help="API path to probe (default: profiles)")
    p_dc.add_argument("--limit", type=int, default=20, help="Limit per probe request")
    p_dc.add_argument("--sample", type=int, default=200, help="Items to sample for candidate values")
    p_dc.add_argument("--sample-params", type=str, default=None, help='JSON params to include when sampling (e.g., "{\"cid\":15,\"pid\":1}")')
    p_dc.add_argument("--base-params", type=str, default=None, help='JSON params to include in every probe (merged with each pair)')
    p_dc.add_argument("--cids", type=str, default=None, help="Comma-separated cid values to probe (skip sampling)")
    p_dc.add_argument("--pids", type=str, default=None, help="Comma-separated pid values to probe (skip sampling)")
    p_dc.add_argument("--cat-ids", type=str, default=None, help="Comma-separated cat_id values to probe (skip sampling)")
    p_dc.add_argument("--property-ids", type=str, default=None, help="Comma-separated property_id values to probe (skip sampling)")

    p_ir = sub.add_parser("ingest-report", help="Summarize ingested v2 search/top and follow-hot items")
    p_ir.add_argument("--top-queries", type=int, default=5, help="Top N queries per list to show")

    # New: scan-related orchestrates v2 related → optional search-top by names → v1 profile scrape
    p_scan = sub.add_parser("scan-related", help="Scan seeds, fetch v2 related, optionally search names, and scrape v1 profiles")
    p_scan.add_argument("--seed-ids", type=str, default=None, help="Comma-separated seed profile IDs; if omitted, seeds are inferred from raw parquet")
    p_scan.add_argument("--max-seeds", type=int, default=100, help="Max number of seeds to process when inferred")
    p_scan.add_argument("--depth", type=int, default=1, help="Traversal depth for related expansion (currently supports 1)")
    p_scan.add_argument("--v1-base-url", type=str, default="https://api.personality-database.com/api/v1", help="Base URL for v1 profile fetches")
    p_scan.add_argument("--v1-headers", type=str, default=None, help="Headers JSON for v1 requests (merged last)")
    p_scan.add_argument("--search-names", action="store_true", help="For each related item, call v2 search/top using its name")
    p_scan.add_argument("--limit", type=int, default=20, help="Limit per search-top page when --search-names is set")
    p_scan.add_argument("--pages", type=int, default=1, help="Pages per name for search-top when --search-names")
    p_scan.add_argument("--until-empty", action="store_true", help="Keep paging names until empty when --search-names")
    p_scan.add_argument("--lists", type=str, default=None, help="Comma-separated list names to upsert from search-top (e.g., profiles)")
    p_scan.add_argument("--only-profiles", action="store_true", help="Shortcut for --lists profiles for search-top")
    p_scan.add_argument("--auto-embed", action="store_true", help="Run embedding after scraping")
    p_scan.add_argument("--auto-index", action="store_true", help="Rebuild FAISS index after scraping (implies --auto-embed)")
    p_scan.add_argument("--index-out", type=str, default="data/bot_store/pdb_faiss.index", help="Index output path for --auto-index")
    p_scan.add_argument("--dry-run", action="store_true", help="Preview without upserts/embedding/indexing")

    args = parser.parse_args()

    if args.cmd == "dump":
        try:
            client = _make_client(args)
            asyncio.run(cmd_dump(cid=args.cid, pid=args.pid, max_records=args.max, start_offset=args.start_offset, client=client))
        except Exception as e:
            print(f"Dump failed: {e}\nHint: If the API requires auth, set PDB_API_TOKEN or PDB_API_HEADERS.")
    elif args.cmd == "dump-any":
        async def _run():
            client = _make_client(args)
            store = PdbStorage()
            batch: list[dict] = []
            count = 0
            async for item in client.iter_profiles_any(start_offset=args.start_offset):
                batch.append(item)
                if len(batch) >= 100:
                    store.upsert_raw(batch)
                    batch.clear()
                count += 1
                if args.max and count >= args.max:
                    break
            if batch:
                store.upsert_raw(batch)
            print(f"Dumped {count} profiles (unfiltered)")
        try:
            asyncio.run(_run())
        except Exception as e:
            print(f"Dump-any failed: {e}\nHint: If the API requires auth, set PDB_API_TOKEN or PDB_API_HEADERS.")
    elif args.cmd == "embed":
        try:
            cmd_embed()
        except Exception as e:
            print(f"Embed failed: {e}")
    elif args.cmd == "search":
        try:
            cmd_search(args.query, top_k=args.top)
        except Exception as e:
            print(f"Search failed: {e}")
    elif args.cmd == "index":
        try:
            import faiss  # type: ignore
            import numpy as _np
            from pathlib import Path as _Path
            store = PdbStorage()
            df = store.load_joined()
            rows = df.dropna(subset=["vector"]).reset_index(drop=True)
            if rows.empty:
                print("No vectors found; run embed first.")
                return
            mat = _np.vstack(rows["vector"].to_list()).astype("float32")
            # normalize for cosine similarity via inner product
            norms = _np.linalg.norm(mat, axis=1, keepdims=True) + 1e-12
            mat = mat / norms
            index = faiss.IndexFlatIP(mat.shape[1])
            index.add(mat)
            outp = _Path(args.out)
            outp.parent.mkdir(parents=True, exist_ok=True)
            faiss.write_index(index, str(outp))
            # write cid map alongside
            (outp.with_suffix(outp.suffix + ".cids")).write_text("\n".join(rows["cid"].astype(str).tolist()), encoding="utf-8")
            print(f"Indexed {len(rows)} vectors to {outp}")
        except Exception as e:
            print(f"Indexing failed: {e}")
    elif args.cmd == "search-faiss":
        import numpy as np
        from pathlib import Path
        import faiss  # type: ignore
        # load index and cids
        idxp = Path(args.index)
        map_path = idxp.with_suffix(idxp.suffix + ".cids")
        if not idxp.exists() or not map_path.exists():
            print(f"Missing index or cid map at {idxp} and {map_path}. Run 'pdb-cli index' first.")
            return
        index = faiss.read_index(str(idxp))
        cids = map_path.read_text(encoding="utf-8").splitlines()
        # embed query and normalize
        qv = np.array(embed_texts([args.query])[0], dtype="float32")[None, :]
        qv = qv / (np.linalg.norm(qv, axis=1, keepdims=True) + 1e-12)
        scores, I = index.search(qv, args.top)
        for rank, (i, s) in enumerate(zip(I[0], scores[0]), start=1):
            if i < 0 or i >= len(cids):
                continue
            print(f"{rank}. cid={cids[int(i)][:12]} score={float(s):.4f}")
    elif args.cmd == "summarize":
        import pandas as pd
        from pathlib import Path
        npath = Path(args.normalized)
        if not npath.exists():
            print(f"Missing normalized parquet: {npath}. Run 'pdb-cli export' first.")
            return
        df = pd.read_parquet(npath)
        print(f"Rows: {len(df)}")
        for col in ["mbti", "socionics", "big5"]:
            if col in df.columns:
                vc = df[col].dropna().value_counts().head(10)
                if not vc.empty:
                    print(f"Top {col} values:")
                    for k, v in vc.items():
                        print(f"  {k}: {int(v)}")
    elif args.cmd == "cache-clear":
        from pathlib import Path
        import shutil
        c = _make_client(args)
        d = getattr(c, "_cache_dir", None)
        if not d:
            print("Cache directory not available.")
            return
        d = Path(d)
        if not d.exists():
            print(f"No cache directory at {d}")
            return
        shutil.rmtree(d)
        print(f"Cleared cache at {d}")
    elif args.cmd == "analyze":
        res = analyze_kl(args.file, top_k=args.top, smoothing=args.smoothing)
        if res.empty:
            print("No results (check input file schema or data coverage).")
        else:
            if args.format == "csv":
                print(res.to_csv(index=False))
            else:
                # simple table
                for _, r in res.iterrows():
                    print(f"q={r['question']}	{r['type_a']} vs {r['type_b']}	jsd={r['jsd']:.4f}	kl_ab={r['kl_ab']:.4f}	kl_ba={r['kl_ba']:.4f}")
    elif args.cmd == "export":
        import pandas as pd
        from pathlib import Path

        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing source parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows = []
        for _, row in df.iterrows():
            cid = row["cid"]
            pb = row["payload_bytes"]
            try:
                if isinstance(pb, (bytes, bytearray)):
                    obj = orjson.loads(pb)
                elif isinstance(pb, str):
                    obj = json.loads(pb)
                elif isinstance(pb, dict):
                    obj = pb
                else:
                    obj = None
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            norm = normalize_profile(obj)
            rows.append({"cid": cid, **norm})
        out = pd.DataFrame(rows)
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(out_path, index=False)
        print(f"Wrote {len(out)} rows to {out_path}")
    elif args.cmd == "peek":
        import json as _json
        params = None
        if args.params:
            try:
                params = _json.loads(args.params)
            except Exception as e:
                print(f"Invalid JSON for --params: {e}")
                return
        async def _run():
            client = _make_client(args)
            try:
                data = await client.fetch_json(args.path, params)
            except Exception as e:
                print(f"Peek failed: {e}\nHint: Set PDB_API_TOKEN or PDB_API_HEADERS if required.")
                return
            # Print summary
            if isinstance(data, list):
                print(f"Got list with {len(data)} items. First keys: {list(data[0].keys()) if data else []}")
            elif isinstance(data, dict):
                top_keys = list(data.keys())
                items = data.get("data") or data.get("results") or data.get("profiles") or data.get("related") or []
                nested_keys = []
                # drill into data dict if it contains the list
                if isinstance(items, dict):
                    nested_keys = list(items.keys())
                    # heuristic: pick the first list-valued field
                    list_fields = [k for k, v in items.items() if isinstance(v, list)]
                    if list_fields:
                        items = items[list_fields[0]]
                    else:
                        items = items.get("related") or items.get("profiles") or items.get("results") or []
                err = data.get("error")
                print(f"Got dict keys: {top_keys}. Items: {len(items) if isinstance(items, list) else 0}")
                if nested_keys:
                    print(f"Data nested keys: {nested_keys}")
                if err:
                    print(f"Error: {err}")
                if isinstance(items, list) and items:
                    print(f"First item keys: {list(items[0].keys())}")
            else:
                print(f"Got {type(data).__name__}")
        asyncio.run(_run())
    elif args.cmd == "get-profiles":
        async def _run():
            client = _make_client(args)
            store = PdbStorage()
            try:
                ids = [int(x.strip()) for x in (args.ids or '').split(',') if x.strip()]
            except Exception:
                print("Invalid --ids; expected comma-separated integers")
                return
            total_new = total_upd = 0
            for pid in ids:
                try:
                    data = await client.get_profile(pid)
                except Exception as e:
                    print(f"Fetch failed for id={pid}: {e}")
                    continue
                if not isinstance(data, dict):
                    print(f"Unexpected response for id={pid}")
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                total_new += n
                total_upd += u
                if args.include_related:
                    rel = data.get("related_profiles") if isinstance(data, dict) else None
                    rel_items = []
                    if isinstance(rel, list):
                        for it in rel:
                            if isinstance(it, dict):
                                rel_items.append({**it, "_source": "v1_profile_related", "_parent_profile_id": pid})
                    if rel_items:
                        n2, u2 = store.upsert_raw(rel_items)
                        total_new += n2
                        total_upd += u2
                print(f"id={pid}: upserted {n} new, {u} updated" + (f", plus {len(rel_items)} related" if args.include_related else ""))
            print(f"Done. Total upserts: {total_new} new, {total_upd} updated")
            if args.embed:
                try:
                    cmd_embed()
                except Exception as e:
                    print(f"Embed failed after get-profiles: {e}")
        asyncio.run(_run())
    elif args.cmd == "discover":
        import json as _json
        from collections import Counter
        params = None
        if args.params:
            try:
                params = _json.loads(args.params)
            except Exception as e:
                print(f"Invalid JSON for --params: {e}")
                return
        fields = [k.strip() for k in (args.keys or '').split(',') if k.strip()]
        async def _run():
            client = _make_client(args)
            try:
                data = await client.fetch_json(args.path, params)
            except Exception as e:
                print(f"Discover failed: {e}\nHint: Set PDB_API_TOKEN or PDB_API_HEADERS if required.")
                return
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("data") or data.get("results") or data.get("profiles") or data.get("related") or []
                if isinstance(items, dict):
                    list_fields = [k for k, v in items.items() if isinstance(v, list)]
                    if list_fields:
                        items = items[list_fields[0]]
                    else:
                        items = items.get("related") or items.get("profiles") or items.get("results") or []
            if not isinstance(items, list):
                print("No list of items found in response.")
                return
            for f in fields:
                c = Counter()
                for it in items:
                    v = it.get(f) if isinstance(it, dict) else None
                    if isinstance(v, (str, int)):
                        c[v] += 1
                most = c.most_common(10)
                if most:
                    print(f"Field '{f}':")
                    for val, cnt in most:
                        print(f"  {val}: {cnt}")
        asyncio.run(_run())
    elif args.cmd == "get-profile":
        async def _run():
            client = _make_client(args)
            store = PdbStorage()
            try:
                data = await client.get_profile(args.id)
            except Exception as e:
                print(f"Get-profile failed: {e}\nHint: Ensure PDB_API_BASE_URL is v1 and headers/cookies are set.")
                return
            if not isinstance(data, dict):
                print("Unexpected response shape; expected a single object.")
                return
            # annotate provenance
            main_obj = {**data, "_source": "v1_profile", "_profile_id": args.id}
            new, upd = store.upsert_raw([main_obj])
            total_new, total_upd = new, upd
            # optionally include related_profiles array
            if args.include_related:
                rel = data.get("related_profiles") if isinstance(data, dict) else None
                rel_items = []
                if isinstance(rel, list):
                    for it in rel:
                        if isinstance(it, dict):
                            rel_items.append({**it, "_source": "v1_profile_related", "_parent_profile_id": args.id})
                if rel_items:
                    n2, u2 = store.upsert_raw(rel_items)
                    total_new += n2
                    total_upd += u2
            print(f"Upserted {total_new} new, {total_upd} updated for profile id={args.id}")
            if args.embed:
                try:
                    cmd_embed()
                except Exception as e:
                    print(f"Embed failed after get-profile: {e}")
        asyncio.run(_run())
    elif args.cmd == "related":
        # Requires PDB_API_BASE_URL to point to v2 (https://api.personality-database.com/api/v2)
        # and appropriate headers (often including Referer/Origin and possibly Cookie)
        async def _run():
            client = _make_client(args)
            store = PdbStorage()
            try:
                ids = [int(x.strip()) for x in (args.ids or "").split(',') if x.strip()]
            except Exception:
                print("Invalid --ids; expected comma-separated integers")
                return
            total = 0
            for pid in ids:
                try:
                    data = await client.fetch_json(f"profiles/{pid}/related")
                except Exception as e:
                    print(f"Fetch failed for id={pid}: {e}")
                    continue
                items = []
                if isinstance(data, dict):
                    container = data.get("data") or data
                    if isinstance(container, dict):
                        # Prefer 'relatedProfiles' if present
                        rp = container.get("relatedProfiles")
                        if isinstance(rp, list):
                            items = rp
                        else:
                            # fallback: collect all list-valued fields
                            for k, v in container.items():
                                if isinstance(v, list):
                                    for it in v:
                                        if isinstance(it, dict):
                                            it = {**it, "_source_list": k}
                                        items.append(it)
                # annotate source id
                out = []
                for it in items:
                    if isinstance(it, dict):
                        it = {**it, "_source_profile_id": pid}
                        out.append(it)
                if not out:
                    print(f"No related items for id={pid}")
                    continue
                new, upd = store.upsert_raw(out)
                total += new + upd
                print(f"id={pid}: upserted {new} new, {upd} updated")
            print(f"Done. Total upserts: {total}")
        asyncio.run(_run())
    elif args.cmd == "hot-queries":
        # v2 search/hot_queries: upsert data.queries
        async def _run():
            client = _make_client(args)
            store = PdbStorage()
            try:
                data = await client.fetch_json("search/hot_queries")
            except Exception as e:
                print(f"Fetch failed: {e}")
                return
            items = []
            if isinstance(data, dict):
                container = data.get("data") or data
                if isinstance(container, dict):
                    q = container.get("queries")
                    if isinstance(q, list):
                        items = q
            if not items:
                print("No queries found.")
                return
            # annotate for provenance
            out = []
            for it in items:
                if isinstance(it, dict):
                    it = {**it, "_source": "search_hot_queries"}
                    out.append(it)
            new, upd = store.upsert_raw(out)
            print(f"Upserted {new} new, {upd} updated from hot queries")
        asyncio.run(_run())
    elif args.cmd == "follow-hot":
        import pandas as pd

        async def _run():
            store = PdbStorage()
            raw_path = store.raw_path
            if not raw_path.exists():
                print(f"Missing raw parquet: {raw_path}. Run 'pdb-cli hot-queries' first.")
                return
            df = pd.read_parquet(raw_path)
            keys: list[str] = []
            for _, row in df.iterrows():
                pb = row.get("payload_bytes")
                try:
                    obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
                except Exception:
                    obj = None
                if isinstance(obj, dict) and obj.get("_source") == "search_hot_queries":
                    k = obj.get("key")
                    if isinstance(k, str) and k not in keys:
                        keys.append(k)
                if len(keys) >= args.max_keys:
                    break
            if not keys:
                print("No hot query keys found in raw parquet.")
                return
            client = _make_client(args)
            grand_new = 0
            grand_upd = 0
            grand_would_be = 0  # for dry-run reporting
            # parse filters
            allowed_lists = None
            if args.only_profiles:
                allowed_lists = {"profiles"}
            elif args.lists:
                allowed_lists = {s.strip() for s in args.lists.split(',') if s.strip()}
            for k in keys:
                next_cursor = args.next_cursor
                page = 0
                while True:
                    page += 1
                    params = {"keyword": k, "limit": args.limit, "nextCursor": next_cursor}
                    try:
                        data = await client.fetch_json("search/top", params)
                    except Exception as e:
                        print(f"Fetch failed for key={k!r} page {page}: {e}")
                        break
                    container = data.get("data") if isinstance(data, dict) else None
                    if not isinstance(container, dict):
                        print(f"No data container for key={k!r} on page {page}")
                        break
                    out = []
                    for fname, val in container.items():
                        if allowed_lists is not None and fname not in allowed_lists:
                            continue
                        if isinstance(val, list) and val:
                            for it in val:
                                if isinstance(it, dict):
                                    out.append({**it, "_source": "search_follow_hot_top", "_source_list": fname, "_query": k, "_page": page, "_nextCursor": next_cursor})
                    if not out:
                        print(f"key={k!r} page {page}: no list items")
                        if args.until_empty:
                            break
                    else:
                        if args.dry_run:
                            n_items = len(out)
                            grand_would_be += n_items
                            print(f"key={k!r} page {page}: would upsert {n_items} items (dry-run)")
                        else:
                            new, upd = store.upsert_raw(out)
                            grand_new += new
                            grand_upd += upd
                            print(f"key={k!r} page {page}: upserted {new} new, {upd} updated")
                    nc = container.get("nextCursor") if isinstance(container, dict) else None
                    if isinstance(nc, int):
                        next_cursor = nc
                    else:
                        next_cursor += 1
                    if not args.until_empty and page >= max(args.pages, 1):
                        break
            if args.dry_run:
                print(f"Done (dry-run). Would upsert {grand_would_be} items in total.")
                return
            print(f"Done. Total upserts: {grand_new} new, {grand_upd} updated")
            if args.auto_embed or args.auto_index:
                try:
                    cmd_embed()
                except Exception as e:
                    print(f"Auto-embed failed: {e}")
            if args.auto_index:
                try:
                    import faiss  # type: ignore
                    import numpy as _np
                    from pathlib import Path as _Path
                    store = PdbStorage()
                    df = store.load_joined()
                    rows = df.dropna(subset=["vector"]).reset_index(drop=True)
                    if rows.empty:
                        print("No vectors found; run embed first.")
                    else:
                        mat = _np.vstack(rows["vector"].to_list()).astype("float32")
                        norms = _np.linalg.norm(mat, axis=1, keepdims=True) + 1e-12
                        mat = mat / norms
                        index = faiss.IndexFlatIP(mat.shape[1])
                        index.add(mat)
                        outp = _Path(args.index_out)
                        outp.parent.mkdir(parents=True, exist_ok=True)
                        faiss.write_index(index, str(outp))
                        (outp.with_suffix(outp.suffix + ".cids")).write_text("\n".join(rows["cid"].astype(str).tolist()), encoding="utf-8")
                        print(f"Indexed {len(rows)} vectors to {outp}")
                except Exception as e:
                    print(f"Auto-index failed: {e}")
        asyncio.run(_run())
    elif args.cmd == "search-top":
        async def _run():
            client = _make_client(args)
            store = PdbStorage()
            q = args.keyword if args.keyword is not None else args.query
            allowed_lists = None
            if args.only_profiles:
                allowed_lists = {"profiles"}
            elif args.lists:
                allowed_lists = {s.strip() for s in args.lists.split(',') if s.strip()}
            next_cursor = args.next_cursor
            total_new = total_upd = 0
            total_would_be = 0  # for dry-run reporting
            page = 0
            while True:
                page += 1
                if args.encoded and q is not None:
                    keyname = "query" if args.keyword is None else "keyword"
                    path = f"search/top?{keyname}={q}&limit={args.limit}&nextCursor={next_cursor}"
                    params = None
                else:
                    params = {"limit": args.limit, "nextCursor": next_cursor}
                    if q is not None:
                        keyname = "query" if args.keyword is None else "keyword"
                        params[keyname] = q
                    path = "search/top"
                try:
                    data = await client.fetch_json(path, params)
                except Exception as e:
                    print(f"search-top fetch failed (page {page}): {e}")
                    break
                container = data.get("data") if isinstance(data, dict) else None
                if not isinstance(container, dict):
                    print("No data container in response.")
                    break
                out = []
                for fname, val in container.items():
                    if allowed_lists is not None and fname not in allowed_lists:
                        continue
                    if isinstance(val, list) and val:
                        for it in val:
                            if isinstance(it, dict):
                                out.append({**it, "_source": "v2_search_top", "_source_list": fname, "_query": q, "_page": page, "_nextCursor": next_cursor})
                if not out:
                    print(f"Page {page}: no list items found.")
                    if args.until_empty:
                        break
                else:
                    if args.dry_run:
                        n_items = len(out)
                        total_would_be += n_items
                        print(f"Page {page}: would upsert {n_items} items from search/top (dry-run)")
                    else:
                        new, upd = store.upsert_raw(out)
                        total_new += new
                        total_upd += upd
                        print(f"Page {page}: upserted {new} new, {upd} updated from search/top")
                # Determine nextCursor if exposed (may be in container or top-level)
                nc = container.get("nextCursor") if isinstance(container, dict) else None
                if isinstance(nc, int):
                    next_cursor = nc
                else:
                    # if not provided, break unless pages remain
                    next_cursor += 1  # naive fallback to make progress if server expects incremental cursor
                if not args.until_empty and page >= max(args.pages, 1):
                    break
            if args.dry_run:
                print(f"Done (dry-run). Would upsert {total_would_be} items in total.")
                return
            print(f"Done. Total upserts: {total_new} new, {total_upd} updated")
        asyncio.run(_run())
    elif args.cmd == "discover-cidpid":
        import json as _json
        from collections import Counter
        async def _run():
            client = _make_client(args)
            sample_params = {}
            base_params = {}
            if args.sample_params:
                try:
                    sample_params = _json.loads(args.sample_params)
                except Exception as e:
                    print(f"Invalid JSON for --sample-params: {e}")
                    return
            if args.base_params:
                try:
                    base_params = _json.loads(args.base_params)
                except Exception as e:
                    print(f"Invalid JSON for --base-params: {e}")
                    return
            # Build candidate sets either from explicit args or by sampling
            def _parse_csv(s: str | None):
                if not s:
                    return []
                out = []
                for tok in s.split(','):
                    tok = tok.strip()
                    if not tok:
                        continue
                    try:
                        out.append(int(tok))
                    except Exception:
                        pass
                return out
            cid_top = _parse_csv(args.cids)
            pid_top = _parse_csv(args.pids)
            cat_top = _parse_csv(args.cat_ids)
            prop_top = _parse_csv(args.property_ids)
            if not (cid_top or pid_top or cat_top or prop_top):
                # Sample to infer candidates
                try:
                    sample = await client.fetch_json(args.path, {**sample_params, "limit": args.sample})
                except Exception as e:
                    print(f"Discover-cidpid failed to sample: {e}\nHint: Ensure auth via PDB_API_TOKEN or PDB_API_HEADERS.")
                    return
                items = sample if isinstance(sample, list) else (sample.get("data") or sample.get("results") or sample.get("profiles") or sample.get("related") or [])
                if isinstance(items, dict):
                    items = items.get("related") or items.get("profiles") or items.get("results") or []
                if not isinstance(items, list) or not items:
                    print("No items found in initial sample.")
                    return
                cid_vals = Counter()
                pid_vals = Counter()
                cat_vals = Counter()
                prop_vals = Counter()
                for it in items:
                    if isinstance(it, dict):
                        if (v := it.get("cid")) is not None:
                            cid_vals[v] += 1
                        if (v := it.get("pid")) is not None:
                            pid_vals[v] += 1
                        if (v := it.get("cat_id")) is not None:
                            cat_vals[v] += 1
                        if (v := it.get("property_id")) is not None:
                            prop_vals[v] += 1
                cid_top = [v for v,_ in cid_vals.most_common(5)]
                pid_top = [v for v,_ in pid_vals.most_common(5)]
                cat_top = [v for v,_ in cat_vals.most_common(5)]
                prop_top = [v for v,_ in prop_vals.most_common(5)]
            # First, sample items to collect candidate values
            tested = set()
            found = []
            async def probe(params):
                try:
                    data = await client.fetch_json(args.path, {**base_params, **params, "limit": args.limit})
                except Exception:
                    return 0
                items = data if isinstance(data, list) else (data.get("data") or data.get("results") or data.get("profiles") or [])
                return len(items) if isinstance(items, list) else 0
            # Try cid/pid
            for c in cid_top:
                for p in pid_top:
                    key = ("cid", c, "pid", p)
                    if key in tested:
                        continue
                    tested.add(key)
                    n = await probe({"cid": c, "pid": p})
                    if n:
                        found.append(({"cid": c, "pid": p}, n))
                        print(f"Found {n} items for cid={c}, pid={p}")
            # Try cat_id/property_id
            for c in cat_top:
                for p in prop_top:
                    key = ("cat_id", c, "property_id", p)
                    if key in tested:
                        continue
                    tested.add(key)
                    n = await probe({"cat_id": c, "property_id": p})
                    if n:
                        found.append(({"cat_id": c, "property_id": p}, n))
                        print(f"Found {n} items for cat_id={c}, property_id={p}")
            if not found:
                print("No non-empty combinations found in top candidates. Try increasing --sample or adjusting path.")
            else:
                print("Top findings:")
                for params, n in sorted(found, key=lambda x: -x[1])[:10]:
                    print(f"  {params} -> {n} items")
        asyncio.run(_run())
    elif args.cmd == "ingest-report":
        import pandas as pd
        from collections import Counter, defaultdict
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        total = 0
        by_list = Counter()
        by_list_query: dict[str, Counter] = defaultdict(Counter)
        for _, row in df.iterrows():
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            src = obj.get("_source")
            if src not in ("v2_search_top", "search_follow_hot_top"):
                continue
            lst = obj.get("_source_list") or "(unknown)"
            q = obj.get("_query") or ""
            total += 1
            by_list[str(lst)] += 1
            if q:
                by_list_query[str(lst)][str(q)] += 1
        print(f"Total ingested items (search/top + follow-hot): {total}")
        if by_list:
            print("By list (top):")
            for name, cnt in by_list.most_common():
                print(f"  {name}: {cnt}")
        for name, counter in by_list_query.items():
            if not counter:
                continue
            print(f"Top queries for list '{name}':")
            for q, cnt in counter.most_common(max(args.top_queries, 1)):
                print(f"  {q}: {cnt}")

    elif args.cmd == "scan-related":
        import pandas as pd
        import json as _json
        from pathlib import Path as _Path

        async def _run():
            # Clients: v2 for related/search, v1 for profile scrape
            client_v2 = _make_client(args)
            # Build v1 client with overrides
            v1_kwargs = {}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            v1_kwargs["base_url"] = args.v1_base_url
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)

            store = PdbStorage()
            raw_path = store.raw_path
            seeds: list[int] = []
            # Seed IDs from flag or inferred from raw parquet
            if args.seed_ids:
                try:
                    seeds = [int(x.strip()) for x in args.seed_ids.split(',') if x.strip()]
                except Exception:
                    print("Invalid --seed-ids; expected comma-separated integers")
                    return
            else:
                if not raw_path.exists():
                    print(f"Missing raw parquet: {raw_path}. Provide --seed-ids or ingest some data first.")
                    return
                df = pd.read_parquet(raw_path)
                seen = set()
                for _, row in df.iterrows():
                    pb = row.get("payload_bytes")
                    try:
                        obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
                    except Exception:
                        obj = None
                    if not isinstance(obj, dict):
                        continue
                    pid = None
                    if obj.get("_source") == "v1_profile":
                        pid = obj.get("_profile_id")
                    if pid is None and (obj.get("_source_list") == "profiles" or "profiles" in str(obj.get("_source_list", ""))):
                        v = obj.get("id")
                        pid = v if isinstance(v, int) else None
                    if isinstance(pid, int) and pid not in seen:
                        seeds.append(pid)
                        seen.add(pid)
                        if len(seeds) >= max(args.max_seeds, 1):
                            break
            if not seeds:
                print("No seed IDs found.")
                return
            if args.depth != 1:
                print("Warning: --depth>1 not yet implemented; proceeding with depth=1")
            # Pull v2 related for seeds
            total_related_items = 0
            related_ids: set[int] = set()
            allowed_lists = None
            if args.only_profiles:
                allowed_lists = {"profiles"}
            elif args.lists:
                allowed_lists = {s.strip() for s in args.lists.split(',') if s.strip()}
            for pid in seeds:
                try:
                    data = await client_v2.fetch_json(f"profiles/{pid}/related")
                except Exception as e:
                    print(f"Fetch related failed for id={pid}: {e}")
                    continue
                items = []
                if isinstance(data, dict):
                    container = data.get("data") or data
                    if isinstance(container, dict):
                        rp = container.get("relatedProfiles")
                        if isinstance(rp, list):
                            items = rp
                        else:
                            for k, v in container.items():
                                if allowed_lists is not None and k not in allowed_lists:
                                    continue
                                if isinstance(v, list):
                                    for it in v:
                                        if isinstance(it, dict):
                                            it = {**it, "_source_list": k}
                                        items.append(it)
                annotated = []
                newly_ids = 0
                for it in items:
                    if isinstance(it, dict):
                        it = {**it, "_source": "v2_related", "_source_profile_id": pid}
                        vid = it.get("id")
                        if isinstance(vid, int) and vid not in related_ids:
                            related_ids.add(vid)
                            newly_ids += 1
                        annotated.append(it)
                if not annotated:
                    print(f"seed id={pid}: no related items")
                else:
                    if args.dry_run:
                        total_related_items += len(annotated)
                        print(f"seed id={pid}: would upsert {len(annotated)} related items; {newly_ids} new related ids")
                    else:
                        n, u = store.upsert_raw(annotated)
                        total_related_items += n + u
                        print(f"seed id={pid}: upserted {n} new, {u} updated related items; {newly_ids} new related ids")
                # optional: search-top by names from related items
                if args.search_names and items:
                    names: list[str] = []
                    for it in items:
                        if not isinstance(it, dict):
                            continue
                        for k in ("name", "title", "display_name", "username"):
                            v = it.get(k)
                            if isinstance(v, str) and v:
                                names.append(v)
                                break
                    # dedupe names, limit fan-out a bit
                    seen_names = set()
                    for name in names:
                        if name in seen_names:
                            continue
                        seen_names.add(name)
                        next_cursor = 0
                        page = 0
                        while True:
                            page += 1
                            params = {"keyword": name, "limit": args.limit, "nextCursor": next_cursor}
                            try:
                                data = await client_v2.fetch_json("search/top", params)
                            except Exception as e:
                                print(f"search-top failed for name={name!r} page {page}: {e}")
                                break
                            container = data.get("data") if isinstance(data, dict) else None
                            if not isinstance(container, dict):
                                print(f"No data container for name={name!r} page {page}")
                                break
                            out = []
                            for fname, val in container.items():
                                if allowed_lists is not None and fname not in allowed_lists:
                                    continue
                                if isinstance(val, list) and val:
                                    for it in val:
                                        if isinstance(it, dict):
                                            out.append({**it, "_source": "v2_search_top_by_name", "_source_list": fname, "_query": name, "_page": page, "_nextCursor": next_cursor})
                            if not out:
                                if args.until_empty:
                                    break
                            else:
                                if args.dry_run:
                                    print(f"name={name!r} page {page}: would upsert {len(out)} items (dry-run)")
                                else:
                                    new, upd = store.upsert_raw(out)
                                    print(f"name={name!r} page {page}: upserted {new} new, {upd} updated from search/top")
                            nc = container.get("nextCursor") if isinstance(container, dict) else None
                            if isinstance(nc, int):
                                next_cursor = nc
                            else:
                                next_cursor += 1
                            if not args.until_empty and page >= max(args.pages, 1):
                                break
            # Scrape v1 profiles for discovered related IDs
            scraped = 0
            if related_ids:
                for pid in sorted(related_ids):
                    try:
                        data = await client_v1.get_profile(pid)
                    except Exception as e:
                        print(f"v1 get_profile failed for id={pid}: {e}")
                        continue
                    if not isinstance(data, dict):
                        continue
                    main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                    if args.dry_run:
                        scraped += 1
                    else:
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
            if args.dry_run:
                print(f"Scan complete (dry-run). Seeds={len(seeds)}, related_items={total_related_items}, v1_profiles={len(related_ids)}")
                return
            print(f"Scan complete. Seeds={len(seeds)}, related_items_upserts={total_related_items}, v1_profile_upserts={scraped}")
            if args.auto_embed or args.auto_index:
                try:
                    cmd_embed()
                except Exception as e:
                    print(f"Auto-embed failed: {e}")
            if args.auto_index:
                try:
                    import faiss  # type: ignore
                    import numpy as _np
                    store = PdbStorage()
                    df = store.load_joined()
                    rows = df.dropna(subset=["vector"]).reset_index(drop=True)
                    if rows.empty:
                        print("No vectors found; run embed first.")
                    else:
                        mat = _np.vstack(rows["vector"].to_list()).astype("float32")
                        norms = _np.linalg.norm(mat, axis=1, keepdims=True) + 1e-12
                        mat = mat / norms
                        index = faiss.IndexFlatIP(mat.shape[1])
                        index.add(mat)
                        outp = _Path(args.index_out)
                        outp.parent.mkdir(parents=True, exist_ok=True)
                        faiss.write_index(index, str(outp))
                        (outp.with_suffix(outp.suffix + ".cids")).write_text("\n".join(rows["cid"].astype(str).tolist()), encoding="utf-8")
                        print(f"Indexed {len(rows)} vectors to {outp}")
                except Exception as e:
                    print(f"Auto-index failed: {e}")
        asyncio.run(_run())


if __name__ == "__main__":
    main()
