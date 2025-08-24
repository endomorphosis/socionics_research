from __future__ import annotations

import asyncio
import json
from typing import Optional

import numpy as np
import orjson

from .pdb_client import PdbClient
from .pdb_storage import PdbStorage
from .pdb_embed_search import embed_texts, cosine_topk
from .pdb_normalize import normalize_profile
from .pdb_analysis import analyze_kl


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


def _pick_name(obj: dict) -> str:
    for k in ("name", "title", "display_name", "username", "subcategory"):
        v = obj.get(k)
        if isinstance(v, str) and v:
            return v
    return "(unknown)"


def cmd_embed(args) -> None:
    store = PdbStorage()
    df = store.load_joined()
    if df.empty:
        print("No raw data to embed.")
        return
    # Optionally restrict to characters-only cids and prepare alias map
    alias_map: dict[str, list[str]] = {}
    if getattr(args, "chars_only", False):
        try:
            import pandas as _pd
            from pathlib import Path as _Path
            cpath = _Path("data/bot_store/pdb_characters.parquet")
            if not cpath.exists():
                print(f"Missing characters parquet: {cpath}. Run export-characters first or omit --chars-only.")
                return
            cdf = _pd.read_parquet(cpath)
            df = df.merge(cdf[["cid"]], on="cid", how="inner")
            # Build alias map if requested
            if getattr(args, "include_aliases", False) and "alt_names" in cdf.columns:
                for _, r in cdf.iterrows():
                    scid = str(r.get("cid"))
                    nm = r.get("name")
                    alt = r.get("alt_names")
                    pieces: list[str] = []
                    if isinstance(nm, str) and nm.strip():
                        pieces.append(nm.strip())
                    if isinstance(alt, str) and alt:
                        pieces += [p.strip() for p in alt.split(" | ") if p.strip()]
                    if pieces:
                        alias_map[scid] = pieces
        except Exception as e:
            print(f"Failed to load characters parquet: {e}")
            return
    # Select rows to embed
    force = bool(getattr(args, "force", False))
    if not force:
        try:
            mask = df["vector"].isna()
        except Exception:
            mask = [True] * len(df)
        rows = df[mask].reset_index(drop=True)
        if rows.empty:
            print("All selected rows already have vectors. Use --force to overwrite.")
            return
    else:
        rows = df.reset_index(drop=True)
    texts: list[str] = []
    ids: list[str] = []
    for _, row in rows.iterrows():
        pb = row.get("payload_bytes")
        try:
            obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (
                json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None)
            )
        except Exception:
            obj = None
        if not isinstance(obj, dict):
            continue
        name = _pick_name(obj)
        # Fallbacks: prefer human label sources when typical fields are absent
        if not isinstance(name, str) or not name or name == "(unknown)":
            kw = obj.get("_search_keyword") or obj.get("_keyword")
            if isinstance(kw, str) and kw.strip():
                name = kw.strip()
            else:
                st = obj.get("_seed_profile_title")
                if isinstance(st, str) and st.strip():
                    name = st.strip()
                else:
                    try:
                        seed_url = obj.get("_seed_url")
                        if isinstance(seed_url, str) and ("?" in seed_url):
                            from urllib.parse import urlparse as _uparse, parse_qs as _pq, unquote as _unq
                            q = _pq(_uparse(seed_url).query)
                            val = None
                            for kk in ("keyword", "q"):
                                arr = q.get(kk) or []
                                if arr:
                                    val = arr[0]; break
                            if isinstance(val, str) and val:
                                name = _unq(val).strip() or name
                    except Exception:
                        pass

                    if __name__ == "__main__":  # pragma: no cover
                        main()
        if not isinstance(name, str) or not name:
            continue
        cid = str(row.get("cid"))
        ids.append(cid)
        # Build embedding text: name + optional aliases/context
        txt_parts: list[str] = [name]
        if getattr(args, "include_aliases", False):
            aliases = alias_map.get(cid)
            if aliases:
                # avoid duplicating main name
                for a in aliases:
                    if a and a != name:
                        txt_parts.append(a)
        if getattr(args, "include_context", False):
            # include keyword and seed title if present
            for k in ("_search_keyword", "_keyword", "_seed_profile_title"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip() and v not in txt_parts:
                    txt_parts.append(v.strip())
        texts.append(" | ".join(txt_parts))
    if not ids:
        print("No rows to embed.")
        return
    vecs = embed_texts(texts)
    store.upsert_vectors(zip(ids, vecs))
    print(f"Embedded {len(ids)} rows.")


async def cmd_dump(
    cid: int,
    pid: int,
    max_records: Optional[int],
    start_offset: int,
    client: PdbClient,
) -> None:
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
    print(f"Dumped {count} profiles for cid={cid} pid={pid}")


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
    parser.add_argument("--headers", type=str, default=None, help='Extra headers as JSON (merged last). Tip: pass @path or a JSON filepath to read from file.')
    parser.add_argument("--headers-file", type=str, default=None, help="Path to JSON file with extra headers (merged last)")
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
        else:
            try:
                v2_cmds = {
                    "search-top",
                    "search-keywords",
                    "follow-hot",
                    "hot-queries",
                    "find-subcats",
                    "expand-related",
                    "scan-related",
                    "scan-all",
                    "auth-check",
                    # Use v2 for URL expansion: relies on v2 endpoints
                    "expand-from-url",
                }
                if getattr(args, "cmd", None) in v2_cmds:
                    kwargs["base_url"] = "https://api.personality-database.com/api/v2"
            except Exception:
                pass
        # Merge headers from --headers-file or --headers. Support @path or direct path in --headers.
        headers_obj = None
        try:
            import json as _json
            import os as _os
            # Prefer explicit --headers-file when provided
            hdr_path = getattr(args, "headers_file", None)
            raw_hdrs = getattr(args, "headers", None)
            if isinstance(raw_hdrs, str):
                s = raw_hdrs.strip()
                # If --headers looks like @file or an existing file path, read file
                if (s.startswith("@") and _os.path.exists(s[1:])) or _os.path.exists(s):
                    hdr_path = s[1:] if s.startswith("@") else s
            if hdr_path:
                with open(hdr_path, "r", encoding="utf-8") as f:
                    headers_obj = _json.loads(f.read())
            elif isinstance(raw_hdrs, str) and raw_hdrs:
                headers_obj = _json.loads(raw_hdrs)
            else:
                # Convenience: if a default headers file exists, use it
                try:
                    default_hdr = _os.path.join("data", "bot_store", "headers.json")
                    if _os.path.exists(default_hdr):
                        with open(default_hdr, "r", encoding="utf-8") as f:
                            headers_obj = _json.loads(f.read())
                except Exception:
                    headers_obj = None
        except Exception:
            headers_obj = None
        if headers_obj is not None:
            kwargs["headers"] = headers_obj
        return PdbClient(**kwargs)

    p_dump = sub.add_parser("dump", help="Dump profiles to parquet")
    p_dump.add_argument("--cid", type=int, required=True, help="category id (cid)")
    p_dump.add_argument("--pid", type=int, required=True, help="property id (pid)")
    p_dump.add_argument("--max", type=int, default=None, help="optional max records")
    p_dump.add_argument("--start-offset", type=int, default=0, help="start offset for pagination")

    p_embed = sub.add_parser("embed", help="Generate/refresh embeddings")
    p_embed.add_argument("--force", action="store_true", help="Re-embed even if vectors exist")
    p_embed.add_argument("--chars-only", action="store_true", help="Only embed rows present in characters export")
    p_embed.add_argument("--include-aliases", action="store_true", help="When available, include character aliases (alt_names) to enrich embedding text")
    p_embed.add_argument("--include-context", action="store_true", help="Include keyword/seed-title hints in embedding text for better recall")

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

    # Pretty FAISS search: print names
    p_sfp = sub.add_parser("search-faiss-pretty", help="Search FAISS index and show names for top results")
    p_sfp.add_argument("query", type=str)
    p_sfp.add_argument("--top", type=int, default=5)
    p_sfp.add_argument("--index", type=str, default="data/bot_store/pdb_faiss.index")

    # Character-only search shortcut
    p_schar = sub.add_parser("search-characters", help="Search the character-only FAISS index and print names")
    p_schar.add_argument("query", type=str)
    p_schar.add_argument("--top", type=int, default=5)
    p_schar.add_argument("--index", type=str, default="data/bot_store/pdb_faiss_char.index")
    p_schar.add_argument("--contains", type=str, default=None, help="Case-insensitive substring filter on names")
    p_schar.add_argument("--regex", type=str, default=None, help="Regex filter on names (case-insensitive)")
    p_schar.add_argument("--show-aliases", action="store_true", help="Print known aliases for each result if available")
    p_schar.add_argument("--save-csv", type=str, default=None, help="Path to save results as CSV (score,name,cid,aliases)")

    p_sn = sub.add_parser(
        "search-names",
        help="Filter names from raw or character parquet by substring/regex",
    )
    p_sn.add_argument("--contains", type=str, default=None, help="Case-insensitive substring to match against names")
    p_sn.add_argument("--regex", type=str, default=None, help="Regex to match against names")
    p_sn.add_argument(
        "--chars-only",
        action="store_true",
        help="Search only character parquet (export-characters output)",
    )
    p_sn.add_argument("--limit", type=int, default=50, help="Print up to N matches")

    # New: ids-by-name helper to extract profile IDs by name match from raw parquet
    p_ibn = sub.add_parser(
        "ids-by-name",
        help="Scan raw parquet payloads for names matching a substring/regex and print their profile IDs",
    )
    p_ibn.add_argument("--contains", type=str, default=None, help="Case-insensitive substring to match")
    p_ibn.add_argument("--regex", type=str, default=None, help="Regex to match against names")
    p_ibn.add_argument("--limit", type=int, default=50, help="Print up to N matches")

    p_sum = sub.add_parser("summarize", help="Summarize current dataset sizes and type distributions")
    p_sum.add_argument("--normalized", type=str, default="data/bot_store/pdb_profiles_normalized.parquet")

    sub.add_parser("cache-clear", help="Clear local API GET cache if enabled")

    p_cleanup = sub.add_parser("cleanup", help="Clean up parquet databases by removing duplicates and empty rows")
    p_cleanup.add_argument("--dry-run", action="store_true", help="Show what would be cleaned without making changes")
    p_cleanup.add_argument("--edges", action="store_true", help="Also clean edges database")

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

    # Scrape missing v1 profiles for any seen ids in raw parquet
    p_svm = sub.add_parser("scrape-v1-missing", help="Fetch v1 profiles for seen IDs that lack v1_profile entries")
    p_svm.add_argument("--v1-base-url", type=str, default="https://api.personality-database.com/api/v1", help="Base URL for v1 profile fetches")
    p_svm.add_argument("--v1-headers", type=str, default=None, help="Headers JSON for v1 requests (merged last)")
    p_svm.add_argument("--max", type=int, default=0, help="Max number of profiles to fetch (0 = all)")
    p_svm.add_argument("--shuffle", action="store_true", help="Shuffle fetch order")
    p_svm.add_argument("--auto-embed", action="store_true", help="Run embedding after scraping")
    p_svm.add_argument("--auto-index", action="store_true", help="Rebuild FAISS index after scraping (implies --auto-embed)")
    p_svm.add_argument("--index-out", type=str, default="data/bot_store/pdb_faiss.index", help="Index output path for --auto-index")
    p_svm.add_argument("--fallback-v2", action="store_true", help="On v1 error (e.g., 401), attempt v2 profiles/{id} and upsert as v2_profile")
    p_svm.add_argument("--v2-base-url", type=str, default="https://api.personality-database.com/api/v2", help="Base URL for v2 fallback fetches")
    p_svm.add_argument("--v2-headers", type=str, default=None, help="Headers JSON for v2 fallback requests (merged last)")
    p_svm.add_argument("--dry-run", action="store_true", help="Preview without upserts/embedding/indexing")

    p_disc = sub.add_parser("discover", help="Discover frequent values of fields to guide filters")
    p_disc.add_argument("path", type=str, help="API path, e.g., profiles")
    p_disc.add_argument("--params", type=str, default=None, help='JSON params, e.g., "{\"limit\":200}"')
    p_disc.add_argument("--keys", type=str, default="cid,pid,cat_id,property_id,category_id,type", help="Comma-separated fields to count")

    p_rel = sub.add_parser("related", help="Fetch related profiles (v2) for given profile IDs")
    p_rel.add_argument("--ids", type=str, required=True, help="Comma-separated profile IDs, e.g., 498239,12345")

    p_hq = sub.add_parser("hot-queries", help="Fetch trending search hot queries (v2) and store raw")
    p_hq.add_argument("--dry-run", action="store_true", help="Preview without writing/upserting")
    p_hq.add_argument("--verbose", action="store_true", help="Print a sample of returned keywords")

    p_fh = sub.add_parser("follow-hot", help="Resolve stored hot queries via v2 search/top; supports pagination and auto index")
    p_fh.add_argument("--limit", type=int, default=10, help="Max results per page per query")
    p_fh.add_argument("--max-keys", type=int, default=10, help="Max number of hot query keys to follow")
    p_fh.add_argument("--pages", type=int, default=1, help="Number of pages to fetch via nextCursor for each key")
    p_fh.add_argument("--until-empty", action="store_true", help="Keep paging per key until an empty page")
    p_fh.add_argument("--next-cursor", type=int, default=0, help="Starting nextCursor value for paging")
    p_fh.add_argument("--max-no-progress-pages", type=int, default=3, help="Stop if this many consecutive pages yield no new items (0 to disable)")
    p_fh.add_argument("--auto-embed", action="store_true", help="Run embedding after ingestion")
    p_fh.add_argument("--auto-index", action="store_true", help="Rebuild FAISS index after ingestion (implies --auto-embed)")
    p_fh.add_argument("--index-out", type=str, default="data/bot_store/pdb_faiss.index", help="Index output path for --auto-index")
    p_fh.add_argument("--lists", type=str, default=None, help="Comma-separated list names to upsert (e.g., profiles,boards)")
    p_fh.add_argument("--only-profiles", action="store_true", help="Shortcut for --lists profiles")
    p_fh.add_argument("--verbose", action="store_true", help="Print entity names per page per key")
    p_fh.add_argument("--filter-characters", action="store_true", help="Only include items where isCharacter==True when present")
    p_fh.add_argument("--characters-relaxed", action="store_true", help="When filtering characters, allow expanded related items even if isCharacter flag is missing")
    p_fh.add_argument("--expand-subcategories", action="store_true", help="Expand 'subcategories' via profiles/{id}/related to surface profiles")
    p_fh.add_argument("--expand-max", type=int, default=5, help="Max subcategories to expand per page when --expand-subcategories")
    p_fh.add_argument(
        "--force-character-group",
        action="store_true",
        help="Treat expanded subcategories as character groups for relaxed filtering and mapping",
    )
    p_fh.add_argument("--expand-boards", action="store_true", help="For board hits, run search/top on board names and merge their results")
    p_fh.add_argument("--boards-max", type=int, default=5, help="Max boards per page to expand when --expand-boards")
    p_fh.add_argument("--chase-hints", action="store_true", help="If payload contains hint terms, run search/top on them and merge results")
    p_fh.add_argument("--hints-max", type=int, default=5, help="Max hint terms to chase when --chase-hints")
    p_fh.add_argument("--dry-run", action="store_true", help="Preview results without writing/upserting or embedding/indexing")

    p_st = sub.add_parser("search-top", help="Call v2 search/top and upsert list results")
    p_st.add_argument("--query", type=str, default="", help="Query string (passes as 'keyword' if empty fallbacks apply)")
    p_st.add_argument("--keyword", type=str, default=None, help="Explicit 'keyword' param; if set, overrides query")
    p_st.add_argument("--limit", type=int, default=20)
    p_st.add_argument("--next-cursor", type=int, default=0)
    p_st.add_argument("--encoded", action="store_true", help="Treat --query as already URL-encoded (e.g., Elon%%2520Musk)")
    p_st.add_argument("--pages", type=int, default=1, help="Number of pages to fetch via nextCursor")
    p_st.add_argument("--until-empty", action="store_true", help="Keep paging until an empty page")
    p_st.add_argument("--max-no-progress-pages", type=int, default=3, help="Stop if this many consecutive pages yield no new items (0 to disable)")
    p_st.add_argument("--auto-embed", action="store_true", help="Run embedding after ingestion")
    p_st.add_argument("--auto-index", action="store_true", help="Rebuild FAISS index after ingestion (implies --auto-embed)")
    p_st.add_argument("--index-out", type=str, default="data/bot_store/pdb_faiss.index", help="Index output path for --auto-index")
    p_st.add_argument("--lists", type=str, default=None, help="Comma-separated list names to upsert (e.g., profiles,boards)")
    p_st.add_argument("--only-profiles", action="store_true", help="Shortcut for --lists profiles")
    p_st.add_argument("--verbose", action="store_true", help="Print the actual entity names per page")
    p_st.add_argument("--filter-characters", action="store_true", help="Only include items where isCharacter==True when present")
    p_st.add_argument("--characters-relaxed", action="store_true", help="When filtering characters, allow expanded results from subcategories even if isCharacter flag is missing")
    p_st.add_argument("--expand-subcategories", action="store_true", help="Expand search/top 'subcategories' via profiles/{id}/related to surface profiles")
    p_st.add_argument("--expand-max", type=int, default=5, help="Max subcategories to expand per page when --expand-subcategories")
    p_st.add_argument(
        "--force-character-group",
        action="store_true",
        help="Treat expanded subcategories as character groups for relaxed filtering and mapping",
    )
    p_st.add_argument("--expand-boards", action="store_true", help="For board hits, run search/top on board names and merge their results")
    p_st.add_argument("--boards-max", type=int, default=5, help="Max boards per page to expand when --expand-boards")
    p_st.add_argument("--chase-hints", action="store_true", help="If payload contains hint terms, run search/top on them and merge results")
    p_st.add_argument("--hints-max", type=int, default=5, help="Max hint terms to chase when --chase-hints")
    p_st.add_argument("--dry-run", action="store_true", help="Preview results without writing/upserting or embedding/indexing")

    # Bulk keyword search: expand over many queries and ingest results
    p_sb = sub.add_parser(
        "search-keywords",
        help="Call v2 search/top for multiple queries (from --queries/--query-file) and upsert results",
    )
    p_sb.add_argument(
        "--verbose",
        action="store_true",
        help="Print the actual entity names per page (instead of only counts)"
    )
    p_sb.add_argument("--queries", type=str, default=None, help="Comma-separated list of keywords to search (aliases: --keywords, --keyword)")
    # Aliases for ergonomics
    p_sb.add_argument("--keywords", dest="queries", type=str, default=None, help=argparse.SUPPRESS)
    p_sb.add_argument("--keyword", dest="queries", type=str, default=None, help=argparse.SUPPRESS)
    p_sb.add_argument(
        "--query-file",
        type=str,
        default=None,
        help="Path to file containing keywords (comma/newline/space separated). Use '-' for stdin.",
    )
    p_sb.add_argument("--limit", type=int, default=20, help="Limit per page")
    p_sb.add_argument("--pages", type=int, default=1, help="Pages to fetch per query (unless --until-empty)")
    p_sb.add_argument("--until-empty", action="store_true", help="Keep paging per query until empty page")
    p_sb.add_argument("--next-cursor", type=int, default=0, help="Starting nextCursor value for paging")
    p_sb.add_argument(
        "--max-no-progress-pages",
        type=int,
        default=3,
        help="Stop paging after N consecutive pages with no new items (0 to disable)",
    )
    p_sb.add_argument("--lists", type=str, default=None, help="Comma-separated list names to upsert (e.g., profiles,boards)")
    p_sb.add_argument("--only-profiles", action="store_true", help="Shortcut for --lists profiles")
    p_sb.add_argument("--auto-embed", action="store_true", help="Run embedding after ingestion")
    p_sb.add_argument(
        "--auto-index",
        action="store_true",
        help="Rebuild FAISS index after ingestion (implies --auto-embed)",
    )
    p_sb.add_argument(
        "--index-out",
        type=str,
        default="data/bot_store/pdb_faiss.index",
        help="Index output path for --auto-index",
    )
    p_sb.add_argument("--dry-run", action="store_true", help="Preview without writing/upserting or embedding/indexing")
    p_sb.add_argument(
        "--filter-characters",
        action="store_true",
        help="Only include items where isCharacter==True when present"
    )
    p_sb.add_argument(
        "--characters-relaxed",
        action="store_true",
        help="When filtering characters, allow expanded results from subcategories even if isCharacter flag is missing",
    )
    p_sb.add_argument(
        "--expand-subcategories",
        action="store_true",
        help="For subcategory hits, call profiles/{id}/related and include those results (helps surface characters)"
    )
    p_sb.add_argument(
        "--expand-max",
        type=int,
        default=5,
        help="Max subcategories per page to expand when --expand-subcategories is set"
    )
    p_sb.add_argument(
        "--force-character-group",
        action="store_true",
        help="Treat expanded subcategories as character groups for relaxed filtering and mapping",
    )
    p_sb.add_argument(
        "--expand-characters",
        action="store_true",
        help="For each keyword, sweep appended A-Z/0-9 tokens to discover character profiles"
    )
    p_sb.add_argument(
        "--expand-boards",
        action="store_true",
        help="For board hits, run search/top on board names and merge their results",
    )
    p_sb.add_argument("--boards-max", type=int, default=5, help="Max boards per page to expand when --expand-boards")
    p_sb.add_argument(
        "--chase-hints",
        action="store_true",
        help="If payload contains hint terms, run search/top on them and merge results",
    )
    p_sb.add_argument("--hints-max", type=int, default=5, help="Max hint terms to chase when --chase-hints")
    p_sb.add_argument(
        "--expand-pages",
        type=int,
        default=1,
        help="Pages per expanded token (used with --expand-characters)"
    )
    p_sb.add_argument(
        "--append-terms",
        type=str,
        default=None,
        help="Comma-separated suffix terms to append to each keyword (e.g., 'characters,cast')",
    )
    p_sb.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Write verbose output to a file as well as stdout",
    )
    p_sb.add_argument(
        "--html-fallback",
        action="store_true",
        help="If v2 returns no character items for a keyword, scrape the search URL and expand via profiles/{id}/related",
    )
    p_sb.add_argument(
        "--render-js",
        action="store_true",
        help="When using --html-fallback, render pages with a headless browser before scraping",
    )
    p_sb.add_argument(
        "--html-limit",
        type=int,
        default=None,
        help="Optional limit to pass to expand-from-url when parsing search URLs (defaults to --limit)",
    )

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

    # Edges reporting: summarize relationship graph
    p_er = sub.add_parser("edges-report", help="Summarize edges parquet: totals and top degrees")
    p_er.add_argument("--top", type=int, default=10, help="Show top N nodes by out-degree and in-degree")

    # Edges analytics: connected components (undirected) and top component summaries
    p_ea = sub.add_parser(
        "edges-analyze",
        help="Analyze edges parquet: connected components (undirected) and top component degree stats",
    )
    p_ea.add_argument("--top", type=int, default=3, help="Show top N largest components")
    p_ea.add_argument(
        "--per-component-top", type=int, default=5, help="Top nodes by degree to show per component"
    )

    # Edges export: write per-node component and degrees to a Parquet file
    p_ex = sub.add_parser("edges-export", help="Export per-node component id and degrees to Parquet")
    p_ex.add_argument(
        "--out",
        type=str,
        default="data/bot_store/pdb_profile_edges_components.parquet",
        help="Output Parquet path",
    )

    p_ir = sub.add_parser("ingest-report", help="Summarize ingested v2 search/top and follow-hot items")
    p_ir.add_argument("--top-queries", type=int, default=5, help="Top N queries per list to show")

    # Expand related for explicit IDs (helper when known subcategory/board IDs are available)
    p_xrel = sub.add_parser(
        "expand-related",
        help="Fetch profiles/{id}/related for provided IDs and upsert items with optional character filtering",
    )
    p_xrel.add_argument("--ids", type=str, required=False, help="Comma-separated IDs to expand via profiles/{id}/related")
    p_xrel.add_argument(
        "--id-file",
        type=str,
        default=None,
        help="Optional path to a file containing IDs (comma/newline/space separated)",
    )
    p_xrel.add_argument(
        "--max-ids",
        type=int,
        default=0,
        help="Process at most N IDs (0 = all)",
    )
    p_xrel.add_argument("--lists", type=str, default=None, help="Comma-separated list names to upsert (e.g., profiles,boards)")
    p_xrel.add_argument("--only-profiles", action="store_true", help="Shortcut for --lists profiles")
    p_xrel.add_argument("--filter-characters", action="store_true", help="Only include items where isCharacter==True when present")
    p_xrel.add_argument(
        "--characters-relaxed",
        action="store_true",
        help="When filtering characters, accept items inferred from character-group provenance",
    )
    p_xrel.add_argument(
        "--force-character-group",
        action="store_true",
        help="Treat each seed ID as a character-group source for relaxed filtering",
    )
    p_xrel.add_argument("--dry-run", action="store_true", help="Preview without writing/upserting")
    p_xrel.add_argument(
        "--set-keyword",
        type=str,
        default=None,
        help="Optional keyword to tag upserted rows as _search_keyword (useful for survivability and aliasing)",
    )

    # Expand starting from one or more PDB profile URLs
    p_xurl = sub.add_parser(
        "expand-from-url",
        help="Parse PDB profile URLs, extract sub_cat_id via meta, then expand profiles/{sub_cat_id}/related",
    )
    p_xurl.add_argument(
        "--urls",
        type=str,
        default=None,
        help="Comma-separated PDB profile URLs (e.g., https://www.personality-database.com/profile/1061891/...)",
    )
    p_xurl.add_argument(
        "--url-file",
        type=str,
        default=None,
        help="File containing URLs (comma/newline/space separated). Use '-' for stdin.",
    )
    p_xurl.add_argument("--lists", type=str, default=None, help="Comma-separated list names to upsert (e.g., profiles,boards)")
    p_xurl.add_argument("--only-profiles", action="store_true", help="Shortcut for --lists profiles")
    p_xurl.add_argument("--filter-characters", action="store_true", help="Only include items where isCharacter==True when present")
    p_xurl.add_argument(
        "--characters-relaxed",
        action="store_true",
        help="When filtering characters, accept items inferred from character-group provenance",
    )
    p_xurl.add_argument(
        "--force-character-group",
        action="store_true",
        help="Treat discovered sub_cat_id groups as character-group sources for relaxed filtering",
    )
    p_xurl.add_argument("--dry-run", action="store_true", help="Preview without writing/upserting")
    p_xurl.add_argument(
        "--set-keyword",
        type=str,
        default=None,
        help="Force a keyword to attach to upserted rows (as _search_keyword) when expanding from URLs",
    )
    p_xurl.add_argument(
        "--render-js",
        action="store_true",
        help="Render search pages with a headless browser (Playwright) before scraping links",
    )
    p_xurl.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Limit for v2 search/top fallback when parsing search URLs",
    )
    p_xurl.add_argument(
        "--html-file",
        type=str,
        default=None,
        help="Provide an HTML file for a PDB search page; use content instead of fetching",
    )
    p_xurl.add_argument(
        "--html-stdin",
        action="store_true",
        help="Read HTML for a PDB search page from stdin; use content instead of fetching",
    )
    p_xurl.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Write console output to this file as well (tee)",
    )

    # Diagnostics: peek meta payload for given ID(s)
    p_pkm = sub.add_parser(
        "peek-meta",
        help="Fetch https://meta.personality-database.com/api/v2/meta/profile/{id} and print list keys/counts",
    )
    p_pkm.add_argument("--id", type=int, default=None, help="Single profile ID to peek")
    p_pkm.add_argument("--ids", type=str, default=None, help="Comma-separated IDs to peek (in addition to --id)")
    p_pkm.add_argument("--sample", type=int, default=10, help="Print up to N example names from discovered lists")
    p_pkm.add_argument("--raw", action="store_true", help="Print raw JSON payload as well (pretty-printed)")

    # Maintenance: compact raw parquet by recomputing CID without ephemeral fields
    p_comp = sub.add_parser(
        "compact-raw",
        help="Deduplicate raw parquet by recomputing CID from content without ephemeral fields (keys starting with '_')",
    )
    p_comp.add_argument(
        "--out",
        type=str,
        default=None,
        help="Output parquet path (default: write to data/bot_store/pdb_profiles_compacted.parquet)",
    )
    p_comp.add_argument(
        "--replace",
        action="store_true",
        help="Replace the original raw parquet in-place after compaction",
    )
    p_comp.add_argument(
        "--drop-vectors-on-replace",
        action="store_true",
        help="When --replace, drop vectors parquet to avoid CID mismatches (requires re-embedding)",
    )
    p_comp.add_argument("--dry-run", action="store_true", help="Compute stats only, do not write any files")

    # Coverage: quick snapshot of ingestion and vector coverage
    p_cov = sub.add_parser(
        "coverage",
        help="Report counts for raw rows, unique CIDs, vectors present, and v1 profile coverage; show small samples of missing",
    )
    p_cov.add_argument("--sample", type=int, default=10, help="Number of sample IDs to print per missing category")

    # Characters export: filter raw payloads for character-like items into a separate parquet
    p_xc = sub.add_parser(
        "export-characters",
        help="Extract rows likely representing characters (isCharacter True or provenance from character group)",
    )
    p_xc.add_argument(
        "--out",
        type=str,
        default="data/bot_store/pdb_characters.parquet",
        help="Output Parquet path for character-only rows",
    )
    p_xc.add_argument("--sample", type=int, default=20, help="Print first N names as a sample")

    # Character-only index
    p_xci = sub.add_parser(
        "index-characters",
        help="Build a FAISS index from vectors of character-only rows exported by export-characters",
    )
    p_xci.add_argument(
        "--char-parquet",
        type=str,
        default="data/bot_store/pdb_characters.parquet",
        help="Path to characters parquet produced by export-characters",
    )
    p_xci.add_argument("--out", type=str, default="data/bot_store/pdb_faiss_char.index", help="Index output path")

    # Generate/update names file aligned with index cids
    p_rn = sub.add_parser(
        "refresh-names",
        help="Generate or update an index .names file aligned to .cids using characters parquet and payload fallbacks",
    )
    p_rn.add_argument(
        "--index",
        type=str,
        default="data/bot_store/pdb_faiss_char.index",
        help="Path to FAISS index to align names with",
    )
    p_rn.add_argument(
        "--char-parquet",
        type=str,
        default="data/bot_store/pdb_characters.parquet",
        help="Path to characters parquet used for preferred names",
    )

    # Orchestrate ingestion from a list of seed keywords and build the search index
    p_ss = sub.add_parser(
        "scan-seeds",
        help="Ingest character seeds via v2 search and HTML, then export, embed, index, refresh names, and optionally validate",
    )
    # Replace existing keywords arg to be optional and add keywords-file
    p_ss.add_argument("--keywords", type=str, required=False, help="Comma-separated seed keywords, e.g., 'superman,gandalf'")
    p_ss.add_argument(
        "--keywords-file",
        type=str,
        default=None,
        help="Path to file containing keywords (comma/newline/space separated). Use '-' for stdin.",
    )
    p_ss.add_argument("--headers-file", type=str, default="data/bot_store/headers.json", help="Headers JSON file for v2 requests")
    p_ss.add_argument("--limit", type=int, default=40, help="Limit per v2 search page")
    p_ss.add_argument("--pages", type=int, default=2, help="Pages per keyword for v2 search")
    p_ss.add_argument("--index", type=str, default="data/bot_store/pdb_faiss_char.index", help="Index output path")
    p_ss.add_argument("--no-render", action="store_true", help="Skip HTML render fallback for expand-from-url")
    p_ss.add_argument("--validate-top", type=int, default=10, help="Top-K to show per validation query")
    p_ss.add_argument("--validate", action="store_true", help="Run validation searches for each keyword after indexing")
    p_ss.add_argument(
        "--max-seeds",
        type=int,
        default=0,
        help="Process at most N keywords (0 = all)",
    )
    # Optional: broaden coverage by appending terms and alphanumeric sweep
    p_ss.add_argument(
        "--append-terms",
        type=str,
        default=None,
        help="Comma-separated suffix terms to append per keyword for search-keywords (e.g., 'characters,cast')",
    )
    p_ss.add_argument(
        "--sweep-alnum",
        action="store_true",
        help="Enable alphanumeric sweep (A-Z,0-9) via search-keywords --expand-characters",
    )
    p_ss.add_argument(
        "--sweep-pages",
        type=int,
        default=1,
        help="Pages per sweep token when --sweep-alnum is set (maps to search-keywords --expand-pages)",
    )
    p_ss.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Write console output of the entire scan-seeds run to this file as well (tee)",
    )

    # Helper: find subcategories by keyword (to discover character groups)
    p_fsc = sub.add_parser(
        "find-subcats",
        help="List subcategories from v2 search/top for a keyword; useful to find character groups",
    )
    p_fsc.add_argument("--keyword", type=str, required=True, help="Keyword to query in search/top")
    p_fsc.add_argument("--limit", type=int, default=40, help="Limit per page")
    p_fsc.add_argument("--pages", type=int, default=1, help="Pages to fetch (via nextCursor)")
    p_fsc.add_argument("--until-empty", action="store_true", help="Keep paging until an empty page")
    p_fsc.add_argument("--log-file", type=str, default=None, help="Write output to a file as well as stdout")
    p_fsc.add_argument("--ids-out", type=str, default=None, help="Optional path to write discovered subcategory IDs (one per line)")

    # Quick auth/header validation: detect whether v2 search/top returns rich lists
    p_auth = sub.add_parser("auth-check", help="Check v2 auth/headers by probing search/top for character surfacing")
    p_auth.add_argument("--keyword", type=str, default="harry potter", help="Keyword to probe")
    p_auth.add_argument("--limit", type=int, default=20, help="Limit per page")
    p_auth.add_argument("--pages", type=int, default=1, help="Pages to fetch")
    p_auth.add_argument("--log-file", type=str, default=None, help="Write output to a file as well as stdout")

    # Diagnostics: analyze collected results for a given query/substring
    p_diag = sub.add_parser("diagnose-query", help="Analyze collected search/top results for a keyword")
    p_diag.add_argument("--contains", type=str, default=None, help="Substring match on _query to include rows (case-insensitive)")
    p_diag.add_argument("--exact", type=str, default=None, help="Exact match on _query (overrides --contains)")
    p_diag.add_argument("--sources", type=str, default="v2_search_top,v2_search_top_by_name,v2_search_top_sweep,search_follow_hot_top", help="Comma-separated _source names to include")
    p_diag.add_argument("--limit", type=int, default=50, help="Show up to N summary lines of page/cursor stats")

    # New: scan-related orchestrates v2 related → optional search-top by names → v1 profile scrape
    p_scan = sub.add_parser("scan-related", help="Scan seeds, fetch v2 related, optionally search names, and scrape v1 profiles")
    p_scan.add_argument("--seed-ids", type=str, default=None, help="Comma-separated seed profile IDs; if omitted, seeds are inferred from raw parquet")
    p_scan.add_argument(
        "--seed-file",
        type=str,
        default=None,
        help="Path to a file containing seed profile IDs (comma/newline/space separated)",
    )
    p_scan.add_argument("--max-seeds", type=int, default=100, help="Max number of seeds to process when inferred")
    p_scan.add_argument("--depth", type=int, default=1, help="Traversal depth for related expansion (currently supports 1)")
    p_scan.add_argument("--v1-base-url", type=str, default="https://api.personality-database.com/api/v1", help="Base URL for v1 profile fetches")
    p_scan.add_argument("--v1-headers", type=str, default=None, help="Headers JSON for v1 requests (merged last)")
    p_scan.add_argument("--search-names", action="store_true", help="For each related item, call v2 search/top using its name")
    p_scan.add_argument("--limit", type=int, default=20, help="Limit per search-top page when --search-names is set")
    p_scan.add_argument("--pages", type=int, default=1, help="Pages per name for search-top when --search-names")
    p_scan.add_argument("--until-empty", action="store_true", help="Keep paging names until empty when --search-names")
    p_scan.add_argument("--max-no-progress-pages", type=int, default=3, help="Stop name paging after N no-progress pages (0 to disable)")
    p_scan.add_argument("--lists", type=str, default=None, help="Comma-separated list names to upsert from search-top (e.g., profiles)")
    p_scan.add_argument("--only-profiles", action="store_true", help="Shortcut for --lists profiles for search-top")
    p_scan.add_argument("--filter-characters", action="store_true", help="Only include items where isCharacter==True when present")
    p_scan.add_argument("--characters-relaxed", action="store_true", help="When filtering characters, allow expanded results from subcategories even if isCharacter flag is missing")
    p_scan.add_argument("--expand-subcategories", action="store_true", help="Expand search/top 'subcategories' via profiles/{id}/related to surface profiles")
    p_scan.add_argument("--expand-max", type=int, default=5, help="Max subcategories to expand per page when --expand-subcategories")
    p_scan.add_argument(
        "--force-character-group",
        action="store_true",
        help="Treat expanded subcategories as character groups for relaxed filtering and mapping",
    )
    p_scan.add_argument("--auto-embed", action="store_true", help="Run embedding after scraping")
    p_scan.add_argument("--auto-index", action="store_true", help="Rebuild FAISS index after scraping (implies --auto-embed)")
    p_scan.add_argument("--index-out", type=str, default="data/bot_store/pdb_faiss.index", help="Index output path for --auto-index")
    p_scan.add_argument("--dry-run", action="store_true", help="Preview without upserts/embedding/indexing")

    # New: scan-all iteratively expands coverage using v2 related and search/top, with optional query sweep
    p_all = sub.add_parser(
        "scan-all",
        help="Iteratively expand via v2 related and search/top until exhausted; optional v1 scraping and query sweep",
    )
    p_all.add_argument(
        "--seed-ids",
        type=str,
        default=None,
        help="Comma-separated seed profile IDs; if omitted, seeds are inferred from raw parquet",
    )
    p_all.add_argument(
        "--seed-file",
        type=str,
        default=None,
        help="Path to a file containing seed profile IDs (comma/newline/space separated)",
    )
    p_all.add_argument("--max-iterations", type=int, default=5, help="Maximum BFS iterations across related expansions (0 = until exhaustion)")
    p_all.add_argument("--search-names", action="store_true", help="For related items, call v2 search/top by their names")
    p_all.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Limit per search-top page for name/search sweeps",
    )
    p_all.add_argument("--pages", type=int, default=1, help="Pages per search-top call (when not using --until-empty)")
    p_all.add_argument("--until-empty", action="store_true", help="Keep paging search-top until empty pages")
    p_all.add_argument("--max-no-progress-pages", type=int, default=3, help="Stop search paging after N no-progress pages (0 to disable)")
    p_all.add_argument("--lists", type=str, default=None, help="Comma-separated list names to upsert from search-top (e.g., profiles)")
    p_all.add_argument("--only-profiles", action="store_true", help="Shortcut for --lists profiles for search-top")
    p_all.add_argument("--filter-characters", action="store_true", help="Only include items where isCharacter==True when present")
    p_all.add_argument("--characters-relaxed", action="store_true", help="When filtering characters, allow expanded results from subcategories even if isCharacter flag is missing")
    p_all.add_argument("--expand-subcategories", action="store_true", help="Expand search/top 'subcategories' via profiles/{id}/related to surface profiles")
    p_all.add_argument("--expand-max", type=int, default=5, help="Max subcategories to expand per page when --expand-subcategories")
    p_all.add_argument(
        "--force-character-group",
        action="store_true",
        help="Treat expanded subcategories as character groups for relaxed filtering and mapping",
    )
    p_all.add_argument(
        "--sweep-queries",
        type=str,
        default=",".join(list("abcdefghijklmnopqrstuvwxyz") + [str(i) for i in range(10)]),
        help="Comma-separated tokens to sweep via search/top to broaden coverage",
    )
    p_all.add_argument("--sweep-pages", type=int, default=1, help="Pages per sweep query (when not using --sweep-until-empty)")
    p_all.add_argument("--sweep-until-empty", action="store_true", help="Keep paging per sweep query until empty")
    p_all.add_argument("--sweep-into-frontier", action="store_true", help="Add IDs discovered via sweeps into frontier for related expansion")
    p_all.add_argument("--initial-frontier-size", type=int, default=100, help="When inferring seeds from raw, limit initial frontier size")
    p_all.add_argument("--auto-embed", action="store_true", help="Run embedding after expansion")
    p_all.add_argument("--auto-index", action="store_true", help="Rebuild FAISS index after expansion (implies --auto-embed)")
    p_all.add_argument("--index-out", type=str, default="data/bot_store/pdb_faiss.index", help="Index output path for --auto-index")
    p_all.add_argument("--scrape-v1", action="store_true", help="Fetch v1 profile/{id} for newly discovered profile IDs")
    p_all.add_argument("--v1-base-url", type=str, default="https://api.personality-database.com/api/v1", help="Base URL for v1 profile fetches")
    p_all.add_argument("--v1-headers", type=str, default=None, help="Headers JSON for v1 requests (merged last)")

    # Maintenance: retroactively tag rows with a keyword for alias enrichment
    p_tag = sub.add_parser(
        "tag-keyword",
        help="Set _search_keyword on existing raw rows filtered by seed pid/subcategory/source; then re-export to surface aliases",
    )
    p_tag.add_argument("--keyword", type=str, required=True, help="Keyword to set (e.g., 'superman')")
    p_tag.add_argument("--seed-pids", type=str, default=None, help="Comma-separated seed profile IDs to match (_seed_pid)")
    p_tag.add_argument("--subcat-ids", type=str, default=None, help="Comma-separated subcategory IDs to match (_seed_sub_cat_id)")
    p_tag.add_argument(
        "--sources",
        type=str,
        default="v2_related_from_url:profiles",
        help="Comma-separated _source values to include (default: v2_related_from_url:profiles)",
    )
    p_tag.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    p_all.add_argument("--dry-run", action="store_true", help="Preview without upserts/embedding/indexing")
    # State and efficiency
    p_all.add_argument("--use-state", action="store_true", help="Persist and reuse skip-state to avoid reprocessing items across runs")
    p_all.add_argument(
        "--state-file",
        type=str,
        default="data/bot_store/scan_state.json",
        help="Path to JSON state file used when --use-state",
    )
    p_all.add_argument("--state-reset", action="store_true", help="When --use-state, ignore existing state and start fresh")

    # Reusable async helper to expand from URL without re-entering asyncio.run
    async def _expand_from_url_async(args_expand) -> None:
        import re as _re
        import sys as _sys
        from pathlib import Path as _Path
        from urllib.parse import urlparse as _urlparse, parse_qs as _parse_qs
        # Optional: tee stdout to a log file so shell pipelines with --log-file work
        orig_stdout = _sys.stdout
        lf = None
        if getattr(args_expand, "log_file", None):
            try:
                pth = _Path(args_expand.log_file)
                pth.parent.mkdir(parents=True, exist_ok=True)
                lf = pth.open("w", encoding="utf-8")
                class _Tee:
                    def __init__(self, a, b):
                        self.a = a; self.b = b
                    def write(self, s):
                        try:
                            self.a.write(s)
                        except Exception:
                            pass
                        try:
                            self.b.write(s)
                        except Exception:
                            pass
                    def flush(self):
                        try:
                            self.a.flush()
                        except Exception:
                            pass
                        try:
                            self.b.flush()
                        except Exception:
                            pass
                _sys.stdout = _Tee(orig_stdout, lf)
            except Exception:
                lf = None
        base_client = _make_client(args_expand)
        # Build meta client reusing headers/limits
        meta_client = None
        try:
            from .pdb_client import PdbClient as _PdbClient
            meta_client = _PdbClient(
                base_url="https://meta.personality-database.com/api/v2",
                concurrency=getattr(base_client, "concurrency", 4),
                rate_per_minute=getattr(base_client, "rate_per_minute", 60),
                timeout_s=getattr(base_client, "timeout_s", 20.0),
                headers=getattr(base_client, "_extra_headers", None),
            )
        except Exception:
            meta_client = None
        if meta_client is None:
            print("Meta client unavailable; cannot extract sub_cat_id from URLs.")
            # Restore stdout and close log if used
            try:
                _sys.stdout = orig_stdout
                if lf:
                    lf.close()
            except Exception:
                pass
            return
        # Collect URLs
        urls: list[str] = []
        if getattr(args_expand, "urls", None):
            urls.extend([u.strip() for u in str(args_expand.urls).split(",") if u.strip()])
        if getattr(args_expand, "url_file", None):
            try:
                if args_expand.url_file.strip() == "-":
                    txt = _sys.stdin.read()
                else:
                    txt = _Path(args_expand.url_file).read_text(encoding="utf-8")
                for tok in _re.split(r"[\s,]+", txt):
                    t = tok.strip()
                    if t:
                        urls.append(t)
            except Exception:
                pass
        # If any URLs are PDB search pages, expand them into profile/group links, keeping keyword context
        from urllib.parse import unquote as _unquote
        expanded_pairs: list[tuple[str, str | None]] = []  # (url, search_keyword)
        # Optional HTML override content (from --html-file or --html-stdin)
        html_override: str | None = None
        try:
            if getattr(args_expand, "html_file", None):
                html_override = _Path(args_expand.html_file).read_text(encoding="utf-8")
            elif getattr(args_expand, "html_stdin", False):
                html_override = _sys.stdin.read()
        except Exception:
            html_override = None
        for u in urls:
            try:
                parsed = _urlparse(u)
                if parsed.netloc.endswith("personality-database.com") and parsed.path == "/search":
                    # Build best-effort headers, including user-provided cookies
                    import urllib.request as _ur
                    hdrs = {"User-Agent": "Mozilla/5.0"}
                    try:
                        _hdrs = getattr(base_client, "_extra_headers", {}) or {}
                        if isinstance(_hdrs, dict):
                            for hk in ("User-Agent","Accept","Accept-Language","Cookie","Origin","Referer"):
                                v = _hdrs.get(hk) or _hdrs.get(hk.lower())
                                if isinstance(v, str) and v:
                                    hdrs[hk] = v
                    except Exception:
                        pass
                    html = html_override or ""
                    # Optionally render via Playwright for JS-generated content
                    if (not html) and getattr(args_expand, "render_js", False):
                        try:
                            from playwright.async_api import async_playwright  # type: ignore
                            async with async_playwright() as p:
                                browser = await p.chromium.launch(headless=True)
                                context = await browser.new_context(user_agent=hdrs.get("User-Agent") or None)
                                extra = {k: v for k, v in hdrs.items() if k not in {"User-Agent", "Cookie"}}
                                if extra:
                                    try:
                                        await context.set_extra_http_headers(extra)  # type: ignore
                                    except Exception:
                                        pass
                                ck = hdrs.get("Cookie")
                                if isinstance(ck, str) and ck:
                                    try:
                                        cookies = []
                                        for part in ck.split(";"):
                                            part = part.strip()
                                            if not part or "=" not in part:
                                                continue
                                            name, value = part.split("=", 1)
                                            cookies.append({
                                                "name": name.strip(),
                                                "value": value.strip(),
                                                "domain": parsed.hostname or ".personality-database.com",
                                                "path": "/",
                                            })
                                        if cookies:
                                            await context.add_cookies(cookies)  # type: ignore
                                    except Exception:
                                        pass
                                page = await context.new_page()
                                # Scale timeouts from client setting to avoid short 20s default
                                try:
                                    _t_s = float(getattr(base_client, "timeout_s", 20.0) or 20.0)
                                except Exception:
                                    _t_s = 20.0
                                _goto_ms = int(max(_t_s, 5.0) * 2000)  # e.g., timeout_s * 2 (in ms)
                                await page.goto(u, wait_until="load", timeout=_goto_ms)
                                await page.wait_for_timeout(2000)
                                html = await page.content()
                                await context.close()
                                await browser.close()
                        except Exception as e:
                            print(f"[expand-from-url] JS rendering failed or Playwright missing: {e}")
                            print("Hint: pip install playwright && python -m playwright install chromium")
                            html = ""
                    if not html:
                        req = _ur.Request(u, headers=hdrs)
                        # Scale urlopen timeout from client setting
                        try:
                            _t_s = float(getattr(base_client, "timeout_s", 20.0) or 20.0)
                        except Exception:
                            _t_s = 20.0
                        _to = max(int(_t_s * 2), 10)
                        with _ur.urlopen(req, timeout=_to) as resp:
                            html = resp.read().decode("utf-8", errors="ignore")
                    try:
                        _has_links = ("/profile/" in html)
                        print(f"[expand-from-url] fetched search page html_len={len(html)} has_profile_links={_has_links}")
                    except Exception:
                        pass
                    q = _parse_qs(parsed.query)
                    kw = None
                    try:
                        kws = q.get("keyword") or q.get("q") or []
                        for kk in kws:
                            if isinstance(kk, str) and kk:
                                kw = _unquote(kk).strip() or None
                                if kw:
                                    break
                    except Exception:
                        kw = None
                    # Try v2 search/top directly using the keyword (more reliable than scraping)
                    if kw:
                        try:
                            data2 = await base_client.fetch_json("search/top", {"limit": max(int(getattr(args_expand, "limit", 20)), 1), "keyword": kw})
                            payload2 = data2.get("data") if isinstance(data2, dict) else data2
                            if isinstance(payload2, dict):
                                profs = (payload2.get("profiles") or [])
                                for alt_key in ("recommendProfiles", "characters", "relatedProfiles"):
                                    alt_list = payload2.get(alt_key) or []
                                    if alt_list:
                                        if alt_key in ("characters", "relatedProfiles"):
                                            alt_list = [({**it, "_from_character_group": True} if isinstance(it, dict) else it) for it in alt_list]
                                        profs = (profs or []) + alt_list
                                for it in profs or []:
                                    if isinstance(it, dict):
                                        pid = None
                                        for kk in ("id","profileId","profile_id","profileID"):
                                            vv = it.get(kk)
                                            if isinstance(vv, int): pid = vv; break
                                            if isinstance(vv, str) and vv.isdigit(): pid = int(vv); break
                                        if isinstance(pid, int):
                                            expanded_pairs.append((f"https://www.personality-database.com/profile/{pid}", kw))
                                for sc in (payload2.get("subcategories") or []):
                                    if isinstance(sc, dict):
                                        sid = None
                                        for kk in ("id","profileId","profile_id","profileID"):
                                            vv = sc.get(kk)
                                            if isinstance(vv, int): sid = vv; break
                                            if isinstance(vv, str) and vv.isdigit(): sid = int(vv); break
                                        if isinstance(sid, int):
                                            expanded_pairs.append((f"https://www.personality-database.com/profile?sub_cat_id={sid}", kw))
                        except Exception:
                            pass
                    for m in _re.finditer(r'href=[\"\'](/profile/[^\"\']+)[\"\']', html):
                        href = m.group(1)
                        expanded_pairs.append((f"https://www.personality-database.com{href}", kw))
                    for m in _re.finditer(r'href=[\"\'](/profile\?[^\"\']*sub_cat_id=\d+[^\"\']*)[\"\']', html):
                        href = m.group(1)
                        expanded_pairs.append((f"https://www.personality-database.com{href}", kw))
                    if not any('/profile' in p[0] for p in expanded_pairs):
                        for mm in _re.finditer(r'sub_cat_id\s*[:=]\s*(\d+)', html):
                            sid = mm.group(1)
                            synthetic = f"https://www.personality-database.com/profile?sub_cat_id={sid}"
                            expanded_pairs.append((synthetic, kw))
                else:
                    expanded_pairs.append((u, None))
            except Exception:
                expanded_pairs.append((u, None))
        # De-duplicate expanded list (prefer keeping a non-None keyword)
        seen_map: dict[str, str | None] = {}
        for u, kw in expanded_pairs:
            if u not in seen_map or (kw and not seen_map.get(u)):
                seen_map[u] = kw
        url_list = [(u, seen_map[u]) for u in seen_map.keys()]
        if not url_list:
            if urls:
                try:
                    print(f"[expand-from-url] No profile links extracted from provided URL(s); first={urls[0]}")
                except Exception:
                    print("[expand-from-url] No profile links extracted from provided URL(s)")
            else:
                print("No URLs provided. Use --urls or --url-file (or '-' for stdin).")
            # Restore stdout and close log if used
            try:
                _sys.stdout = orig_stdout
                if lf:
                    lf.close()
            except Exception:
                pass
            return
        # Helper: extract numeric profile id from URL path
        def _extract_pid(u: str) -> int | None:
            try:
                m = _re.search(r"/profile/(\d+)", u)
                if m:
                    return int(m.group(1))
            except Exception:
                return None
            return None
        # Lists selection
        target_lists: set[str] | None = None
        if getattr(args_expand, "only_profiles", False):
            target_lists = {"profiles"}
        elif isinstance(getattr(args_expand, "lists", None), str) and args_expand.lists:
            target_lists = {s.strip() for s in args_expand.lists.split(",") if s.strip()}
        store = PdbStorage()
        total = 0
        forced_kw = getattr(args_expand, "set_keyword", None)
        for u, search_kw in url_list:
            pid = _extract_pid(u)
            seed_title: str | None = None
            if isinstance(pid, int):
                try:
                    prof = await base_client.fetch_json(f"profiles/{pid}")
                except Exception:
                    prof = None
                pobj = prof.get("data") if isinstance(prof, dict) else prof
                if isinstance(pobj, dict):
                    for kk in ("name","title","display_name","username","subcategory"):
                        vv = pobj.get(kk)
                        if isinstance(vv, str) and vv.strip():
                            seed_title = vv.strip()
                            break
                    main_obj = {**pobj, "_source": "v2_profile", "_profile_id": pid, "_seed_url": u}
                    kw_to_use = (forced_kw or search_kw)
                    if isinstance(kw_to_use, str) and kw_to_use:
                        main_obj["_search_keyword"] = kw_to_use
                    if getattr(args_expand, "force_character_group", False):
                        try:
                            main_obj.setdefault("_from_character_group", True)
                        except Exception:
                            pass
                    if getattr(args_expand, "filter_characters", False):
                        is_char0 = main_obj.get("isCharacter") is True
                        if not is_char0 and getattr(args_expand, "characters_relaxed", False):
                            is_char0 = main_obj.get("_from_character_group") is True
                        if is_char0:
                            n0, u0 = store.upsert_raw([main_obj])
                            total += (n0 + u0)
                            print(f"url={u} pid={pid}: upserted profile new={n0} updated={u0}")
                    else:
                        n0, u0 = store.upsert_raw([main_obj])
                        total += (n0 + u0)
                        print(f"url={u} pid={pid}: upserted profile new={n0} updated={u0}")
            # sub_cat_id extraction from URL query
            sub_ids: list[int] = []
            try:
                _parsed = _urlparse(u)
                q = _parse_qs(_parsed.query)
                for key in ("sub_cat_id", "subCatId"):
                    vals = q.get(key) or []
                    for vv in vals:
                        if isinstance(vv, str) and vv.isdigit():
                            sub_ids.append(int(vv))
            except Exception:
                pass
            # Fallback to meta if needed
            if not sub_ids and isinstance(pid, int):
                try:
                    mdata = await meta_client.fetch_json(f"meta/profile/{pid}")
                except Exception as e:
                    print(f"meta fetch failed for pid={pid}: {e}")
                    mdata = None
                payload = mdata.get("data") if isinstance(mdata, dict) else mdata
                if isinstance(payload, dict):
                    entries = payload.get("data") if isinstance(payload.get("data"), list) else []
                    for ent in entries:
                        if not isinstance(ent, dict):
                            continue
                        if ent.get("tag") != "script":
                            continue
                        val = ent.get("value")
                        if isinstance(val, str) and "sub_cat_id" in val:
                            for mm in _re.finditer(r"sub_cat_id=(\d+)", val):
                                try:
                                    sid = int(mm.group(1))
                                    sub_ids.append(sid)
                                except Exception:
                                    pass
            sub_ids = list(dict.fromkeys([s for s in sub_ids if isinstance(s, int)]))
            if not sub_ids:
                print(f"url={u}: no sub_cat_id found (query or meta); skipping")
                continue
            for sid in sub_ids:
                lists: dict[str, list] = {}
                try:
                    data = await base_client.fetch_json(f"profiles/{sid}/related")
                except Exception as e:
                    print(f"related fetch failed for sub_cat_id={sid}: {e}")
                    data = None
                rel_payload = data.get("data") if isinstance(data, dict) else data
                if isinstance(rel_payload, dict):
                    rel_list = (
                        rel_payload.get("relatedProfiles")
                        or rel_payload.get("profiles")
                        or rel_payload.get("characters")
                        or []
                    )
                    if isinstance(rel_list, list) and rel_list:
                        lists["profiles"] = rel_list
                if not lists.get("profiles") and meta_client is not None:
                    mpayload = None
                    try:
                        mdata = await meta_client.fetch_json(f"meta/profile/{sid}")
                        mpayload = mdata.get("data") if isinstance(mdata, dict) else mdata
                    except Exception:
                        mpayload = None
                    if isinstance(mpayload, dict):
                        merged: list = []
                        for k in ("profiles", "relatedProfiles", "characters", "recommendProfiles"):
                            v = mpayload.get(k)
                            if isinstance(v, list) and v:
                                if k in ("characters", "relatedProfiles"):
                                    v = [({**it, "_from_character_group": True} if isinstance(it, dict) else it) for it in v]
                                merged.extend(v)
                        if not merged:
                            prof_like: list[dict] = []
                            def _walk(x):
                                try:
                                    if isinstance(x, dict):
                                        has_name = any(
                                            isinstance(x.get(k), str) and x.get(k)
                                            for k in ("name","title","subcategory","display_name","username")
                                        )
                                        id_val = None
                                        for kk in ("id","profileId","profile_id","profileID"):
                                            vv = x.get(kk)
                                            if isinstance(vv, int):
                                                id_val = vv; break
                                            if isinstance(vv, str) and vv.isdigit():
                                                id_val = int(vv); break
                                        if (has_name and id_val is not None) or (x.get("isCharacter") is True):
                                            prof_like.append(x)
                                        for v in x.values():
                                            _walk(v)
                                    elif isinstance(x, list):
                                        for v in x:
                                            _walk(v)
                                except Exception:
                                    return
                            _walk(mpayload)
                            if prof_like:
                                merged.extend(prof_like)
                        if merged:
                            lists["profiles"] = merged
                selected = set(lists.keys()) if target_lists is None else (set(lists.keys()) & set(target_lists))
                batch: list[dict] = []
                for key in sorted(selected):
                    for it in lists.get(key, []) or []:
                        if not isinstance(it, dict):
                            continue
                        obj = {**it, "_source": f"v2_related_from_url:{key}", "_seed_url": u, "_seed_pid": pid, "_seed_sub_cat_id": sid}
                        kw_to_use = (forced_kw or search_kw)
                        if isinstance(kw_to_use, str) and kw_to_use:
                            obj["_search_keyword"] = kw_to_use
                        if isinstance(seed_title, str) and seed_title:
                            obj["_seed_profile_title"] = seed_title
                        if getattr(args_expand, "force_character_group", False):
                            try:
                                obj.setdefault("_from_character_group", True)
                            except Exception:
                                pass
                        if getattr(args_expand, "filter_characters", False):
                            is_char = obj.get("isCharacter") is True
                            if not is_char and getattr(args_expand, "characters_relaxed", False):
                                is_char = obj.get("_from_character_group") is True
                            if not is_char:
                                continue
                        batch.append(obj)
                if not batch:
                    print(f"url={u} sid={sid}: no items after filtering.")
                    continue
                n, u2 = store.upsert_raw(batch)
                total += (n + u2)
                print(f"url={u} sid={sid}: upserted new={n} updated={u2}")
        print(f"Done. Upserted total rows: {total}")
        # Restore stdout and close log if used
        try:
            _sys.stdout = orig_stdout
            if lf:
                lf.close()
        except Exception:
            pass

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
            try:
                from .pdb_edges import PdbEdgesStorage as _PdbEdges
                edges_store = _PdbEdges()
            except Exception:
                edges_store = None
            # no edges recorded for dump-any
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
            cmd_embed(args)
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
        q_list = embed_texts([args.query])[0]
        qv = np.array(q_list, dtype="float32")[None, :]
        # adapt dimension if needed
        if qv.shape[1] != index.d:
            # fallback: simple token-hash embedding to index dimension
            import hashlib as _hashlib
            buckets = [0.0] * index.d
            for tok in (args.query or "").split():
                h = int(_hashlib.sha256(tok.lower().encode()).hexdigest(), 16)
                buckets[h % index.d] += 1.0
            qv = np.array([buckets], dtype="float32")
        qv = qv / (np.linalg.norm(qv, axis=1, keepdims=True) + 1e-12)
        # If filters are provided, search a larger candidate set to improve chances of matches
        desired_top = int(getattr(args, "top", 10) or 10)
        has_filters = bool(getattr(args, "contains", None) or getattr(args, "regex", None))
        search_k = desired_top
        if has_filters:
            # Search up to 1000 or 50x requested, bounded by index size
            search_k = min(len(cids), max(desired_top * 50, 1000))
        scores, I = index.search(qv, search_k)
        for rank, (i, s) in enumerate(zip(I[0], scores[0]), start=1):
            if i < 0 or i >= len(cids):
                continue
            print(f"{rank}. cid={cids[int(i)][:12]} score={float(s):.4f}")
    elif args.cmd in {"search-faiss-pretty", "search-characters"}:
        import numpy as np
        from pathlib import Path
        import faiss  # type: ignore
        import re as _re
        # load index and cid map
        idxp = Path(args.index)
        map_path = idxp.with_suffix(idxp.suffix + ".cids")
        if not idxp.exists() or not map_path.exists():
            print(f"Missing index or cid map at {idxp} and {map_path}. Run the corresponding index command first.")
            return
        index = faiss.read_index(str(idxp))
        cids = map_path.read_text(encoding="utf-8").splitlines()
        # Optional names file aligned with cids
        names_path = idxp.with_suffix(idxp.suffix + ".names")
        # embed and normalize query
        q_list = embed_texts([args.query])[0]
        qv = np.array(q_list, dtype="float32")[None, :]
        # adapt dimension if needed
        if qv.shape[1] != index.d:
            import hashlib as _hashlib
            buckets = [0.0] * index.d
            for tok in (args.query or "").split():
                h = int(_hashlib.sha256(tok.lower().encode()).hexdigest(), 16)
                buckets[h % index.d] += 1.0
            qv = np.array([buckets], dtype="float32")
        qv = qv / (np.linalg.norm(qv, axis=1, keepdims=True) + 1e-12)
        desired_top = int(getattr(args, "top", 10) or 10)
        # If filters are provided, search a larger candidate set
        contains = (getattr(args, "contains", None) or "").strip().lower()
        pat = None
        if getattr(args, "regex", None):
            try:
                pat = _re.compile(args.regex, _re.IGNORECASE)
            except Exception:
                pat = None
        has_filters = bool(contains or pat)
        search_k = desired_top
        if has_filters:
            search_k = min(len(cids), max(desired_top * 50, 1000))
        scores, I = index.search(qv, search_k)
        # Map cids to names using names file when available; fallback to joined store
        cid_to_name: dict[str, str] = {}
        if names_path.exists():
            names = names_path.read_text(encoding="utf-8").splitlines()
            for cid, nm in zip(cids, names):
                cid_to_name[cid] = nm or "(unknown)"
        else:
            store = PdbStorage()
            df = store.load_joined()
            for _, row in df.iterrows():
                cid = str(row.get("cid"))
                if not cid:
                    continue
                pb = row.get("payload_bytes")
                try:
                    obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
                except Exception:
                    obj = None
                name = None
                if isinstance(obj, dict):
                    for k in ("name", "title", "display_name", "username", "subcategory"):
                        v = obj.get(k)
                        if isinstance(v, str) and v:
                            name = v; break
                cid_to_name[cid] = name or "(unknown)"
        # Load names/alt_names from characters parquet when available so filters can match aliases and fill unknowns
        cid_altnames: dict[str, list[str]] = {}
        cid_charnames: dict[str, str] = {}
        try:
            from pathlib import Path as _Path
            char_path = _Path("data/bot_store/pdb_characters.parquet")
            if char_path.exists():
                cdf = __import__("pandas").read_parquet(char_path)
                if "cid" in cdf.columns and "alt_names" in cdf.columns:
                    for _, r in cdf.iterrows():
                        scid = str(r.get("cid"))
                        alt = r.get("alt_names")
                        if isinstance(alt, str) and alt:
                            cid_altnames[scid] = [p.strip() for p in alt.split(" | ") if p.strip()]
                        nmv = r.get("name")
                        if isinstance(nmv, str) and nmv:
                            cid_charnames[scid] = nmv
        except Exception:
            cid_altnames = {}
            cid_charnames = {}
        printed = 0
        results_rows = [] if getattr(args, "save_csv", None) else None
        for i, s in zip(I[0], scores[0]):
            if i < 0 or i >= len(cids):
                continue
            cid = cids[int(i)]
            nm = cid_to_name.get(cid, "(unknown)")
            if (not nm) or nm == "(unknown)":
                nm = cid_charnames.get(cid, nm)
            low = nm.lower()
            # Check contains/regex against name and any known aliases
            alias_list = cid_altnames.get(cid, [])
            alias_match = False
            if contains:
                alias_match = any(contains in (a.lower()) for a in alias_list)
                if (contains not in low) and not alias_match:
                    continue
            if pat:
                alias_match = alias_match or any(pat.search(a or "") for a in alias_list)
                if (not pat.search(nm)) and (not alias_match):
                    continue
            printed += 1
            if getattr(args, "show_aliases", False):
                al = cid_altnames.get(cid, [])
                extra = f" | aliases: {', '.join(al[:5])}" if al else ""
            else:
                extra = ""
            print(f"{printed}. score={float(s):.4f} name={nm} cid={cid[:12]}{extra}")
            if results_rows is not None:
                alias_str = "; ".join(cid_altnames.get(cid, [])[:20]) if cid_altnames.get(cid) else ""
                results_rows.append({"rank": printed, "score": float(s), "name": nm, "cid": cid, "aliases": alias_str})
            if printed >= desired_top:
                break
        # Summary line to ensure some visible output in quiet environments
        try:
            print(f"Found {printed} results (requested top={desired_top})")
        except Exception:
            pass
        # Save CSV if requested; create file even when empty and without pandas
        if results_rows is not None and getattr(args, "save_csv", None):
            csv_path = getattr(args, "save_csv")
            wrote = False
            try:
                import pandas as _pd  # type: ignore
                df_out = _pd.DataFrame(results_rows)
                if df_out.empty:
                    # Ensure header exists even when there are zero rows
                    df_out = _pd.DataFrame(columns=["rank", "score", "name", "cid", "aliases"])
                df_out.to_csv(csv_path, index=False)
                wrote = True
            except Exception:
                # Fallback to Python csv module
                try:
                    import csv as _csv
                    with open(csv_path, "w", encoding="utf-8", newline="") as f:
                        writer = _csv.DictWriter(f, fieldnames=["rank", "score", "name", "cid", "aliases"])
                        writer.writeheader()
                        for row in (results_rows or []):
                            writer.writerow(row)
                    wrote = True
                except Exception as e2:
                    try:
                        print(f"Failed to save CSV to {csv_path}: {e2}")
                    except Exception:
                        pass
            if wrote:
                try:
                    print(f"Saved {len(results_rows)} rows to {csv_path}")
                except Exception:
                    pass
    elif args.cmd == "search-names":
        import pandas as pd
        import re as _re
        from pathlib import Path as _Path
        store = PdbStorage()
        # Choose source parquet
        src_path = _Path("data/bot_store/pdb_characters.parquet") if args.chars_only else store.raw_path
        if not src_path.exists():
            print(f"Missing source parquet: {src_path}")
            return
        df = pd.read_parquet(src_path)
        # Ensure we have names: for raw, parse payloads
        names: list[tuple[str, str]] = []  # (cid, name)
        if not args.chars_only:
            for _, row in df.iterrows():
                cid = str(row.get("cid"))
                pb = row.get("payload_bytes")
                try:
                    obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else None
                except Exception:
                    obj = None
                nm = None
                if isinstance(obj, dict):
                    for k in ("name","title","display_name","username","subcategory"):
                        v = obj.get(k)
                        if isinstance(v, str) and v:
                            nm = v; break
                if nm:
                    names.append((cid, nm))
        else:
            # characters parquet may have name and alt_names
            for _, row in df.iterrows():
                cid = str(row.get("cid"))
                nm = row.get("name")
                alt = row.get("alt_names")
                if isinstance(nm, str) and nm:
                    names.append((cid, nm))
                if isinstance(alt, str) and alt:
                    # Split alternates by ' | '
                    for part in alt.split(" | "):
                        p = part.strip()
                        if p:
                            names.append((cid, p))
        contains = (args.contains or "").strip().lower()
        pattern = None
        if args.regex:
            try:
                pattern = _re.compile(args.regex, _re.IGNORECASE)
            except Exception as e:
                print(f"Invalid regex: {e}")
                return
        out = []
        seen_pairs = set()
        for cid, nm in names:
            key = (cid, nm)
            if key in seen_pairs:
                continue
            low = nm.lower()
            if contains and contains not in low:
                continue
            if pattern and not pattern.search(nm):
                continue
            out.append((cid, nm))
            seen_pairs.add(key)
            if len(out) >= args.limit:
                break
        if not out:
            print("No matches.")
            return
        for cid, nm in out:
            print(f"name={nm} cid={cid[:12]}")
    elif args.cmd == "ids-by-name":
        import pandas as pd
        import re as _re
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        contains = (args.contains or "").strip().lower()
        pattern = None
        if args.regex:
            try:
                pattern = _re.compile(args.regex, _re.IGNORECASE)
            except Exception as e:
                print(f"Invalid regex: {e}")
                return
        count = 0
        for _, row in df.iterrows():
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else None
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            nm = None
            for k in ("name","title","display_name","username","subcategory"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    nm = v
                    break
            if not nm:
                continue
            low = nm.lower()
            if contains and contains not in low:
                continue
            if pattern and not pattern.search(nm):
                continue
            pid = None
            for k in ("id","profileId","profile_id","profileID"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v
                    break
                if isinstance(v, str) and v.isdigit():
                    pid = int(v)
                    break
            if pid is None:
                continue
            print(f"name={nm} pid={pid}")
            count += 1
            if count >= args.limit:
                break
        if count == 0:
            print("No matches.")
    elif args.cmd == "search-keywords":
        import sys as _sys
        from pathlib import Path as _Path
        # Helper to read queries from --queries/--query-file (supports '-' for stdin)
        def _read_queries(qs: Optional[str], qfile: Optional[str]) -> list[str]:
            items: list[str] = []
            src = None
            if qfile:
                if qfile.strip() == "-":
                    try:
                        src = _sys.stdin.read()
                    except Exception:
                        src = None
                else:
                    try:
                        src = _Path(qfile).read_text(encoding="utf-8")
                    except Exception:
                        src = None
            if isinstance(qs, str) and qs:
                items.extend([x.strip() for x in qs.split(",") if x.strip()])
            if isinstance(src, str) and src:
                # split on commas, newlines, or whitespace
                import re as _re
                parts = [p.strip() for p in _re.split(r"[\s,]+", src) if p.strip()]
                items.extend(parts)
            # de-duplicate while preserving order
            seen = set()
            out: list[str] = []
            for it in items:
                if it not in seen:
                    seen.add(it)
                    out.append(it)
            return out

        def _pick_name(obj: dict) -> str:
            for k in ("name", "title", "display_name", "username", "subcategory"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    return v
            return "(unknown)"

        async def _run():
            client = _make_client(args)
            store = PdbStorage()
            log_fp = None
            def _log(msg: str) -> None:
                try:
                    print(msg)
                except Exception:
                    pass
                try:
                    if log_fp:
                        log_fp.write(msg + "\n"); log_fp.flush()
                except Exception:
                    pass
            if getattr(args, "log_file", None):
                try:
                    from pathlib import Path as _Path
                    lf = _Path(args.log_file)
                    lf.parent.mkdir(parents=True, exist_ok=True)
                    log_fp = lf.open("w", encoding="utf-8")
                except Exception:
                    log_fp = None
            _log("[search-keywords] start")
            base_queries = _read_queries(getattr(args, "queries", None), getattr(args, "query_file", None))
            # Expand queries per flags
            expanded: list[tuple[str, bool]] = []
            # Base queries first
            for q in base_queries:
                expanded.append((q, False))
            # Append terms (e.g., "characters", "cast")
            if getattr(args, "append_terms", None):
                for q in base_queries:
                    for term in [t.strip() for t in str(args.append_terms).split(",") if t.strip()]:
                        expanded.append((f"{q} {term}", True))
            # Alphanumeric sweep for character discovery
            if getattr(args, "expand_characters", False):
                sweep = list("abcdefghijklmnopqrstuvwxyz") + [str(i) for i in range(10)]
                for q in base_queries:
                    for tok in sweep:
                        expanded.append((f"{q} {tok}", True))
            # De-duplicate while preserving order
            seen_q = set()
            queries: list[tuple[str, bool]] = []
            for q, is_exp in expanded:
                if q not in seen_q:
                    seen_q.add(q)
                    queries.append((q, is_exp))
            if not queries:
                print("No queries provided. Use --queries or --query-file (or '-' for stdin).")
                return
            # Lists selection
            target_lists: Optional[set[str]] = None
            if args.only_profiles:
                target_lists = {"profiles"}
            elif isinstance(args.lists, str) and args.lists:
                target_lists = {s.strip() for s in args.lists.split(",") if s.strip()}

            total_new = total_updated = 0
            for q, is_expanded in queries:
                cursor = int(getattr(args, "next_cursor", 0)) if hasattr(args, "next_cursor") else 0
                no_prog = 0
                pages = 0
                found_any = 0
                if args.verbose:
                    _log(f"[debug] search-keywords start keyword='{q}' limit={args.limit} only_profiles={args.only_profiles}")
                while True:
                    params = {"limit": args.limit}
                    # For v2 search/top the query param is typically 'keyword'
                    params["keyword"] = q
                    if cursor:
                        params["nextCursor"] = cursor
                    data = None
                    try:
                        data = await client.fetch_json("search/top", params)
                    except Exception as e:
                        print(f"search/top failed for '{q}': {e}")
                        break
                    block = data if isinstance(data, dict) else {}
                    payload = block.get("data") if isinstance(block.get("data"), dict) else block
                    # Determine next cursor
                    next_cur = 0
                    for kc in ("nextCursor", "nextcursor", "next_cursor"):
                        try:
                            v = payload.get(kc) if isinstance(payload, dict) else None
                            if isinstance(v, int):
                                next_cur = v
                                break
                            if isinstance(v, str) and v.isdigit():
                                next_cur = int(v)
                                break
                        except Exception:
                            pass
                    # Collect list items
                    lists: dict[str, list] = {}
                    if isinstance(payload, dict):
                        for k, v in payload.items():
                            if k in {"nextCursor", "keyword", "query"}:
                                continue
                            if isinstance(v, list):
                                lists[k] = v
                        # Map alt keys into profiles so --only-profiles retains them
                        # - recommendProfiles: surfaced by search/top alongside profiles
                        # - characters / relatedProfiles: some responses use these instead of profiles
                        if "recommendProfiles" in lists:
                            recs = lists.get("recommendProfiles") or []
                            if recs:
                                base = lists.get("profiles") or []
                                lists["profiles"] = (base + recs)
                        for alt_key in ("characters", "relatedProfiles"):
                            if alt_key in lists:
                                alt = lists.get(alt_key) or []
                                if alt:
                                    # Preserve provenance so relaxed character filtering can pass
                                    marked = [
                                        ({**it, "_from_character_group": True} if isinstance(it, dict) else it)
                                        for it in alt
                                    ]
                                    base = lists.get("profiles") or []
                                    lists["profiles"] = (base + marked)
                    if args.verbose:
                        key_counts = ", ".join(f"{k}:{len(lists.get(k, []) or [])}" for k in sorted(lists.keys()))
                        _log(f"[debug] page={pages+1} keys={{ {key_counts} }} nextCursor={next_cur}")
                    selected_keys = (
                        set(lists.keys()) if target_lists is None else (set(lists.keys()) & set(target_lists))
                    )
                    # Expand subcategories when requested
                    expanded_profiles: list[dict] = []
                    if args.expand_subcategories and "subcategories" in lists:
                        subcats = lists.get("subcategories", [])[: max(int(args.expand_max), 0)]
                        for sc in subcats:
                            sid = None
                            if isinstance(sc, dict):
                                for key in ("id", "profileId", "profile_id", "profileID"):
                                    v = sc.get(key)
                                    if isinstance(v, int):
                                        sid = v; break
                                    if isinstance(v, str) and v.isdigit():
                                        sid = int(v); break
                            if not isinstance(sid, int):
                                continue
                            try:
                                rel = await client.fetch_json(f"profiles/{sid}/related")
                            except Exception:
                                continue
                            rel_payload = rel.get("data") if isinstance(rel, dict) else rel
                            rel_list = []
                            if isinstance(rel_payload, dict):
                                rel_list = (
                                    rel_payload.get("relatedProfiles")
                                    or rel_payload.get("profiles")
                                    or []
                                )
                            if not isinstance(rel_list, list):
                                continue
                            for it in rel_list:
                                if isinstance(it, dict):
                                    ex = {
                                        **it,
                                        "_source": "v2_related_from_subcategory",
                                        "_keyword": q,
                                        "_from_character_group": True if args.force_character_group else it.get("_from_character_group") or False,
                                    }
                                    expanded_profiles.append(ex)

                    # Optional: expand via boards and chase payload hints
                    async def _expand_by_term(term: str, tag: str) -> list[dict]:
                        try:
                            data2 = await client.fetch_json("search/top", {"limit": args.limit, "keyword": term})
                        except Exception:
                            return []
                        block2 = data2 if isinstance(data2, dict) else {}
                        payload2 = block2.get("data") if isinstance(block2.get("data"), dict) else block2
                        lists2: dict[str, list] = {}
                        if isinstance(payload2, dict):
                            for kk, vv in payload2.items():
                                if kk in {"nextCursor", "keyword", "query"}:
                                    continue
                                if isinstance(vv, list):
                                    lists2[kk] = vv
                            # Apply same survivability mapping to nested expansion
                            if "recommendProfiles" in lists2:
                                recs2 = lists2.get("recommendProfiles") or []
                                if recs2:
                                    base2 = lists2.get("profiles") or []
                                    lists2["profiles"] = (base2 + recs2)
                            for alt_key in ("characters", "relatedProfiles"):
                                if alt_key in lists2:
                                    alt2 = lists2.get(alt_key) or []
                                    if alt2:
                                        marked2 = [
                                            ({**it, "_from_character_group": True} if isinstance(it, dict) else it)
                                            for it in alt2
                                        ]
                                        base2 = lists2.get("profiles") or []
                                        lists2["profiles"] = (base2 + marked2)
                        out: list[dict] = []
                        for ksel in sorted(selected_keys):
                            for it2 in lists2.get(ksel, []) or []:
                                if not isinstance(it2, dict):
                                    continue
                                obj2 = {**it2, "_source": f"v2_search_top_by_{tag}:{ksel}", "_keyword": q}
                                if args.filter_characters:
                                    is_char2 = obj2.get("isCharacter") is True
                                    if not is_char2 and args.characters_relaxed:
                                        is_char2 = obj2.get("_from_character_group") is True
                                    if not is_char2:
                                        continue
                                out.append(obj2)
                        return out
                    extra_items: list[dict] = []
                    if getattr(args, "expand_boards", False) and "boards" in lists:
                        boards = lists.get("boards", [])[: max(int(getattr(args, "boards_max", 0)), 0)]
                        for b in boards:
                            if not isinstance(b, dict):
                                continue
                            bname = None
                            for k in ("name", "title", "display_name", "username", "subcategory"):
                                v = b.get(k)
                                if isinstance(v, str) and v:
                                    bname = v; break
                            if not bname:
                                continue
                            extra_items.extend(await _expand_by_term(bname, "board"))
                    if getattr(args, "chase_hints", False) and isinstance(payload, dict):
                        raw_hints = []
                        for hk in ("hint", "hints", "suggestions", "suggested", "suggestedKeywords", "suggested_terms"):
                            hv = payload.get(hk)
                            if hv is None:
                                continue
                            if isinstance(hv, str):
                                raw_hints.append(hv)
                            elif isinstance(hv, list):
                                for x in hv:
                                    if isinstance(x, str):
                                        raw_hints.append(x)
                                    elif isinstance(x, dict):
                                        for nk in ("name", "title", "keyword", "key", "query", "text", "value"):
                                            nv = x.get(nk)
                                            if isinstance(nv, str):
                                                raw_hints.append(nv); break
                        seen_h = set()
                        hints = []
                        for h in raw_hints:
                            if not isinstance(h, str):
                                continue
                            hh = h.strip()
                            if not hh or hh == q or hh in seen_h:
                                continue
                            seen_h.add(hh)
                            hints.append(hh)
                        hints = hints[: max(int(getattr(args, "hints_max", 0)), 0)]
                        for h in hints:
                            extra_items.extend(await _expand_by_term(h, "hint"))

                    # Build batch with optional character filtering
                    batch: list[dict] = []
                    for key in sorted(selected_keys):
                        items = lists.get(key, [])
                        src_name = f"v2_search_top:{key}"
                        for it in items:
                            if not isinstance(it, dict):
                                continue
                            obj = {**it, "_source": src_name, "_keyword": q}
                            # Filtering
                            if args.filter_characters:
                                is_char = obj.get("isCharacter") is True
                                if not is_char and args.characters_relaxed:
                                    is_char = obj.get("_from_character_group") is True
                                if not is_char:
                                    continue
                            batch.append(obj)
                    if extra_items:
                        batch.extend(extra_items)
                    # Include expanded profiles (treated as profiles)
                    if expanded_profiles:
                        for obj in expanded_profiles:
                            if args.filter_characters:
                                is_char = obj.get("isCharacter") is True or obj.get("_from_character_group") is True if args.characters_relaxed else obj.get("isCharacter") is True
                                if not is_char:
                                    continue
                            batch.append(obj)

                    if args.verbose:
                        names = ", ".join(_pick_name(x) for x in batch[:10])
                        more = max(len(batch) - 10, 0)
                        tail = f" (+{more} more)" if more else ""
                        _log(f"keyword='{q}' page={pages+1} items={len(batch)}{tail}")
                        if names:
                            _log(f"  {names}")

                    new = upd = 0
                    if not args.dry_run and batch:
                        n, u = store.upsert_raw(batch)
                        new += n; upd += u
                        total_new += n; total_updated += u
                    # track that we surfaced some items for this keyword, even if they were duplicates
                    if batch:
                        found_any += len(batch)

                    # progress checks
                    if new == 0:
                        no_prog += 1
                    else:
                        no_prog = 0
                    pages += 1
                    # decide on next page
                    # Determine page budget for this query
                    max_pages = args.expand_pages if (is_expanded and getattr(args, "expand_pages", None)) else args.pages
                    if args.until_empty:
                        if (not next_cur and (not any(len(lists.get(k, [])) for k in selected_keys))) or (
                            args.max_no_progress_pages > 0 and no_prog >= args.max_no_progress_pages
                        ):
                            break
                    else:
                        if pages >= max_pages:
                            break
                    cursor = next_cur

                # If nothing surfaced for this keyword and html-fallback is enabled, run expand-from-url
                if found_any == 0 and getattr(args, "html_fallback", False):
                    try:
                        class _Obj:
                            pass
                        _a = _Obj()
                        # propagate global HTTP/env settings
                        setattr(_a, "rpm", getattr(args, "rpm", None))
                        setattr(_a, "concurrency", getattr(args, "concurrency", None))
                        setattr(_a, "timeout", getattr(args, "timeout", None))
                        setattr(_a, "base_url", getattr(args, "base_url", None))
                        setattr(_a, "headers", getattr(args, "headers", None))
                        setattr(_a, "headers_file", getattr(args, "headers_file", None))
                        # expand-from-url specific
                        setattr(_a, "urls", f"https://www.personality-database.com/search?keyword={q.replace(' ', '+')}")
                        setattr(_a, "url_file", None)
                        setattr(_a, "lists", None)
                        setattr(_a, "only_profiles", True)
                        setattr(_a, "filter_characters", getattr(args, "filter_characters", False))
                        setattr(_a, "characters_relaxed", getattr(args, "characters_relaxed", False))
                        setattr(_a, "force_character_group", getattr(args, "force_character_group", False))
                        setattr(_a, "dry_run", False)
                        setattr(_a, "set_keyword", q)
                        setattr(_a, "render_js", getattr(args, "render_js", False))
                        setattr(_a, "limit", getattr(args, "html_limit", None) or getattr(args, "limit", None) or 20)
                        setattr(_a, "html_file", None)
                        setattr(_a, "html_stdin", False)
                        setattr(_a, "log_file", None)
                        await _expand_from_url_async(_a)
                    except Exception as e:
                        _log(f"[search-keywords] html-fallback failed for '{q}': {e}")

            # Post actions
            if args.auto_embed or args.auto_index:
                try:
                    cmd_embed()
                except Exception as e:
                    print(f"Auto-embed failed: {e}")
            if args.auto_index:
                try:
                    import faiss  # type: ignore
                    import numpy as _np
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
            _log(f"Done. New: {total_new} Updated: {total_updated}")
            _log("[search-keywords] end")
            if log_fp:
                try:
                    log_fp.close()
                except Exception:
                    pass
        try:
            asyncio.run(_run())
        except Exception as e:
            print(f"search-keywords failed: {e}")
        return
    elif args.cmd == "export-characters":
        import pandas as pd
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        for _, row in df.iterrows():
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            is_char = obj.get("isCharacter") is True or obj.get("_from_character_group") is True or obj.get("_seed_sub_cat_id") is not None
            if not is_char:
                continue
            # Build candidate names and pick the most descriptive
            candidates: list[str] = []
            for k in ("name", "title", "display_name", "username", "subcategory"):
                v = obj.get(k)
                if isinstance(v, str):
                    s = v.strip()
                    if s:
                        candidates.append(s)
            # Add search keyword from seed URL as alias when present
            try:
                seed_url = obj.get("_seed_url")
                if isinstance(seed_url, str) and "search?" in seed_url:
                    from urllib.parse import urlparse as _urlparse, parse_qs as _parse_qs, unquote as _unquote
                    parsed = _urlparse(seed_url)
                    q = _parse_qs(parsed.query)
                    kws = q.get("keyword") or q.get("q") or []
                    for kw in kws:
                        if isinstance(kw, str) and kw:
                            s = _unquote(kw).strip()
                            if s and any(isinstance(x, str) and s.lower() in x.lower() for x in (obj.get("name"), obj.get("title"), obj.get("subcategory"))):
                                candidates.append(s)
                stitle = obj.get("_seed_profile_title")
                if isinstance(stitle, str) and stitle.strip():
                    candidates.append(stitle.strip())
                for kkw in ("_search_keyword", "_keyword"):
                    kwv = obj.get(kkw)
                    if isinstance(kwv, str) and kwv.strip():
                        s2 = kwv.strip()
                        if any(isinstance(x, str) and s2.lower() in x.lower() for x in (obj.get("name"), obj.get("title"), obj.get("subcategory"), stitle)):
                            candidates.append(s2)
            except Exception:
                pass
            if not candidates:
                try:
                    seed_url = obj.get("_seed_url")
                    if isinstance(seed_url, str) and "/profile/" in seed_url:
                        from urllib.parse import urlparse as _urlparse, unquote as _unquote
                        up = _urlparse(seed_url)
                        parts = [p for p in up.path.split("/") if p]
                        slug = parts[-1] if parts else None
                        if isinstance(slug, str) and slug.isdigit() and len(parts) >= 2:
                            slug = parts[-2]
                        if isinstance(slug, str) and slug:
                            s = _unquote(slug)
                            low = s.lower()
                            for tail in ("-mbti-personality-type","-personality-type","-mbti","-personality","-enneagram","-big-five","-big5","-pdb-profiles"):
                                if tail in low:
                                    idx = low.find(tail)
                                    s = s[:idx]
                                    low = s.lower()
                            s = s.replace("-", " ").strip()
                            s = " ".join([t for t in s.split() if t])
                            if s:
                                s_tc = " ".join(w.capitalize() if len(w) > 2 else w for w in s.split())
                                candidates.append(s_tc)
                except Exception:
                    pass
            try:
                import re as _re
                base_strs = candidates[:]
                for s in base_strs:
                    for pat in [r'"([^"]+)"', r'“([^”]+)”', r'‘([^’]+)’']:
                        for m in _re.finditer(pat, s):
                            al = m.group(1).strip()
                            if al:
                                candidates.append(al)
                    for m in _re.finditer(r'\(([^)]+)\)', s):
                        al = m.group(1).strip()
                        if al:
                            candidates.append(al)
                    if "/" in s:
                        for part in s.split("/"):
                            al = part.strip()
                            if al:
                                candidates.append(al)
            except Exception:
                pass
            # Deduplicate, remove known MBTI codes
            seen_c = set()
            uniq_candidates: list[str] = []
            for c in candidates:
                if not isinstance(c, str):
                    continue
                if c in seen_c:
                    continue
                seen_c.add(c)
                uniq_candidates.append(c)
            if not uniq_candidates:
                continue
            nm0 = uniq_candidates[0].strip().upper()
            if nm0 in {"INTJ","INTP","ENTJ","ENTP","INFJ","INFP","ENFJ","ENFP","ISTJ","ISFJ","ESTJ","ESFJ","ISTP","ISFP","ESTP","ESFP"}:
                continue
            rec: dict = {"cid": str(row.get("cid")), "_source": obj.get("_source")}
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            best = max(uniq_candidates, key=lambda s: len(s)) if uniq_candidates else None
            if best:
                rec["name"] = best
            rec["alt_names"] = " | ".join(uniq_candidates) if uniq_candidates else None
            rows.append(rec)
        out = _Path(getattr(args, "out", "data/bot_store/pdb_characters.parquet"))
        out.parent.mkdir(parents=True, exist_ok=True)
        df_out = pd.DataFrame(rows)
        if not df_out.empty:
            if "alt_names" not in df_out.columns:
                df_out["alt_names"] = None
            df_out = df_out.drop_duplicates(subset=["cid"], keep="last")
        df_out.to_parquet(out, index=False)
        print(f"Exported {len(rows)} character-like rows to {out}")
        if rows and getattr(args, "sample", 0):
            for r in rows[: args.sample]:
                print(f"  pid={r.get('pid')} name={r.get('name')}")
    elif args.cmd == "index-characters":
        import pandas as pd
        from pathlib import Path as _Path
        import faiss  # type: ignore
        import numpy as _np
        chars_path = _Path(getattr(args, "char_parquet", "data/bot_store/pdb_characters.parquet"))
        if not chars_path.exists():
            print(f"Missing characters parquet: {chars_path}. Run export-characters first.")
            return
        store = PdbStorage()
        df = store.load_joined()
        try:
            cdf = pd.read_parquet(chars_path)[["cid"]]
        except Exception as e:
            print(f"Failed to read characters parquet: {e}")
            return
        merged = df.merge(cdf, on="cid", how="inner").dropna(subset=["vector"]).reset_index(drop=True)
        if merged.empty:
            print("No vectors for character rows; run embed first.")
            return
        # Ensure consistent embedding dimension; filter to majority dimension
        dims = []
        for v in merged["vector"].to_list():
            try:
                dims.append(len(v))
            except Exception:
                dims.append(None)
        from collections import Counter as _Counter
        dim_counts = _Counter([d for d in dims if isinstance(d, int)])
        if not dim_counts:
            print("Vectors present but dimensions unknown; cannot index.")
            return
        target_dim, _ = max(dim_counts.items(), key=lambda kv: kv[1])
        if len(dim_counts) > 1:
            before = len(merged)
            merged = merged[[len(v) == target_dim for v in merged["vector"].to_list()]].reset_index(drop=True)
            after = len(merged)
            print(f"Note: filtered mixed embedding dims to {target_dim}-d ({after}/{before} rows kept)")
        mat = _np.vstack(merged["vector"].to_list()).astype("float32")
        norms = _np.linalg.norm(mat, axis=1, keepdims=True) + 1e-12
        mat = mat / norms
        index = faiss.IndexFlatIP(mat.shape[1])
        index.add(mat)
        outp = _Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        faiss.write_index(index, str(outp))
        cid_list = merged["cid"].astype(str).tolist()
        (outp.with_suffix(outp.suffix + ".cids")).write_text("\n".join(cid_list), encoding="utf-8")
        # Write names file aligned with cids for faster lookups in search
        name_map: dict[str, str] = {}
        # Prefer names from characters parquet when available
        char_names: dict[str, str] = {}
        try:
            cdf2 = pd.read_parquet(chars_path)
            if "cid" in cdf2.columns and "name" in cdf2.columns:
                for _, r in cdf2.iterrows():
                    scid = str(r.get("cid"))
                    nm = r.get("name")
                    if isinstance(nm, str) and nm:
                        char_names[scid] = nm
        except Exception as e:
            print(f"Note: could not read character names from {chars_path}: {e}")
        # Fallback to joined payload names for any missing entries
        for _, row in merged.iterrows():
            scid = str(row.get("cid"))
            if scid in char_names:
                name_map[scid] = char_names[scid]
                continue
            pb = row.get("payload_bytes")
            nm = None
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if isinstance(obj, dict):
                for k in ("name","title","display_name","username","subcategory"):
                    v = obj.get(k)
                    if isinstance(v, str) and v:
                        nm = v; break
            name_map[scid] = nm or "(unknown)"
        names_out = outp.with_suffix(outp.suffix + ".names")
        names_out.write_text("\n".join([name_map.get(c, "(unknown)") for c in cid_list]), encoding="utf-8")
        print(f"Wrote names for {len(cid_list)} entries to {names_out}")
        print(f"Indexed {len(merged)} character vectors to {outp}")
    elif args.cmd == "refresh-names":
        import pandas as pd
        from pathlib import Path as _Path
        idxp = _Path(getattr(args, "index", "data/bot_store/pdb_faiss_char.index"))
        map_path = idxp.with_suffix(idxp.suffix + ".cids")
        names_out = idxp.with_suffix(idxp.suffix + ".names")
        if not map_path.exists():
            print(f"Missing cid map: {map_path}. Build the index first.")
            return
        cids = [l.strip() for l in map_path.read_text(encoding="utf-8").splitlines() if l.strip()]
        # Prefer names from characters parquet
        char_names: dict[str, str] = {}
        char_path = _Path(getattr(args, "char_parquet", "data/bot_store/pdb_characters.parquet"))
        if char_path.exists():
            try:
                cdf = pd.read_parquet(char_path)
                if "cid" in cdf.columns and "name" in cdf.columns:
                    for _, r in cdf.iterrows():
                        scid = str(r.get("cid"))
                        nm = r.get("name")
                        if isinstance(nm, str) and nm:
                            char_names[scid] = nm
            except Exception as e:
                print(f"Note: could not read character names from {char_path}: {e}")
        # Fallback to joined payload names
        store = PdbStorage()
        df = store.load_joined()
        fallback_names: dict[str, str] = {}
        for _, row in df.iterrows():
            scid = str(row.get("cid"))
            if scid in fallback_names or scid in char_names:
                continue
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            nm = None
            if isinstance(obj, dict):
                for k in ("name", "title", "display_name", "username", "subcategory"):
                    v = obj.get(k)
                    if isinstance(v, str) and v:
                        nm = v
                        break
            fallback_names[scid] = nm or "(unknown)"
        lines = [(char_names.get(c) or fallback_names.get(c) or "(unknown)") for c in cids]
        names_out.write_text("\n".join(lines), encoding="utf-8")
        print(f"Wrote {len(lines)} names to {names_out}")
    elif args.cmd == "scan-seeds":
        from pathlib import Path as _Path
        import sys as _sys
        # Optional: tee stdout/stderr for the whole run so operators can inspect logs even if terminal output is truncated
        _orig_stdout = _sys.stdout
        _orig_stderr = _sys.stderr
        _lf = None
        if getattr(args, "log_file", None):
            try:
                _pth = _Path(args.log_file)
                _pth.parent.mkdir(parents=True, exist_ok=True)
                _lf = _pth.open("w", encoding="utf-8")
                class _Tee:
                    def __init__(self, a, b):
                        self.a = a; self.b = b
                    def write(self, s):
                        try:
                            self.a.write(s)
                        except Exception:
                            pass
                        try:
                            self.b.write(s)
                        except Exception:
                            pass
                    def flush(self):
                        try:
                            self.a.flush()
                        except Exception:
                            pass
                        try:
                            self.b.flush()
                        except Exception:
                            pass
                _sys.stdout = _Tee(_orig_stdout, _lf)
                _sys.stderr = _Tee(_orig_stderr, _lf)
            except Exception:
                _lf = None
        try:
            # Ensure logs are not empty even if early failures occur
            try:
                print("scan-seeds: start", flush=True)
            except Exception:
                pass
            # Build keywords from --keywords and/or --keywords-file
            kw: list[str] = []
            try:
                kraw = getattr(args, "keywords", None)
                if isinstance(kraw, str) and kraw.strip():
                    kw.extend([k.strip() for k in kraw.split(",") if k.strip()])
            except Exception:
                pass
            try:
                kfile = getattr(args, "keywords_file", None)
                if isinstance(kfile, str) and kfile.strip():
                    import re as _re, sys as _sys
                    if kfile.strip() == "-":
                        src = _sys.stdin.read()
                    else:
                        src = _Path(kfile).read_text(encoding="utf-8")
                    parts = [p.strip() for p in _re.split(r"[\s,]+", src or "") if p.strip()]
                    kw.extend(parts)
            except Exception:
                pass
            # De-duplicate while preserving order
            _seen_kw = set()
            _uniq: list[str] = []
            for _k in kw:
                if _k not in _seen_kw:
                    _seen_kw.add(_k)
                    _uniq.append(_k)
            kw = _uniq
            if not kw:
                print("No keywords provided.")
                return
            # Apply --max-seeds limit when provided
            try:
                _mx = int(getattr(args, "max_seeds", 0) or 0)
                if _mx > 0:
                    kw = kw[:_mx]
            except Exception:
                pass
            # Basic run header
            # Force-render by default in scan-seeds to improve recall on dynamic pages
            try:
                _render = True
            except Exception:
                _render = True
            try:
                print(
                    f"scan-seeds: seeds={len(kw)} pages={getattr(args, 'pages', 2)} limit={getattr(args, 'limit', 40)} render={_render} validate={bool(getattr(args, 'validate', False))}",
                    flush=True,
                )
            except Exception:
                pass
            # v2 keyword search across all seeds (with HTML fallback if v2 is sparse)
            try:
                headers_file = getattr(args, "headers_file", None)
                argv = ["pdb_cli.py"]
                # Important: pass global flags before the subcommand so nested parsing picks them up
                if headers_file:
                    argv += ["--headers-file", headers_file]
                argv += [
                    "search-keywords", "--keywords", ",".join(kw),
                    "--only-profiles", "--filter-characters", "--characters-relaxed",
                    "--expand-subcategories", "--force-character-group",
                    "--pages", str(getattr(args, "pages", 2)),
                    "--limit", str(getattr(args, "limit", 40)),
                    "--until-empty",
                    "--html-fallback",
                ]
                # Always enable JS rendering for search-keywords phase
                argv += ["--render-js"]
                # Add a top-level log file for search-keywords phase so we can inspect output post-run
                try:
                    argv += ["--log-file", "data/bot_store/scan_seeds_search.log"]
                except Exception:
                    pass
                # Pass-through query expansion knobs when provided
                if getattr(args, "append_terms", None):
                    argv += ["--append-terms", getattr(args, "append_terms")]
                if getattr(args, "sweep_alnum", False):
                    argv += ["--expand-characters", "--expand-pages", str(getattr(args, "sweep_pages", 1))]
                # Reuse main by invoking a sub-run
                try:
                    import sys as _sys
                    try:
                        print("scan-seeds: invoking search-keywords phase…")
                    except Exception:
                        pass
                    saved = list(_sys.argv)
                    _sys.argv = argv
                    main()
                finally:
                    _sys.argv = saved
            except Exception as e:
                print(f"scan-seeds: v2 keyword search failed: {e}")
            # HTML expand-from-url per seed
            for k in kw:
                try:
                    print(f"scan-seeds: expanding from search page for '{k}'…")
                except Exception:
                    pass
                url = f"https://www.personality-database.com/search?keyword={k.replace(' ', '+')}"
                argv = ["pdb_cli.py"]
                if headers_file:
                    argv += ["--headers-file", headers_file]
                argv += [
                    "expand-from-url", "--urls", url,
                    "--only-profiles", "--filter-characters", "--characters-relaxed", "--force-character-group",
                ]
                # Always enable JS rendering for expand-from-url per seed
                argv += ["--render-js"]
                # Tag with keyword for later alias enrichment
                argv += ["--set-keyword", k]
                # Per-key log file to inspect rendering/scrape behavior
                try:
                    _slug = "".join(ch if ch.isalnum() else "_" for ch in k.lower())
                    if len(_slug) > 60:
                        _slug = _slug[:60]
                    argv += ["--log-file", f"data/bot_store/expand_{_slug}.log"]
                except Exception:
                    pass
                try:
                    import sys as _sys
                    saved = list(_sys.argv)
                    _sys.argv = argv
                    main()
                except Exception as e:
                    print(f"scan-seeds: expand-from-url failed for {k}: {e}")
                finally:
                    _sys.argv = saved
            # Export characters
            try:
                import sys as _sys
                saved = list(_sys.argv)
                _sys.argv = ["pdb_cli.py", "export-characters"]
                main()
            except Exception as e:
                print(f"scan-seeds: export-characters failed: {e}")
            finally:
                _sys.argv = saved
            # Embed characters with aliases/context
            try:
                import sys as _sys
                saved = list(_sys.argv)
                _sys.argv = ["pdb_cli.py", "embed", "--chars-only", "--force", "--include-aliases", "--include-context"]
                main()
            except Exception as e:
                print(f"scan-seeds: embed failed: {e}")
            finally:
                _sys.argv = saved
            # Index & refresh names
            try:
                import sys as _sys
                saved = list(_sys.argv)
                _sys.argv = ["pdb_cli.py", "index-characters", "--out", getattr(args, "index", "data/bot_store/pdb_faiss_char.index")]
                main()
            except Exception as e:
                print(f"scan-seeds: index-characters failed: {e}")
            finally:
                _sys.argv = saved
            try:
                import sys as _sys
                saved = list(_sys.argv)
                _sys.argv = ["pdb_cli.py", "refresh-names", "--index", getattr(args, "index", "data/bot_store/pdb_faiss_char.index")]
                main()
            except Exception as e:
                print(f"scan-seeds: refresh-names failed: {e}")
            finally:
                _sys.argv = saved
            # Optional validation
            if getattr(args, "validate", False):
                try:
                    import sys as _sys
                    for k in kw:
                        saved = list(_sys.argv)
                        _sys.argv = [
                            "pdb_cli.py", "search-characters", "--index", getattr(args, "index", "data/bot_store/pdb_faiss_char.index"),
                            "--top", str(getattr(args, "validate_top", 10)), k,
                        ]
                        main()
                        _sys.argv = saved
                except Exception as e:
                    print(f"scan-seeds: validation failed: {e}")
        finally:
            # Restore stdout and close log if used
            try:
                _sys.stdout = _orig_stdout
                _sys.stderr = _orig_stderr
                if _lf:
                    _lf.close()
            except Exception:
                pass
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
    elif args.cmd == "export":
        import pandas as pd
        from pathlib import Path
        store = PdbStorage()
        raw_path = store.raw_path
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        return
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
    elif args.cmd == "cleanup":
        print("Starting parquet database cleanup...")
        
        # Clean main storage
        store = PdbStorage()
        results = store.cleanup_storage(dry_run=args.dry_run)
        
        print(f"\n=== Main Storage Cleanup ===")
        for file_type, file_results in results.items():
            if 'error' in file_results:
                print(f"{file_type}: {file_results['error']}")
                continue
                
            print(f"{file_type}:")
            print(f"  Original count: {file_results.get('original_count', 0)}")
            print(f"  Duplicates removed: {file_results.get('duplicates', 0)}")
            print(f"  Empty rows removed: {file_results.get('empty', 0)}")
            print(f"  Invalid entries removed: {file_results.get('invalid', 0)}")
            print(f"  Final count: {file_results.get('final_count', 0)}")
        
        # Clean edges if requested
        if args.edges:
            try:
                from .pdb_edges import PdbEdgesStorage
                edges_store = PdbEdgesStorage()
                edges_results = edges_store.cleanup_edges(dry_run=args.dry_run)
                
                print(f"\n=== Edges Storage Cleanup ===")
                if 'error' in edges_results:
                    print(f"Error: {edges_results['error']}")
                else:
                    print(f"Original count: {edges_results.get('original_count', 0)}")
                    print(f"Duplicates removed: {edges_results.get('duplicates', 0)}")
                    print(f"Empty rows removed: {edges_results.get('empty', 0)}")
                    print(f"Invalid entries removed: {edges_results.get('invalid', 0)}")
                    print(f"Final count: {edges_results.get('final_count', 0)}")
            except Exception as e:
                print(f"\nEdges cleanup failed: {e}")
        
        # Summary
        total_removed = 0
        for file_type, file_results in results.items():
            if 'error' not in file_results:
                removed = file_results.get('duplicates', 0) + file_results.get('empty', 0) + file_results.get('invalid', 0)
                total_removed += removed
        
        if args.dry_run:
            print(f"\n[DRY RUN] Would remove {total_removed} rows total")
            print("Use --dry-run=false to perform actual cleanup")
        else:
            print(f"\nCleanup completed! Removed {total_removed} rows total")
            if total_removed > 0:
                print("Backup files were created with .cleanup_backup extension")
    elif args.cmd == "hot-queries":
        async def _run():
            client = _make_client(args)
            store = PdbStorage()
            try:
                data = await client.fetch_json("search/hot_queries")
            except Exception as e:
                print(f"hot-queries failed: {e}")
                return
            payload = data.get("data") if isinstance(data, dict) else data
            items = []
            if isinstance(payload, dict):
                items = payload.get("queries") or payload.get("hotQueries") or payload.get("results") or []
            elif isinstance(payload, list):
                items = payload
            batch = []
            for it in items or []:
                if isinstance(it, dict):
                    batch.append({**it, "_source": "v2_hot_queries"})
                elif isinstance(it, str):
                    batch.append({"keyword": it, "_source": "v2_hot_queries"})
            if not batch:
                print("No hot queries found.")
                return
            if getattr(args, "verbose", False):
                # print first 10 keywords if possible
                names = []
                for it in batch[:10]:
                    if isinstance(it, dict):
                        names.append(str(it.get("keyword") or it.get("key") or it.get("query") or "(unknown)"))
                if names:
                    print("Hot queries sample:", ", ".join(names))
            if getattr(args, "dry_run", False):
                print(f"Would upsert {len(batch)} hot queries (dry-run)")
            else:
                n, u = store.upsert_raw(batch)
                print(f"Upserted hot queries: new={n} updated={u}")
        asyncio.run(_run())
        return
    elif args.cmd == "tag-keyword":
        import pandas as pd
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        if df.empty:
            print("No rows in raw parquet.")
            return
        kw = getattr(args, "keyword", "").strip()
        if not kw:
            print("Keyword is required")
            return
        srcs = {s.strip() for s in str(getattr(args, "sources", "")).split(",") if s.strip()}
        if not srcs:
            srcs = set()
        seed_pids = set()
        if getattr(args, "seed_pids", None):
            for tok in str(args.seed_pids).split(","):
                t = tok.strip()
                if t.isdigit():
                    seed_pids.add(int(t))
        subcats = set()
        if getattr(args, "subcat_ids", None):
            for tok in str(args.subcat_ids).split(","):
                t = tok.strip()
                if t.isdigit():
                    subcats.add(int(t))
        to_update: list[dict] = []
        matched = 0
        updated = 0
        for _, row in df.iterrows():
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            s = obj.get("_source")
            if srcs and s not in srcs:
                continue
            ok_seed = True
            if seed_pids:
                sp = obj.get("_seed_pid")
                try:
                    spv = int(sp)
                except Exception:
                    spv = None
                ok_seed = spv in seed_pids if spv is not None else False
            ok_sub = True
            if subcats:
                sc = obj.get("_seed_sub_cat_id")
                try:
                    scv = int(sc)
                except Exception:
                    scv = None
                ok_sub = scv in subcats if scv is not None else False
            if not (ok_seed and ok_sub):
                continue
            matched += 1
            prev_kw = obj.get("_search_keyword")
            if isinstance(prev_kw, str) and prev_kw.strip() == kw:
                continue
            obj = {**obj, "_search_keyword": kw}
            to_update.append(obj)
        if not to_update:
            print(f"No rows matched for update (matched={matched}, to_update=0)")
            return
        if getattr(args, "dry_run", False):
            print(f"Would update {len(to_update)} rows (matched={matched}) with _search_keyword='{kw}'")
            return
        n, u = store.upsert_raw(to_update)
        updated = u + n
        print(f"Updated rows: {updated} (matched={matched})")
        return
    elif args.cmd == "find-subcats":
        async def _run():
            client = _make_client(args)
            log_fp = None
            def _log(msg: str) -> None:
                try:
                    print(msg)
                except Exception:
                    pass
                try:
                    if log_fp:
                        log_fp.write(msg + "\n"); log_fp.flush()
                except Exception:
                    pass
            if getattr(args, "log_file", None):
                try:
                    from pathlib import Path as _Path
                    lf = _Path(args.log_file)
                    lf.parent.mkdir(parents=True, exist_ok=True)
                    log_fp = lf.open("w", encoding="utf-8")
                except Exception:
                    log_fp = None
            _log("[find-subcats] start")
            q = getattr(args, "keyword", "") or ""
            cursor = 0
            pages = 0
            total = 0
            seen_ids: set[int] = set()
            out_ids: list[int] = []
            while True:
                params = {"limit": args.limit, "keyword": q}
                if cursor:
                    params["nextCursor"] = cursor
                try:
                    data = await client.fetch_json("search/top", params)
                except Exception as e:
                    _log(f"find-subcats failed: {e}")
                    break
                payload = data.get("data") if isinstance(data, dict) else data
                if not isinstance(payload, dict):
                    _log("Unexpected payload shape from search/top")
                    break
                # collect and print subcategories
                subcats = payload.get("subcategories") or []
                count_this = 0
                for sc in subcats:
                    if not isinstance(sc, dict):
                        continue
                    sid = None
                    for kk in ("id", "profileId", "profileID", "profile_id"):
                        v = sc.get(kk)
                        if isinstance(v, int):
                            sid = v; break
                        if isinstance(v, str) and v.isdigit():
                            sid = int(v); break
                    if not isinstance(sid, int) or sid in seen_ids:
                        continue
                    seen_ids.add(sid)
                    nm = sc.get("name") or sc.get("title") or sc.get("subcategory") or "(unnamed)"
                    is_group = sc.get("isCharacterGroup") is True
                    is_char = sc.get("isCharacter") is True
                    _log(f"id={sid} | name={nm} | isCharacterGroup={is_group} isCharacter={is_char}")
                    total += 1
                    count_this += 1
                    out_ids.append(sid)
                # next cursor
                next_cur = 0
                for kc in ("nextCursor", "nextcursor", "next_cursor"):
                    v = payload.get(kc)
                    if isinstance(v, int):
                        next_cur = v; break
                    if isinstance(v, str) and v.isdigit():
                        next_cur = int(v); break
                pages += 1
                # decide paging
                if args.until_empty:
                    if (not next_cur and not subcats) or (args.pages and pages >= args.pages):
                        break
                else:
                    if pages >= args.pages:
                        break
                cursor = next_cur
            if total == 0:
                _log("No subcategories found.")
            # Optionally write IDs to file
            try:
                if getattr(args, "ids_out", None):
                    from pathlib import Path as _Path
                    pth = _Path(args.ids_out)
                    pth.parent.mkdir(parents=True, exist_ok=True)
                    pth.write_text("\n".join(str(x) for x in out_ids), encoding="utf-8")
                    _log(f"Wrote {len(out_ids)} subcategory IDs to {pth}")
            except Exception:
                pass
            _log("[find-subcats] end")
            if log_fp:
                try:
                    log_fp.close()
                except Exception:
                    pass
        asyncio.run(_run())
        return
    elif args.cmd == "auth-check":
        async def _run():
            client = _make_client(args)
            log_fp = None
            def _log(msg: str) -> None:
                try:
                    print(msg)
                except Exception:
                    pass
                try:
                    if log_fp:
                        log_fp.write(msg + "\n"); log_fp.flush()
                except Exception:
                    pass
            if getattr(args, "log_file", None):
                try:
                    from pathlib import Path as _Path
                    lf = _Path(args.log_file)
                    lf.parent.mkdir(parents=True, exist_ok=True)
                    log_fp = lf.open("w", encoding="utf-8")
                except Exception:
                    log_fp = None
            _log("[auth-check] start")
            q = getattr(args, "keyword", "harry potter")
            cursor = 0
            pages = 0
            rich = False
            surfaced_chars = 0
            while True:
                params = {"limit": args.limit, "keyword": q}
                if cursor:
                    params["nextCursor"] = cursor
                try:
                    data = await client.fetch_json("search/top", params)
                except Exception as e:
                    _log(f"auth-check request failed: {e}")
                    return
                payload = data.get("data") if isinstance(data, dict) else data
                if not isinstance(payload, dict):
                    _log("Unexpected payload shape from search/top (no data dict)")
                    return
                keys = [k for k, v in payload.items() if isinstance(v, list)]
                profs = payload.get("profiles") or []
                subcats = payload.get("subcategories") or []
                boards = payload.get("boards") or []
                recs = payload.get("recommendProfiles") or []
                if recs:
                    profs = (profs or []) + recs
                # determine richness
                rich = bool(subcats or boards or recs)
                # count character flags
                for it in profs:
                    if isinstance(it, dict) and it.get("isCharacter") is True:
                        surfaced_chars += 1
                key_counts = ", ".join(f"{k}:{len(payload.get(k, []) or [])}" for k in sorted(keys))
                _log(f"page={pages+1} keys={{ {key_counts} }} chars_in_profiles={surfaced_chars}")
                # advance page
                next_cur = 0
                for kc in ("nextCursor", "nextcursor", "next_cursor"):
                    v = payload.get(kc)
                    if isinstance(v, int):
                        next_cur = v; break
                    if isinstance(v, str) and v.isdigit():
                        next_cur = int(v); break
                pages += 1
                if pages >= args.pages or not next_cur:
                    break
                cursor = next_cur
            if rich or surfaced_chars > 0:
                _log("Result: OK — headers likely authenticated; rich lists/characters surfaced.")
            else:
                _log("Result: Limited — likely missing auth/cookie. Add --headers-file with browser headers.")
            _log("[auth-check] end")
            if log_fp:
                try:
                    log_fp.close()
                except Exception:
                    pass
        asyncio.run(_run())
        return
    elif args.cmd == "expand-related":
        import re as _re
        from pathlib import Path as _Path
        async def _run():
            client = _make_client(args)
            # Prepare optional meta-client for richer related data when available
            meta_client = None
            try:
                from .pdb_client import PdbClient as _PdbClient
                # Reuse current client's settings/headers if possible
                # Fallback to a known meta base URL
                meta_client = _PdbClient(
                    base_url="https://meta.personality-database.com/api/v2",
                    concurrency=getattr(client, "concurrency", 4),
                    rate_per_minute=getattr(client, "rate_per_minute", 60),
                    timeout_s=getattr(client, "timeout_s", 20.0),
                    headers=getattr(client, "_extra_headers", None),
                )
            except Exception:
                meta_client = None
            store = PdbStorage()
            # collect seed IDs
            seeds: list[int] = []
            if getattr(args, "ids", None):
                for tok in str(args.ids).split(","):
                    tok = tok.strip()
                    if tok.isdigit():
                        seeds.append(int(tok))
            if getattr(args, "id_file", None):
                try:
                    txt = _Path(args.id_file).read_text(encoding="utf-8")
                    for tok in _re.split(r"[\s,]+", txt):
                        tok = tok.strip()
                        if tok.isdigit():
                            seeds.append(int(tok))
                except Exception:
                    pass
            # fallback: try a default seeds file if present
            if not seeds:
                df_path = _Path("data/bot_store/character_seeds.txt")
                if df_path.exists():
                    try:
                        txt = df_path.read_text(encoding="utf-8")
                        for tok in _re.split(r"[\s,]+", txt):
                            tok = tok.strip()
                            if tok.isdigit():
                                seeds.append(int(tok))
                    except Exception:
                        pass
            # de-duplicate, preserve order
            seen = set()
            uniq: list[int] = []
            for s in seeds:
                if s not in seen:
                    seen.add(s)
                    uniq.append(s)
            if args.max_ids and args.max_ids > 0:
                uniq = uniq[: args.max_ids]
            if not uniq:
                print("No seed IDs provided via --ids/--id-file and no default seeds file found.")
                return
            # lists filter
            target_lists: set[str] | None = None
            if args.only_profiles:
                target_lists = {"profiles"}
            elif isinstance(args.lists, str) and args.lists:
                target_lists = {s.strip() for s in args.lists.split(",") if s.strip()}
            total = 0
            forced_kw = getattr(args, "set_keyword", None)
            for sid in uniq:
                lists: dict[str, list] = {}
                # Helper to merge list-like fields into canonical keys
                def _merge_lists_from_payload(pl: dict) -> None:
                    if not isinstance(pl, dict):
                        return
                    for k in (
                        "relatedProfiles",
                        "profiles",
                        "characters",
                        "recommendProfiles",
                        "boards",
                        "subcategories",
                    ):
                        v = pl.get(k)
                        if isinstance(v, list) and v:
                            key = "profiles" if k in {"relatedProfiles", "characters", "recommendProfiles"} else k
                            base = lists.get(key) or []
                            lists[key] = base + v
                # 1) Try standard v2 related endpoint first
                try:
                    data = await client.fetch_json(f"profiles/{sid}/related")
                    payload = data.get("data") if isinstance(data, dict) else data
                    if isinstance(payload, dict):
                        _merge_lists_from_payload(payload)
                except Exception as e:
                    print(f"related fetch failed for id={sid}: {e}")
                # 2) Augment via meta endpoint if available
                if meta_client is not None:
                    try:
                        mdata = await meta_client.fetch_json(f"meta/profile/{sid}")
                        mpayload = mdata.get("data") if isinstance(mdata, dict) else mdata
                        if isinstance(mpayload, dict):
                            _merge_lists_from_payload(mpayload)
                            # As a fallback, recursively scan for profile-like dicts
                            prof_like: list[dict] = []
                            def _walk(x):
                                try:
                                    if isinstance(x, dict):
                                        # Heuristic: looks like a profile if it has a name/title and an id-ish field
                                        has_name = any(isinstance(x.get(k), str) and x.get(k) for k in ("name","title","subcategory","display_name","username"))
                                        id_val = None
                                        for kk in ("id","profileId","profile_id","profileID"):
                                            v = x.get(kk)
                                            if isinstance(v, int):
                                                id_val = v; break
                                            if isinstance(v, str) and v.isdigit():
                                                id_val = int(v); break
                                        is_char_flag = x.get("isCharacter") is True
                                        if (has_name and id_val is not None) or is_char_flag:
                                            prof_like.append(x)
                                        for v in x.values():
                                            _walk(v)
                                    elif isinstance(x, list):
                                        for v in x:
                                            _walk(v)
                                except Exception:
                                    return
                            _walk(mpayload)
                            if prof_like:
                                base = lists.get("profiles") or []
                                lists["profiles"] = base + prof_like
                    except Exception:
                        pass
                selected = set(lists.keys()) if target_lists is None else (set(lists.keys()) & set(target_lists))
                batch: list[dict] = []
                for key in sorted(selected):
                    for it in lists.get(key, []) or []:
                        if not isinstance(it, dict):
                            continue
                        obj = {**it, "_source": f"v2_related:{key}", "_seed_id": sid, "_seed_sub_cat_id": sid}
                        if isinstance(forced_kw, str) and forced_kw.strip():
                            obj["_search_keyword"] = forced_kw.strip()
                        if args.force_character_group:
                            try:
                                # only set when absent to avoid overwriting true value
                                obj.setdefault("_from_character_group", True)
                            except Exception:
                                pass
                        if args.filter_characters:
                            is_char = obj.get("isCharacter") is True
                            if not is_char and args.characters_relaxed:
                                is_char = obj.get("_from_character_group") is True
                            if not is_char:
                                continue
                        batch.append(obj)
                if not batch:
                    print(f"id={sid}: no items after filtering.")
                    continue
                if args.dry_run:
                    print(f"id={sid}: would upsert {len(batch)} items (dry-run)")
                else:
                    n, u = store.upsert_raw(batch)
                    total += (n + u)
                    print(f"id={sid}: upserted new={n} updated={u}")
            if not args.dry_run:
                print(f"Done. Upserted total rows: {total}")
        asyncio.run(_run())
        return
    elif args.cmd == "expand-from-url":
        asyncio.run(_expand_from_url_async(args))
        return
    elif args.cmd == "peek-meta":
        import re as _re
        async def _run():
            base_client = _make_client(args)
            # Build meta client reusing headers/limits
            meta_client = None
            try:
                from .pdb_client import PdbClient as _PdbClient
                meta_client = _PdbClient(
                    base_url="https://meta.personality-database.com/api/v2",
                    concurrency=getattr(base_client, "concurrency", 4),
                    rate_per_minute=getattr(base_client, "rate_per_minute", 60),
                    timeout_s=getattr(base_client, "timeout_s", 20.0),
                    headers=getattr(base_client, "_extra_headers", None),
                )
            except Exception as e:
                print(f"Failed to build meta client: {e}")
                return
            # Collect IDs
            ids: list[int] = []
            if getattr(args, "id", None):
                try:
                    ids.append(int(args.id))
                except Exception:
                    pass
            if getattr(args, "ids", None):
                for tok in str(args.ids).split(","):
                    tok = tok.strip()
                    if tok.isdigit():
                        ids.append(int(tok))
            # De-duplicate
            seen = set()
            uniq: list[int] = []
            for i in ids:
                if i not in seen:
                    seen.add(i)
                    uniq.append(i)
            if not uniq:
                print("No IDs provided. Use --id or --ids.")
                return
            def _pick_name(obj: dict) -> str:
                for k in ("name", "title", "display_name", "username", "subcategory"):
                    v = obj.get(k)
                    if isinstance(v, str) and v:
                        return v
                return "(unknown)"
            for pid in uniq:
                try:
                    data = await meta_client.fetch_json(f"meta/profile/{pid}")
                except Exception as e:
                    print(f"id={pid}: meta fetch failed: {e}")
                    continue
                payload = data.get("data") if isinstance(data, dict) else data
                if not isinstance(payload, dict):
                    print(f"id={pid}: unexpected payload shape")
                    continue
                # Summarize list keys and counts
                list_keys = {k: v for k, v in payload.items() if isinstance(v, list)}
                if not list_keys:
                    print(f"id={pid}: no list keys found in meta payload")
                else:
                    key_counts = ", ".join(f"{k}:{len(v or [])}" for k, v in sorted(list_keys.items()))
                    print(f"id={pid}: keys={{ {key_counts} }}")
                    # Print sample names from recognized profile-like lists
                    samples: list[str] = []
                    for k in ("profiles", "relatedProfiles", "characters", "recommendProfiles"):
                        arr = list_keys.get(k) or []
                        for it in arr[: max(int(getattr(args, "sample", 0)), 0)]:
                            if isinstance(it, dict):
                                samples.append(_pick_name(it))
                        if samples:
                            break
                    if samples:
                        more = max((sum(len(list_keys.get(k) or []) for k in list_keys) - len(samples)), 0)
                        tail = f" (+{more} more)" if more else ""
                        print("  "+", ".join(samples) + tail)
                if getattr(args, "raw", False):
                    try:
                        print(json.dumps(payload, ensure_ascii=False, indent=2))
                    except Exception:
                        try:
                            import pprint as _pp
                            _pp.pprint(payload)
                        except Exception:
                            pass
        asyncio.run(_run())
        return
    elif args.cmd == "search-top":
        import sys as _sys
        from pathlib import Path as _Path
        def _pick_name(obj: dict) -> str:
            for k in ("name", "title", "display_name", "username", "subcategory"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    return v
            return "(unknown)"
        async def _run():
            client = _make_client(args)
            store = PdbStorage()
            q = args.keyword if getattr(args, "keyword", None) else getattr(args, "query", "")
            if getattr(args, "encoded", False) and isinstance(q, str) and q:
                try:
                    from urllib.parse import unquote
                    q = unquote(q)
                except Exception:
                    pass
            if not isinstance(q, str):
                q = str(q)
            target_lists: Optional[set[str]] = None
            if args.only_profiles:
                target_lists = {"profiles"}
            elif isinstance(args.lists, str) and args.lists:
                target_lists = {s.strip() for s in args.lists.split(",") if s.strip()}
            total_new = total_updated = 0
            cursor = args.next_cursor
            no_prog = 0
            pages = 0
            if args.verbose:
                print(f"[debug] search-top start q='{q}' limit={args.limit} only_profiles={args.only_profiles}")
            while True:
                params = {"limit": args.limit, "keyword": q}
                if cursor:
                    params["nextCursor"] = cursor
                try:
                    data = await client.fetch_json("search/top", params)
                except Exception as e:
                    print(f"search/top failed: {e}")
                    break
                block = data if isinstance(data, dict) else {}
                payload = block.get("data") if isinstance(block.get("data"), dict) else block
                next_cur = 0
                if isinstance(payload, dict):
                    for kc in ("nextCursor", "nextcursor", "next_cursor"):
                        v = payload.get(kc)
                        if isinstance(v, int):
                            next_cur = v; break
                        if isinstance(v, str) and v.isdigit():
                            next_cur = int(v); break
                lists: dict[str, list] = {}
                if isinstance(payload, dict):
                    for k, v in payload.items():
                        if k in {"nextCursor", "keyword", "query"}:
                            continue
                        if isinstance(v, list):
                            lists[k] = v
                    # Map recommendProfiles into profiles so --only-profiles retains them
                    if "recommendProfiles" in lists:
                        recs = lists.get("recommendProfiles") or []
                        if recs:
                            base = lists.get("profiles") or []
                            lists["profiles"] = (base + recs)
                    # Also map alt keys, preserving provenance for relaxed character filtering
                    for alt_key in ("characters", "relatedProfiles"):
                        if alt_key in lists:
                            alt = lists.get(alt_key) or []
                            if alt:
                                marked = [({**it, "_from_character_group": True} if isinstance(it, dict) else it) for it in alt]
                                base = lists.get("profiles") or []
                                lists["profiles"] = (base + marked)
                if args.verbose:
                    key_counts = ", ".join(f"{k}:{len(lists.get(k, []) or [])}" for k in sorted(lists.keys()))
                    print(f"[debug] page={pages+1} keys={{ {key_counts} }} nextCursor={next_cur}")
                selected_keys = set(lists.keys()) if target_lists is None else (set(lists.keys()) & set(target_lists))
                expanded_profiles: list[dict] = []
                if args.expand_subcategories and "subcategories" in lists:
                    subcats = lists.get("subcategories", [])[: max(int(args.expand_max), 0)]
                    for sc in subcats:
                        sid = None
                        if isinstance(sc, dict):
                            for key in ("id", "profileId", "profile_id", "profileID"):
                                v = sc.get(key)
                                if isinstance(v, int): sid = v; break
                                if isinstance(v, str) and v.isdigit(): sid = int(v); break
                        if not isinstance(sid, int):
                            continue
                        try:
                            rel = await client.fetch_json(f"profiles/{sid}/related")
                        except Exception:
                            continue
                        rel_payload = rel.get("data") if isinstance(rel, dict) else rel
                        rel_list = []
                        if isinstance(rel_payload, dict):
                            rel_list = rel_payload.get("relatedProfiles") or rel_payload.get("profiles") or []
                        if not isinstance(rel_list, list):
                            continue
                        for it in rel_list:
                            if isinstance(it, dict):
                                expanded_profiles.append({**it, "_source": "v2_related_from_subcategory", "_keyword": q, "_from_character_group": True if args.force_character_group else it.get("_from_character_group") or False})
                # Optional: expand via boards and chase payload hints
                async def _expand_by_term(term: str, tag: str) -> list[dict]:
                    try:
                        data2 = await client.fetch_json("search/top", {"limit": args.limit, "keyword": term})
                    except Exception:
                        return []
                    block2 = data2 if isinstance(data2, dict) else {}
                    payload2 = block2.get("data") if isinstance(block2.get("data"), dict) else block2
                    lists2: dict[str, list] = {}
                    if isinstance(payload2, dict):
                        for kk, vv in payload2.items():
                            if kk in {"nextCursor", "keyword", "query"}:
                                continue
                            if isinstance(vv, list):
                                lists2[kk] = vv
                        if "recommendProfiles" in lists2:
                            recs2 = lists2.get("recommendProfiles") or []
                            if recs2:
                                base2 = lists2.get("profiles") or []
                                lists2["profiles"] = (base2 + recs2)
                        for alt_key in ("characters", "relatedProfiles"):
                            if alt_key in lists2:
                                alt2 = lists2.get(alt_key) or []
                                if alt2:
                                    marked2 = [({**it, "_from_character_group": True} if isinstance(it, dict) else it) for it in alt2]
                                    base2 = lists2.get("profiles") or []
                                    lists2["profiles"] = (base2 + marked2)
                    out: list[dict] = []
                    for ksel in sorted(selected_keys):
                        for it2 in lists2.get(ksel, []) or []:
                            if not isinstance(it2, dict):
                                continue
                            obj2 = {**it2, "_source": f"v2_search_top_by_{tag}:{ksel}", "_keyword": q}
                            if args.filter_characters:
                                is_char2 = obj2.get("isCharacter") is True
                                if not is_char2 and args.characters_relaxed:
                                    is_char2 = obj2.get("_from_character_group") is True
                                if not is_char2:
                                    continue
                            out.append(obj2)
                    return out
                extra_items: list[dict] = []
                if getattr(args, "expand_boards", False) and "boards" in lists:
                    boards = lists.get("boards", [])[: max(int(getattr(args, "boards_max", 0)), 0)]
                    for b in boards:
                        if not isinstance(b, dict):
                            continue
                        bname = None
                        for k in ("name", "title", "display_name", "username", "subcategory"):
                            v = b.get(k)
                            if isinstance(v, str) and v:
                                bname = v; break
                        if not bname:
                            continue
                        extra_items.extend(await _expand_by_term(bname, "board"))
                if getattr(args, "chase_hints", False) and isinstance(payload, dict):
                    raw_hints = []
                    for hk in ("hint", "hints", "suggestions", "suggested", "suggestedKeywords", "suggested_terms"):
                        hv = payload.get(hk)
                        if hv is None:
                            continue
                        if isinstance(hv, str):
                            raw_hints.append(hv)
                        elif isinstance(hv, list):
                            for x in hv:
                                if isinstance(x, str):
                                    raw_hints.append(x)
                                elif isinstance(x, dict):
                                    for nk in ("name", "title", "keyword", "key", "query", "text", "value"):
                                        nv = x.get(nk)
                                        if isinstance(nv, str):
                                            raw_hints.append(nv); break
                    seen_h = set()
                    hints = []
                    for h in raw_hints:
                        if not isinstance(h, str):
                            continue
                        hh = h.strip()
                        if not hh or hh == q or hh in seen_h:
                            continue
                        seen_h.add(hh)
                        hints.append(hh)
                    hints = hints[: max(int(getattr(args, "hints_max", 0)), 0)]
                    for h in hints:
                        extra_items.extend(await _expand_by_term(h, "hint"))
                batch: list[dict] = []
                for key in sorted(selected_keys):
                    items = lists.get(key, [])
                    src_name = f"v2_search_top:{key}"
                    for it in items:
                        if not isinstance(it, dict):
                            continue
                        obj = {**it, "_source": src_name, "_keyword": q}
                        if args.filter_characters:
                            is_char = obj.get("isCharacter") is True
                            if not is_char and args.characters_relaxed:
                                is_char = obj.get("_from_character_group") is True
                            if not is_char:
                                continue
                        batch.append(obj)
                if extra_items:
                    batch.extend(extra_items)
                if expanded_profiles:
                    for obj in expanded_profiles:
                        if args.filter_characters:
                            is_char = obj.get("isCharacter") is True or obj.get("_from_character_group") is True if args.characters_relaxed else obj.get("isCharacter") is True
                            if not is_char:
                                continue
                        batch.append(obj)
                if args.verbose:
                    names = ", ".join(_pick_name(x) for x in batch[:10])
                    more = max(len(batch) - 10, 0)
                    tail = f" (+{more} more)" if more else ""
                    print(f"query='{q}' page={pages+1} items={len(batch)}{tail}")
                    if names:
                        print(f"  {names}")
                new = upd = 0
                if not args.dry_run and batch:
                    n, u = store.upsert_raw(batch)
                    new += n; upd += u
                    total_new += n; total_updated += u
                if new == 0:
                    no_prog += 1
                else:
                    no_prog = 0
                pages += 1
                if args.until_empty:
                    if (not next_cur and (not any(len(lists.get(k, [])) for k in selected_keys))) or (args.max_no_progress_pages > 0 and no_prog >= args.max_no_progress_pages):
                        break
                else:
                    if pages >= args.pages:
                        break
                cursor = next_cur
            if args.auto_embed or args.auto_index:
                try:
                    cmd_embed()
                except Exception as e:
                    print(f"Auto-embed failed: {e}")
            if args.auto_index:
                try:
                    import faiss  # type: ignore
                    import numpy as _np
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        return
    elif args.cmd == "follow-hot":
        from pathlib import Path as _Path
        def _pick_name(obj: dict) -> str:
            for k in ("name", "title", "display_name", "username", "subcategory"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    return v
            return "(unknown)"
        async def _run():
            client = _make_client(args)
            store = PdbStorage()
            try:
                hq = await client.fetch_json("search/hot_queries")
            except Exception as e:
                print(f"fetch hot-queries failed: {e}")
                return
            payload = hq.get("data") if isinstance(hq, dict) else hq
            keys: list[str] = []
            if isinstance(payload, dict):
                arr = payload.get("queries") or payload.get("hotQueries") or payload.get("results") or []
                for it in arr or []:
                    if isinstance(it, dict):
                        k = it.get("keyword") or it.get("key") or it.get("query")
                        if isinstance(k, str) and k:
                            keys.append(k)
                    elif isinstance(it, str):
                        keys.append(it)
            if not keys:
                print("No hot query keys found.")
                return
            keys = keys[: max(int(args.max_keys), 0)]
            target_lists: Optional[set[str]] = None
            if args.only_profiles:
                target_lists = {"profiles"}
            elif isinstance(args.lists, str) and args.lists:
                target_lists = {s.strip() for s in args.lists.split(",") if s.strip()}
            total_new = total_updated = 0
            for keyw in keys:
                cursor = args.next_cursor
                no_prog = 0
                pages = 0
                while True:
                    params = {"limit": args.limit, "keyword": keyw}
                    if cursor:
                        params["nextCursor"] = cursor
                    try:
                        data = await client.fetch_json("search/top", params)
                    except Exception as e:
                        print(f"search/top failed for key='{keyw}': {e}")
                        break
                    block = data if isinstance(data, dict) else {}
                    payload = block.get("data") if isinstance(block.get("data"), dict) else block
                    next_cur = 0
                    if isinstance(payload, dict):
                        for kc in ("nextCursor", "nextcursor", "next_cursor"):
                            v = payload.get(kc)
                            if isinstance(v, int): next_cur = v; break
                            if isinstance(v, str) and v.isdigit(): next_cur = int(v); break
                    lists: dict[str, list] = {}
                    if isinstance(payload, dict):
                        for k, v in payload.items():
                            if k in {"nextCursor", "keyword", "query"}: continue
                            if isinstance(v, list): lists[k] = v
                        # Map recommendProfiles into profiles so --only-profiles retains them
                        if "recommendProfiles" in lists:
                            recs = lists.get("recommendProfiles") or []
                            if recs:
                                base = lists.get("profiles") or []
                                lists["profiles"] = (base + recs)
                    selected_keys = set(lists.keys()) if target_lists is None else (set(lists.keys()) & set(target_lists))
                    expanded_profiles: list[dict] = []
                    if args.expand_subcategories and "subcategories" in lists:
                        subcats = lists.get("subcategories", [])[: max(int(args.expand_max), 0)]
                        for sc in subcats:
                            sid = None
                            if isinstance(sc, dict):
                                for kk in ("id", "profileId", "profile_id", "profileID"):
                                    vv = sc.get(kk)
                                    if isinstance(vv, int): sid = vv; break
                                    if isinstance(vv, str) and vv.isdigit(): sid = int(vv); break
                            if not isinstance(sid, int):
                                continue
                            try:
                                rel = await client.fetch_json(f"profiles/{sid}/related")
                            except Exception:
                                rel = None
                            rel_payload = rel.get("data") if isinstance(rel, dict) else rel
                            rel_list = []
                            if isinstance(rel_payload, dict):
                                rel_list = rel_payload.get("relatedProfiles") or rel_payload.get("profiles") or []
                            if not isinstance(rel_list, list):
                                continue
                            for it in rel_list:
                                if isinstance(it, dict):
                                    expanded_profiles.append({**it, "_source": "v2_related_from_subcategory", "_keyword": keyw, "_from_character_group": True if args.force_character_group else it.get("_from_character_group") or False})
                    # Optional: expand via boards and chase payload hints
                    async def _expand_by_term(term: str, tag: str) -> list[dict]:
                        try:
                            data2 = await client.fetch_json("search/top", {"limit": args.limit, "keyword": term})
                        except Exception:
                            return []
                        block2 = data2 if isinstance(data2, dict) else {}
                        payload2 = block2.get("data") if isinstance(block2.get("data"), dict) else block2
                        lists2: dict[str, list] = {}
                        if isinstance(payload2, dict):
                            for kk, vv in payload2.items():
                                if kk in {"nextCursor", "keyword", "query"}:
                                    continue
                                if isinstance(vv, list):
                                    lists2[kk] = vv
                            if "recommendProfiles" in lists2:
                                recs2 = lists2.get("recommendProfiles") or []
                                if recs2:
                                    base2 = lists2.get("profiles") or []
                                    lists2["profiles"] = (base2 + recs2)
                        out: list[dict] = []
                        for ksel in sorted(selected_keys):
                            for it2 in lists2.get(ksel, []) or []:
                                if not isinstance(it2, dict):
                                    continue
                                obj2 = {**it2, "_source": f"v2_search_top_by_{tag}:{ksel}", "_keyword": keyw}
                                if args.filter_characters:
                                    is_char2 = obj2.get("isCharacter") is True
                                    if not is_char2 and args.characters_relaxed:
                                        is_char2 = obj2.get("_from_character_group") is True
                                    if not is_char2:
                                        continue
                                out.append(obj2)
                        return out
                    extra_items: list[dict] = []
                    if getattr(args, "expand_boards", False) and "boards" in lists:
                        boards = lists.get("boards", [])[: max(int(getattr(args, "boards_max", 0)), 0)]
                        for b in boards:
                            if not isinstance(b, dict):
                                continue
                            bname = None
                            for k in ("name", "title", "display_name", "username", "subcategory"):
                                v = b.get(k)
                                if isinstance(v, str) and v:
                                    bname = v; break
                            if not bname:
                                continue
                            extra_items.extend(await _expand_by_term(bname, "board"))
                    if getattr(args, "chase_hints", False) and isinstance(payload, dict):
                        raw_hints = []
                        for hk in ("hint", "hints", "suggestions", "suggested", "suggestedKeywords", "suggested_terms"):
                            hv = payload.get(hk)
                            if hv is None:
                                continue
                            if isinstance(hv, str):
                                raw_hints.append(hv)
                            elif isinstance(hv, list):
                                for x in hv:
                                    if isinstance(x, str):
                                        raw_hints.append(x)
                                    elif isinstance(x, dict):
                                        for nk in ("name", "title", "keyword", "key", "query", "text", "value"):
                                            nv = x.get(nk)
                                            if isinstance(nv, str):
                                                raw_hints.append(nv); break
                        seen_h = set()
                        hints = []
                        for h in raw_hints:
                            if not isinstance(h, str):
                                continue
                            hh = h.strip()
                            if not hh or hh == keyw or hh in seen_h:
                                continue
                            seen_h.add(hh)
                            hints.append(hh)
                        hints = hints[: max(int(getattr(args, "hints_max", 0)), 0)]
                        for h in hints:
                            extra_items.extend(await _expand_by_term(h, "hint"))
                    batch: list[dict] = []
                    for k in sorted(selected_keys):
                        items = lists.get(k, [])
                        src_name = f"v2_search_top:{k}"
                        for it in items:
                            if not isinstance(it, dict): continue
                            obj = {**it, "_source": src_name, "_keyword": keyw}
                            if args.filter_characters:
                                is_char = obj.get("isCharacter") is True
                                if not is_char and args.characters_relaxed:
                                    is_char = obj.get("_from_character_group") is True
                                if not is_char:
                                    continue
                            batch.append(obj)
                    if extra_items:
                        batch.extend(extra_items)
                    if expanded_profiles:
                        for obj in expanded_profiles:
                            if args.filter_characters:
                                is_char = obj.get("isCharacter") is True or obj.get("_from_character_group") is True if args.characters_relaxed else obj.get("isCharacter") is True
                                if not is_char:
                                    continue
                            batch.append(obj)
                    if args.verbose:
                        names = ", ".join(_pick_name(x) for x in batch[:10])
                        more = max(len(batch) - 10, 0)
                        tail = f" (+{more} more)" if more else ""
                        print(f"key='{keyw}' page={pages+1} items={len(batch)}{tail}")
                        if names:
                            print(f"  {names}")
                    if not args.dry_run and batch:
                        n, u = store.upsert_raw(batch)
                        total_new += n; total_updated += u
                    if not batch:
                        no_prog += 1
                    else:
                        no_prog = 0
                    pages += 1
                    if args.until_empty:
                        if (not next_cur and (not any(len(lists.get(k, [])) for k in selected_keys))) or (args.max_no_progress_pages > 0 and no_prog >= args.max_no_progress_pages):
                            break
                    else:
                        if pages >= args.pages:
                            break
                    cursor = next_cur
            if args.auto_embed or args.auto_index:
                try:
                    cmd_embed()
                except Exception as e:
                    print(f"Auto-embed failed: {e}")
            if args.auto_index:
                try:
                    import faiss  # type: ignore
                    import numpy as _np
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
            print(f"Done. New: {total_new} Updated: {total_updated}")
        asyncio.run(_run())
        return
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
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        seen_ids: set[int] = set()
        have_v1: set[int] = set()
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
                v = obj.get("_profile_id")
                if isinstance(v, int):
                    have_v1.add(v)
                elif isinstance(v, str):
                    try:
                        have_v1.add(int(v))
                    except Exception:
                        pass
            if pid is None:
                for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        pid = v; break
                    if isinstance(v, str):
                        try:
                            pid = int(v); break
                        except Exception:
                            pass
            if isinstance(pid, int):
                seen_ids.add(pid)
        missing = [i for i in seen_ids if i not in have_v1]
        if args.shuffle:
            _random.shuffle(missing)
        if args.max and args.max > 0:
            missing = missing[: args.max]
        print(f"Missing v1 count: {len(missing)} (seen={len(seen_ids)}, have_v1={len(have_v1)})")
        if args.dry_run or not missing:
            return
        async def _run():
            # build v1 client
            v1_kwargs = {"base_url": args.v1_base_url}
            if getattr(args, "rpm", None) is not None:
                v1_kwargs["rate_per_minute"] = args.rpm
            if getattr(args, "concurrency", None) is not None:
                v1_kwargs["concurrency"] = args.concurrency
            if getattr(args, "timeout", None) is not None:
                v1_kwargs["timeout_s"] = args.timeout
            if args.v1_headers:
                try:
                    v1_kwargs["headers"] = _json.loads(args.v1_headers)
                except Exception:
                    v1_kwargs["headers"] = None
            from .pdb_client import PdbClient as _PdbClient
            client_v1 = _PdbClient(**v1_kwargs)
            # optional v2 fallback client
            client_v2 = None
            if args.fallback_v2:
                v2_kwargs = {"base_url": args.v2_base_url}
                if getattr(args, "rpm", None) is not None:
                    v2_kwargs["rate_per_minute"] = args.rpm
                if getattr(args, "concurrency", None) is not None:
                    v2_kwargs["concurrency"] = args.concurrency
                if getattr(args, "timeout", None) is not None:
                    v2_kwargs["timeout_s"] = args.timeout
                if args.v2_headers:
                    try:
                        v2_kwargs["headers"] = _json.loads(args.v2_headers)
                    except Exception:
                        v2_kwargs["headers"] = None
                client_v2 = _PdbClient(**v2_kwargs)
            scraped = 0
            for pid in missing:
                try:
                    data = await client_v1.get_profile(pid)
                except Exception as e:
                    print(f"v1 get_profile failed for id={pid}: {e}")
                    # Try v2 fallback if enabled
                    if client_v2 is not None:
                        try:
                            v2data = await client_v2.fetch_json(f"profiles/{pid}")
                        except Exception as e2:
                            print(f"v2 fallback failed for id={pid}: {e2}")
                            continue
                        obj = None
                        if isinstance(v2data, dict):
                            obj = v2data.get("data") if isinstance(v2data.get("data"), dict) else v2data
                        if not isinstance(obj, dict):
                            print(f"v2 fallback unexpected shape for id={pid}")
                            continue
                        main_obj = {**obj, "_source": "v2_profile", "_profile_id": pid, "_fallback": "v2"}
                        n, u = store.upsert_raw([main_obj])
                        scraped += n + u
                        continue
                    else:
                        continue
                if not isinstance(data, dict):
                    continue
                main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                n, u = store.upsert_raw([main_obj])
                scraped += n + u
            print(f"Scraped v1 profiles: {scraped}")
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
                    st2 = PdbStorage()
                    df2 = st2.load_joined()
                    rows = df2.dropna(subset=["vector"]).reset_index(drop=True)
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
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        rows: list[dict] = []
        # extract a small set of normalized fields from payloads
        for _, row in df.iterrows():
            cid = str(row.get("cid"))
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            rec: dict = {"cid": cid}
            # profile identity
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id", "_profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v; break
                if isinstance(v, str):
                    try:
                        pid = int(v); break
                    except Exception:
                        pass
            if pid is not None:
                rec["pid"] = pid
            # common name/title field guesses
            for k in ("name", "title", "display_name", "username"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            # typology hints if present
            for k in ("mbti", "socionics", "big5", "enneagram"):
                v = obj.get(k)
                if v is not None:
                    rec[k] = v
            rows.append(rec)
        out = pd.DataFrame(rows).drop_duplicates(subset=["cid"]) if rows else pd.DataFrame(columns=["cid","pid","name"]) 
        # attach vector presence flag
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)[["cid"]]
                vdf["has_vector"] = True
                out = out.merge(vdf, on="cid", how="left")
                out["has_vector"] = out["has_vector"].fillna(False)
            except Exception:
                pass
        outp = Path(getattr(args, "out", "data/bot_store/pdb_profiles_normalized.parquet"))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        print(f"Exported {len(out)} rows to {outp}")
    elif args.cmd == "scrape-v1-missing":
        import pandas as pd
        import random as _random
        import json as _json
        from pathlib import Path as _Path
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)