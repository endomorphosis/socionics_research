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

    sub.add_parser("hot-queries", help="Fetch trending search hot queries (v2) and store raw")

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

    # Helper: find subcategories by keyword (to discover character groups)
    p_fsc = sub.add_parser(
        "find-subcats",
        help="List subcategories from v2 search/top for a keyword; useful to find character groups",
    )
    p_fsc.add_argument("--keyword", type=str, required=True, help="Keyword to query in search/top")
    p_fsc.add_argument("--limit", type=int, default=40, help="Limit per page")
    p_fsc.add_argument("--pages", type=int, default=1, help="Pages to fetch (via nextCursor)")
    p_fsc.add_argument("--until-empty", action="store_true", help="Keep paging until an empty page")

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
    elif args.cmd in {"search-faiss-pretty", "search-characters"}:
        import numpy as np
        from pathlib import Path
        import faiss  # type: ignore
        # load index and cid map
        idxp = Path(args.index)
        map_path = idxp.with_suffix(idxp.suffix + ".cids")
        if not idxp.exists() or not map_path.exists():
            print(f"Missing index or cid map at {idxp} and {map_path}. Run the corresponding index command first.")
            return
        index = faiss.read_index(str(idxp))
        cids = map_path.read_text(encoding="utf-8").splitlines()
        # embed and normalize query
        qv = np.array(embed_texts([args.query])[0], dtype="float32")[None, :]
        qv = qv / (np.linalg.norm(qv, axis=1, keepdims=True) + 1e-12)
        scores, I = index.search(qv, args.top)
        # map cids to names via joined store
        store = PdbStorage()
        df = store.load_joined()
        cid_to_name: dict[str, str] = {}
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
        for rank, (i, s) in enumerate(zip(I[0], scores[0]), start=1):
            if i < 0 or i >= len(cids):
                continue
            cid = cids[int(i)]
            print(f"{rank}. score={float(s):.4f} name={cid_to_name.get(cid, '(unknown)')} cid={cid[:12]}")
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
                        print(f"keyword='{q}' page={pages+1} items={len(batch)}{tail}")
                        if names:
                            print(f"  {names}")

                    new = upd = 0
                    if not args.dry_run and batch:
                        n, u = store.upsert_raw(batch)
                        new += n; upd += u
                        total_new += n; total_updated += u

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
            print(f"Done. New: {total_new} Updated: {total_updated}")
        try:
            asyncio.run(_run())
        except Exception as e:
            print(f"search-keywords failed: {e}")
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
            is_char = obj.get("isCharacter") is True or obj.get("_from_character_group") is True
            if not is_char:
                continue
            # avoid obvious MBTI buckets
            nm = obj.get("name") or obj.get("title") or obj.get("subcategory")
            if isinstance(nm, str) and nm.strip().upper() in {
                "INTJ","INTP","ENTJ","ENTP","INFJ","INFP","ENFJ","ENFP",
                "ISTJ","ISFJ","ESTJ","ESFJ","ISTP","ISFP","ESTP","ESFP",
            }:
                continue
            rec: dict = {"cid": str(row.get("cid")), "_source": obj.get("_source")}
            # profile id detection
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
            # carry name
            for k in ("name", "title", "display_name", "username", "subcategory"):
                v = obj.get(k)
                if isinstance(v, str) and v:
                    rec["name"] = v
                    break
            rows.append(rec)
        out = _Path(getattr(args, "out", "data/bot_store/pdb_characters.parquet"))
        out.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).drop_duplicates(subset=["cid"]).to_parquet(out, index=False)
        print(f"Exported {len(rows)} character-like rows to {out}")
        if rows and args.sample:
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
        mat = _np.vstack(merged["vector"].to_list()).astype("float32")
        norms = _np.linalg.norm(mat, axis=1, keepdims=True) + 1e-12
        mat = mat / norms
        index = faiss.IndexFlatIP(mat.shape[1])
        index.add(mat)
        outp = _Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        faiss.write_index(index, str(outp))
        (outp.with_suffix(outp.suffix + ".cids")).write_text("\n".join(merged["cid"].astype(str).tolist()), encoding="utf-8")
        print(f"Indexed {len(merged)} character vectors to {outp}")
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