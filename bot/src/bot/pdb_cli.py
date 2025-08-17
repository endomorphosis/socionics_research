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
        help="Path to file containing keywords (comma/newline/space separated)",
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
    elif args.cmd == "find-subcats":
        async def _run():
            client = _make_client(args)
            q = args.keyword
            page = 0
            next_cursor = None
            seen: set[int] = set()
            while True:
                page += 1
                if not args.until_empty and page > max(getattr(args, "pages", 1), 1):
                    break
                params = {"keyword": q, "limit": args.limit}
                if next_cursor is not None:
                    params["nextCursor"] = next_cursor
                try:
                    data = await client.fetch_json("search/top", params)
                except Exception as e:
                    print(f"fetch failed: {e}")
                    break
                container = data.get("data") if isinstance(data, dict) else None
                if not isinstance(container, dict):
                    print("No data container.")
                    break
                # Collect subcategory-like items from 'subcategories' and embedded in 'profiles'
                found = 0
                items: list[dict] = []
                subs = container.get("subcategories")
                if isinstance(subs, list):
                    items.extend([it for it in subs if isinstance(it, dict)])
                profs = container.get("profiles")
                if isinstance(profs, list):
                    for it in profs:
                        if not isinstance(it, dict):
                            continue
                        if it.get("subcatID") is not None or it.get("subcategory") is not None:
                            items.append(it)
                        # Also consider MBTI bucket items as expansion candidates
                        nm = it.get("name")
                        if isinstance(nm, str) and nm in {
                            "INTJ","INTP","ENTJ","ENTP","INFJ","INFP","ENFJ","ENFP",
                            "ISTJ","ISFJ","ESTJ","ESFJ","ISTP","ISFP","ESTP","ESFP"
                        }:
                            items.append(it)
                # Print unique candidates with id/subcatID
                for it in items:
                    sid = None
                    for k in ("id", "subcatID"):
                        v = it.get(k)
                        if isinstance(v, int):
                            sid = v; break
                        if isinstance(v, str):
                            try:
                                sid = int(v); break
                            except Exception:
                                pass
                    name = it.get("subcategory") or it.get("name")
                    is_char = it.get("isCharacter") is True or (isinstance(name, str) and "character" in name.lower())
                    if isinstance(sid, int) and sid not in seen:
                        seen.add(sid)
                        found += 1
                        print(f"id={sid} | name={name} | isCharacterGroup={bool(is_char)}")
                if not found:
                    print(f"page {page}: no subcategories")
                nc = container.get("nextCursor") if isinstance(container, dict) else None
                if nc is None and isinstance(data, dict):
                    nc = data.get("nextCursor")
                if isinstance(nc, (int, str)):
                    next_cursor = nc
                    continue
                break
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
            # optional edges storage
            try:
                from .pdb_edges import PdbEdgesStorage as _PdbEdges
                edges_store = _PdbEdges()
            except Exception:
                edges_store = None
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
                # Record edges from seed to related profile ids
                try:
                    edges: list[dict] = []
                    for it in out:
                        if isinstance(it, dict):
                            vid = None
                            for kk in ("id", "profileId", "profileID", "profile_id"):
                                vv = it.get(kk)
                                if isinstance(vv, int):
                                    vid = vv; break
                                if isinstance(vv, str):
                                    try:
                                        vid = int(vv); break
                                    except Exception:
                                        pass
                            if isinstance(vid, int):
                                edges.append({"from_pid": pid, "to_pid": vid, "relation": it.get("_source_list") or "related", "source": "v2_related"})
                    if edges:
                        if edges_store is not None:
                            edges_store.upsert_edges(edges)
                except Exception:
                    pass
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
            # MBTI bucket detection helper to warn when only type buckets appear
            MBTI_TYPES = {
                "INTJ", "INTP", "ENTJ", "ENTP",
                "INFJ", "INFP", "ENFJ", "ENFP",
                "ISTJ", "ISFJ", "ESTJ", "ESFJ",
                "ISTP", "ISFP", "ESTP", "ESFP",
            }
            def _is_mbti_bucket_item(it: dict) -> bool:
                if not isinstance(it, dict):
                    return False
                name = it.get("name")
                return isinstance(name, str) and name in MBTI_TYPES
            MBTI_TYPES = {
                "INTJ", "INTP", "ENTJ", "ENTP",
                "INFJ", "INFP", "ENFJ", "ENFP",
                "ISTJ", "ISFJ", "ESTJ", "ESFJ",
                "ISTP", "ISFP", "ESTP", "ESFP",
            }
            def _is_mbti_bucket_item(it: dict) -> bool:
                if not isinstance(it, dict):
                    return False
                name = it.get("name")
                return isinstance(name, str) and name in MBTI_TYPES
            for k in keys:
                next_cursor_token: int | str | None = args.next_cursor if args.next_cursor != 0 else None
                page = 0
                seen_ident: set[str] = set()
                no_progress = 0
                while True:
                    page += 1
                    params = {"keyword": k, "limit": args.limit}
                    if next_cursor_token is not None:
                        params["nextCursor"] = next_cursor_token
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
                    subcats: list[dict] = []
                    for fname, val in container.items():
                        if fname == "subcategories":
                            if isinstance(val, list) and val:
                                for it in val:
                                    if isinstance(it, dict):
                                        subcats.append(it)
                                        if allowed_lists is None or fname in allowed_lists:
                                            out.append({**it, "_source": "search_follow_hot_top", "_source_list": fname, "_query": k, "_page": page, "_nextCursor": next_cursor_token})
                            continue
                        # Heuristic: capture subcategory-like objects inside 'profiles'
                        if fname == "profiles" and isinstance(val, list):
                            for it in val:
                                if isinstance(it, dict) and (it.get("subcatID") is not None or it.get("subcategory") is not None):
                                    subcats.append(it)
                        if allowed_lists is not None and fname not in allowed_lists:
                            continue
                        if isinstance(val, list) and val:
                            for it in val:
                                if isinstance(it, dict):
                                    out.append({**it, "_source": "search_follow_hot_top", "_source_list": fname, "_query": k, "_page": page, "_nextCursor": next_cursor_token})
                    # Expand subcategories if requested
                    if args.expand_subcategories and subcats:
                        def _is_character_group(item: dict) -> bool:
                            try:
                                if item.get("isCharacter") is True:
                                    return True
                                subn = item.get("subcategory")
                                if isinstance(subn, str) and ("character" in subn.lower() or "characters" in subn.lower()):
                                    return True
                            except Exception:
                                pass
                            return False
                        cand = [it for it in subcats if isinstance(it, dict) and _is_character_group(it)] or [it for it in subcats if isinstance(it, dict)]
                        cand = cand[: max(getattr(args, "expand_max", 5), 0)]
                        expanded: list[dict] = []
                        for sc in cand:
                            from_char_group = False
                            try:
                                subn = sc.get("subcategory") if isinstance(sc, dict) else None
                                if sc.get("isCharacter") is True or (isinstance(subn, str) and ("character" in subn.lower() or "characters" in subn.lower())):
                                    from_char_group = True
                            except Exception:
                                from_char_group = False
                            sid = None
                            try:
                                v = sc.get("id") if isinstance(sc, dict) else None
                                if isinstance(v, int):
                                    sid = v
                                elif isinstance(v, str):
                                    sid = int(v)
                            except Exception:
                                sid = None
                            if not isinstance(sid, int):
                                continue
                            try:
                                data_rel = await client.fetch_json(f"profiles/{sid}/related")
                            except Exception:
                                continue
                            rel_items: list[dict] = []
                            if isinstance(data_rel, dict):
                                cont2 = data_rel.get("data") or data_rel
                                if isinstance(cont2, dict):
                                    rp = cont2.get("relatedProfiles")
                                    if isinstance(rp, list):
                                        for it in rp:
                                            if isinstance(it, dict):
                                                rel_items.append({**it, "_source_list": "profiles"})
                                    else:
                                        for k2, v2 in cont2.items():
                                            if isinstance(v2, list):
                                                # allow 'relatedProfiles' even when only 'profiles' requested
                                                if allowed_lists is not None and (k2 not in allowed_lists and k2 != "relatedProfiles"):
                                                    continue
                                                for it in v2:
                                                    if isinstance(it, dict):
                                                        rel_items.append({**it, "_source_list": ("profiles" if k2 == "relatedProfiles" else k2)})
                            # Infer character-group from related result mix
                            ch_total = sum(1 for it in rel_items if isinstance(it, dict))
                            ch_count = sum(1 for it in rel_items if isinstance(it, dict) and it.get("isCharacter") is True)
                            inferred_char_group = False
                            try:
                                if ch_total > 0 and ch_count >= 2 and (ch_count / max(ch_total, 1)) >= 0.3:
                                    inferred_char_group = True
                            except Exception:
                                inferred_char_group = False
                            from_char_group_final = bool(from_char_group or inferred_char_group)
                            for it in rel_items:
                                if not isinstance(it, dict):
                                    continue
                                if args.filter_characters:
                                    ch = it.get("isCharacter")
                                    if not (ch is True or (getattr(args, "characters_relaxed", False) and from_char_group_final is True)):
                                        continue
                                expanded.append({**it, "_source": "search_follow_hot_related", "_query": k, "_page": page, "_expanded_from": sid, "_from_character_group": from_char_group_final})
                        if expanded:
                            out = out + expanded
                    # compute page identity keys (prefer id/profileId/etc plus source_list)
                    page_keys: list[str] = []
                    for it in out:
                        oid = None
                        if isinstance(it, dict):
                            for kk in ("id", "profileId", "profileID", "profile_id"):
                                vv = it.get(kk)
                                if isinstance(vv, (int, str)):
                                    oid = str(vv)
                                    break
                        sl = it.get("_source_list") if isinstance(it, dict) else None
                        page_keys.append(f"{sl}:{oid}")
                    new_keys = [pk for pk in page_keys if pk not in seen_ident]
                    if new_keys:
                        seen_ident.update(new_keys)
                        no_progress = 0
                    else:
                        no_progress += 1
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
                    # Determine nextCursor (may appear in container or at top-level)
                    nc = container.get("nextCursor") if isinstance(container, dict) else None
                    if nc is None and isinstance(data, dict):
                        nc = data.get("nextCursor")
                    if isinstance(nc, (int, str)):
                        if nc == next_cursor_token:
                            # No progress, stop paging
                            break
                        next_cursor_token = nc
                    else:
                        # No cursor provided; stop unless until-empty with non-empty page already handled
                        next_cursor_token = None
                        if not args.until_empty:
                            break
                    if args.max_no_progress_pages > 0 and no_progress >= args.max_no_progress_pages:
                        print(f"key={k!r}: stopping after {no_progress} no-progress pages")
                        break
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
            next_cursor_token: int | str | None = args.next_cursor if args.next_cursor != 0 else None
            total_new = total_upd = 0
            total_would_be = 0  # for dry-run reporting
            page = 0
            seen_ident: set[str] = set()
            no_progress = 0
            while True:
                page += 1
                params = {"limit": args.limit}
                if next_cursor_token is not None:
                    params["nextCursor"] = next_cursor_token
                if q is not None:
                    params["keyword"] = q
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
                # collect subcategories for optional expansion
                subcats: list[dict] = []
                # Helper for detecting MBTI bucket items so we can warn users
                MBTI_TYPES = {
                    "INTJ", "INTP", "ENTJ", "ENTP",
                    "INFJ", "INFP", "ENFJ", "ENFP",
                    "ISTJ", "ISFJ", "ESTJ", "ESFJ",
                    "ISTP", "ISFP", "ESTP", "ESFP",
                }
                def _is_mbti_bucket_item(it: dict) -> bool:
                    if not isinstance(it, dict):
                        return False
                    name = it.get("name")
                    return isinstance(name, str) and name in MBTI_TYPES
                mbti_count = 0
                for fname, val in container.items():
                    if fname == "subcategories":
                        # Always collect subcategories for potential expansion; only upsert if allowed
                        if isinstance(val, list) and val:
                            for it in val:
                                if isinstance(it, dict):
                                    subcats.append(it)
                                    if allowed_lists is None or fname in allowed_lists:
                                        out.append({**it, "_source": "v2_search_top", "_source_list": fname, "_query": q, "_page": page, "_nextCursor": next_cursor_token})
                        continue
                    # Heuristic: sometimes subcategory buckets appear under 'profiles'; detect and collect
                    if fname == "profiles" and isinstance(val, list):
                        for it in val:
                            if isinstance(it, dict) and (it.get("subcatID") is not None or it.get("subcategory") is not None):
                                subcats.append(it)
                            if _is_mbti_bucket_item(it):
                                # MBTI type items are effectively subcategory buckets; expand via related
                                subcats.append(it)
                                mbti_count += 1
                    if allowed_lists is not None and fname not in allowed_lists:
                        # Special-case: map recommendProfiles into profiles when only-profiles is requested
                        if fname == "recommendProfiles" and "profiles" in allowed_lists and isinstance(val, list) and val:
                            for it in val:
                                if isinstance(it, dict):
                                    out.append({**it, "_source": "v2_search_top", "_source_list": "profiles", "_query": q, "_page": page, "_nextCursor": next_cursor_token})
                        continue
                    if isinstance(val, list) and val:
                        for it in val:
                            if isinstance(it, dict):
                                out.append({**it, "_source": "v2_search_top", "_source_list": fname, "_query": q, "_page": page, "_nextCursor": next_cursor_token})
                if (not args.expand_subcategories) and mbti_count >= 12:
                    print("Note: Detected MBTI type buckets in 'profiles'. Re-run with --expand-subcategories to surface real profiles.")
                # Expand subcategories via related if requested
                expanded: list[dict] = []
                if args.expand_subcategories and subcats:
                    # heuristic filter to prioritize likely character groups
                    def _is_character_group(item: dict) -> bool:
                        try:
                            if item.get("isCharacter") is True:
                                return True
                            subn = item.get("subcategory")
                            if isinstance(subn, str) and ("character" in subn.lower() or "characters" in subn.lower()):
                                return True
                        except Exception:
                            pass
                        return False
                    # limit how many we expand per page
                    cand = [it for it in subcats if isinstance(it, dict) and _is_character_group(it)] or [it for it in subcats if isinstance(it, dict)]
                    cand = cand[: max(getattr(args, "expand_max", 5), 0)]
                    # fetch related for each candidate id
                    fetched = 0
                    for sc in cand:
                        from_char_group = False
                        try:
                            from_char_group = _is_character_group(sc)
                        except Exception:
                            from_char_group = False
                        sid = None
                        try:
                            v = sc.get("id") if isinstance(sc, dict) else None
                            if isinstance(v, int):
                                sid = v
                            elif isinstance(v, str):
                                sid = int(v)
                            elif isinstance(sc, dict) and sc.get("subcatID") is not None:
                                v2 = sc.get("subcatID")
                                if isinstance(v2, int):
                                    sid = v2
                                elif isinstance(v2, str):
                                    sid = int(v2)
                        except Exception:
                            sid = None
                        if not isinstance(sid, int):
                            continue
                        try:
                            data_rel = await client.fetch_json(f"profiles/{sid}/related")
                        except Exception:
                            continue
                        rel_items: list[dict] = []
                        if isinstance(data_rel, dict):
                            cont2 = data_rel.get("data") or data_rel
                            if isinstance(cont2, dict):
                                rp = cont2.get("relatedProfiles")
                                if isinstance(rp, list):
                                    for it in rp:
                                        if isinstance(it, dict):
                                            # Treat relatedProfiles as profiles for downstream filtering
                                            rel_items.append({**it, "_source_list": "profiles"})
                                else:
                                    for k2, v2 in cont2.items():
                                        if isinstance(v2, list):
                                            for it in v2:
                                                if not isinstance(it, dict):
                                                    continue
                                                # If only 'profiles' are allowed, map character-like items to profiles
                                                if allowed_lists is not None and (k2 not in allowed_lists and k2 != "relatedProfiles"):
                                                    allow_map = False
                                                    if "profiles" in allowed_lists:
                                                        if getattr(args, "filter_characters", False):
                                                            ch = it.get("isCharacter")
                                                            if ch is True or from_char_group is True:
                                                                allow_map = True
                                                    if not allow_map:
                                                        continue
                                                sl = ("profiles" if (k2 == "relatedProfiles" or (allowed_lists is not None and "profiles" in allowed_lists and (getattr(args, "filter_characters", False) and (it.get("isCharacter") is True or from_char_group is True)))) else k2)
                                                rel_items.append({**it, "_source_list": sl})
                        # infer char group if mix is character-heavy
                        ch_total = sum(1 for it in rel_items if isinstance(it, dict))
                        ch_count = sum(1 for it in rel_items if isinstance(it, dict) and it.get("isCharacter") is True)
                        inferred_char_group = False
                        try:
                            if ch_total > 0 and ch_count >= 2 and (ch_count / max(ch_total, 1)) >= 0.3:
                                inferred_char_group = True
                        except Exception:
                            inferred_char_group = False
                        from_char_group_final = bool(from_char_group or inferred_char_group)
                        # annotate and filter
                        for it in rel_items:
                            if not isinstance(it, dict):
                                continue
                            if args.filter_characters:
                                ch = it.get("isCharacter")
                                if not (ch is True or (from_char_group_final is True)):
                                    if not getattr(args, "characters_relaxed", False):
                                        continue
                            expanded.append({**it, "_source": "v2_related_from_subcategory", "_query": q, "_page": page, "_expanded_from": sid, "_from_character_group": from_char_group_final})
                        fetched += 1
                    # merge expanded into out for unified handling
                    if expanded:
                        out = out + expanded
                # compute identity keys per item (id/profileId etc + source_list)
                page_keys: list[str] = []
                for it in out:
                    oid = None
                    if isinstance(it, dict):
                        for kk in ("id", "profileId", "profileID", "profile_id"):
                            vv = it.get(kk)
                            if isinstance(vv, (int, str)):
                                oid = str(vv)
                                break
                    sl = it.get("_source_list") if isinstance(it, dict) else None
                    page_keys.append(f"{sl}:{oid}")
                new_keys = [pk for pk in page_keys if pk not in seen_ident]
                if new_keys:
                    seen_ident.update(new_keys)
                    no_progress = 0
                else:
                    no_progress += 1
                if not out:
                    print(f"Page {page}: no list items found.")
                    if args.until_empty:
                        break
                else:
                    # Optional verbose listing of entities similar to search-keywords
                    def _display_name(it: dict) -> str:
                        for kk in ("name", "title", "display_name", "username"):
                            v = it.get(kk)
                            if isinstance(v, str) and v:
                                return v
                        for kk in ("subcategory", "boardTitle", "board", "label"):
                            v = it.get(kk)
                            if isinstance(v, str) and v:
                                return v
                        try:
                            return json.dumps({k: it.get(k) for k in ("name", "title", "username") if k in it}, ensure_ascii=False)
                        except Exception:
                            return "(unknown)"
                    if args.verbose:
                        by_list: dict[str, list[str]] = {}
                        for it in out:
                            if not isinstance(it, dict):
                                continue
                            if args.filter_characters:
                                if getattr(args, "characters_relaxed", False):
                                    ch = it.get("isCharacter")
                                    if not (ch is True or it.get("_from_character_group") is True):
                                        continue
                                else:
                                    ch = it.get("isCharacter")
                                    if ch is not True:
                                        continue
                            lst = str(it.get("_source_list") or "")
                            nm = _display_name(it)
                            pid = None
                            for kk in ("id", "profileId", "profileID", "profile_id"):
                                vv = it.get(kk)
                                if isinstance(vv, (int, str)):
                                    pid = str(vv)
                                    break
                            label = f"{nm}" + (f" (id={pid})" if pid is not None else "")
                            by_list.setdefault(lst or "(list)", []).append(label)
                        header = f"Page {page}: {sum(len(v) for v in by_list.values())} items"
                        if args.dry_run:
                            total_would_be += sum(len(v) for v in by_list.values())
                            header += " (dry-run)"
                        print(header)
                        for lst, names in by_list.items():
                            print(f"  {lst}: {len(names)}")
                            for nm in names:
                                print(f"    - {nm}")
                        if not args.dry_run:
                            to_write = out
                            if args.filter_characters:
                                if getattr(args, "characters_relaxed", False):
                                    to_write = [
                                        it for it in out
                                        if isinstance(it, dict) and (
                                            it.get("isCharacter") is True or it.get("_from_character_group") is True
                                        )
                                    ]
                                else:
                                    to_write = [it for it in out if isinstance(it, dict) and (it.get("isCharacter") is True)]
                            new, upd = store.upsert_raw(to_write)
                            total_new += new
                            total_upd += upd
                        else:
                            if args.dry_run:
                                n_items = len(out)
                                if args.filter_characters:
                                    try:
                                        n_items = sum(1 for it in out if isinstance(it, dict) and (it.get("isCharacter") is True))
                                    except Exception:
                                        pass
                                total_would_be += n_items
                                print(f"Page {page}: would upsert {n_items} items from search/top (dry-run)")
                            else:
                                to_write = out
                                if args.filter_characters:
                                    if getattr(args, "characters_relaxed", False):
                                        to_write = [
                                            it for it in out
                                            if isinstance(it, dict) and (
                                                it.get("isCharacter") is True or it.get("_from_character_group") is True
                                            )
                                        ]
                                    else:
                                        to_write = [it for it in out if isinstance(it, dict) and (it.get("isCharacter") is True)]
                                new, upd = store.upsert_raw(to_write)
                                total_new += new
                                total_upd += upd
                                print(f"Page {page}: upserted {new} new, {upd} updated from search/top")
                # Determine nextCursor if exposed (may be in container or top-level)
                nc = container.get("nextCursor") if isinstance(container, dict) else None
                if nc is None and isinstance(data, dict):
                    nc = data.get("nextCursor")
                if isinstance(nc, (int, str)):
                    if nc == next_cursor_token:
                        break
                    next_cursor_token = nc
                else:
                    next_cursor_token = None
                    if not args.until_empty:
                        break
                if args.max_no_progress_pages > 0 and no_progress >= args.max_no_progress_pages:
                    print(f"Stopping after {no_progress} no-progress pages")
                    break
                if not args.until_empty and page >= max(args.pages, 1):
                    break
            if args.dry_run:
                print(f"Done (dry-run). Would upsert {total_would_be} items in total.")
                return
            print(f"Done. Total upserts: {total_new} new, {total_upd} updated")
        asyncio.run(_run())
    elif args.cmd == "search-keywords":
        async def _run():
            client = _make_client(args)
            store = PdbStorage()
            # collect keywords from --queries and --query-file
            keys: list[str] = []
            if getattr(args, "queries", None):
                keys.extend([s.strip() for s in args.queries.split(',') if s.strip()])
            if getattr(args, "query_file", None):
                try:
                    from pathlib import Path as _Path
                    import csv as _csv
                    txt = _Path(args.query_file).read_text(encoding="utf-8")
                    for line in txt.splitlines():
                        line = line.strip()
                        # skip empty lines and full-line comments
                        if not line or line.startswith('#'):
                            continue
                        # normalize tabs to commas; DO NOT split on spaces to preserve multi-word phrases
                        line = line.replace('\t', ',')
                        for row in _csv.reader([line], delimiter=',', quotechar='"'):
                            for tok in row:
                                tok = tok.strip()
                                if tok:
                                    keys.append(tok)
                except Exception as e:
                    print(f"Failed to read --query-file: {e}")
                    return
            # dedupe preserving order
            seen_k = set()
            keys = [k for k in keys if (k not in seen_k and not seen_k.add(k))]
            if not keys:
                print("No keywords provided. Use --queries and/or --query-file.")
                return
            # parse filters
            allowed_lists = None
            if args.only_profiles:
                allowed_lists = {"profiles"}
            elif args.lists:
                allowed_lists = {s.strip() for s in args.lists.split(',') if s.strip()}
            total_new = total_upd = 0
            total_would = 0
            # Build per-keyword sweep tokens when expand-characters is enabled
            alpha_tokens = list("abcdefghijklmnopqrstuvwxyz") + [str(i) for i in range(10)]
            for q in keys:
                page = 0
                next_cursor_token: int | str | None = None
                seen_ident: set[str] = set()
                no_progress = 0
                # Create a list of queries: base keyword + optional expanded tokens
                q_list = [q]
                if args.expand_characters:
                    q_list = [q] + [f"{q} {tok}" for tok in alpha_tokens]
                # Append custom suffix terms if provided (e.g., 'characters,cast,people')
                if getattr(args, "append_terms", None):
                    extra = [s.strip() for s in str(args.append_terms).split(',') if s.strip()]
                    if extra:
                        q_list = q_list + [f"{q} {suf}" for suf in extra]
                for qq in q_list:
                    page = 0
                    next_cursor_token = None
                    seen_ident = set()
                    no_progress = 0
                    local_pages_limit = args.pages if not args.expand_characters else max(args.expand_pages, 1)
                    while True:
                        page += 1
                        # Guard: respect per-keyword page limit before fetching next page
                        if not args.until_empty and page > max(local_pages_limit, 1):
                            break
                        params = {"limit": args.limit, "keyword": qq}
                        if next_cursor_token is not None:
                            params["nextCursor"] = next_cursor_token
                        try:
                            data = await client.fetch_json("search/top", params)
                        except Exception as e:
                            print(f"keyword={qq!r} page {page}: fetch failed: {e}")
                            break
                        container = data.get("data") if isinstance(data, dict) else None
                        if not isinstance(container, dict):
                            print(f"keyword={qq!r} page {page}: no data container")
                            break
                        out = []
                        subcats: list[dict] = []
                        MBTI_TYPES = {
                            "INTJ", "INTP", "ENTJ", "ENTP",
                            "INFJ", "INFP", "ENFJ", "ENFP",
                            "ISTJ", "ISFJ", "ESTJ", "ESFJ",
                            "ISTP", "ISFP", "ESTP", "ESFP",
                        }
                        def _is_mbti_bucket_item(it: dict) -> bool:
                            if not isinstance(it, dict):
                                return False
                            name = it.get("name")
                            return isinstance(name, str) and name in MBTI_TYPES
                        mbti_count = 0
                        for fname, val in container.items():
                            if fname == "subcategories":
                                # Always gather subcategories for potential expansion
                                if isinstance(val, list) and val:
                                    for it in val:
                                        if isinstance(it, dict):
                                            subcats.append(it)
                                            # Only upsert raw subcategories when explicitly allowed
                                            if allowed_lists is None or fname in allowed_lists:
                                                out.append({**it, "_source": "v2_search_top", "_source_list": fname, "_query": qq, "_page": page, "_nextCursor": next_cursor_token})
                                continue
                            # Heuristic: detect subcategory-like objects inside 'profiles'
                            if fname == "profiles" and isinstance(val, list):
                                for it in val:
                                    if isinstance(it, dict) and (it.get("subcatID") is not None or it.get("subcategory") is not None):
                                        subcats.append(it)
                                    if _is_mbti_bucket_item(it):
                                        # Treat MBTI type items as expansion candidates
                                        subcats.append(it)
                                        mbti_count += 1
                            # For other lists, honor allowed_lists filter
                            if allowed_lists is not None and fname not in allowed_lists:
                                # Map recommendProfiles to profiles when only-profiles is requested
                                if fname == "recommendProfiles" and "profiles" in allowed_lists and isinstance(val, list) and val:
                                    for it in val:
                                        if isinstance(it, dict):
                                            out.append({**it, "_source": "v2_search_top", "_source_list": "profiles", "_query": qq, "_page": page, "_nextCursor": next_cursor_token})
                                continue
                            if isinstance(val, list) and val:
                                for it in val:
                                    if isinstance(it, dict):
                                        out.append({**it, "_source": "v2_search_top", "_source_list": fname, "_query": qq, "_page": page, "_nextCursor": next_cursor_token})
                        if (not getattr(args, "expand_subcategories", False)) and mbti_count >= 12:
                            print(f"keyword={qq!r} page {page}: Detected MBTI type buckets in 'profiles'. Re-run with --expand-subcategories to surface real profiles.")
                        # Optionally expand subcategory hits via related to surface profiles/characters
                        if getattr(args, "expand_subcategories", False) and subcats:
                            def _is_character_group(item: dict) -> bool:
                                try:
                                    if item.get("isCharacter") is True:
                                        return True
                                    subn = item.get("subcategory")
                                    if isinstance(subn, str) and ("character" in subn.lower() or "characters" in subn.lower()):
                                        return True
                                except Exception:
                                    pass
                                return False
                            candidates = [it for it in subcats if isinstance(it, dict) and _is_character_group(it)] or [it for it in subcats if isinstance(it, dict)]
                            max_to_expand = max(getattr(args, "expand_max", 5), 0)
                            candidates = candidates[: max_to_expand]
                            expanded: list[dict] = []
                            for sc in candidates:
                                from_char_group = False
                                try:
                                    from_char_group = _is_character_group(sc)
                                except Exception:
                                    from_char_group = False
                                sid = None
                                try:
                                    v = sc.get("id") if isinstance(sc, dict) else None
                                    if isinstance(v, int):
                                        sid = v
                                    elif isinstance(v, str):
                                        sid = int(v)
                                    elif isinstance(sc, dict) and sc.get("subcatID") is not None:
                                        v2 = sc.get("subcatID")
                                        if isinstance(v2, int):
                                            sid = v2
                                        elif isinstance(v2, str):
                                            sid = int(v2)
                                except Exception:
                                    sid = None
                                if not isinstance(sid, int):
                                    continue
                                try:
                                    data_rel = await client.fetch_json(f"profiles/{sid}/related")
                                except Exception:
                                    continue
                                rel_items: list[dict] = []
                                if isinstance(data_rel, dict):
                                    cont2 = data_rel.get("data") or data_rel
                                    if isinstance(cont2, dict):
                                        rp = cont2.get("relatedProfiles")
                                        if isinstance(rp, list):
                                            for it in rp:
                                                if isinstance(it, dict):
                                                    # Treat relatedProfiles as profiles for downstream filtering
                                                    rel_items.append({**it, "_source_list": "profiles"})
                                        else:
                                            for k2, v2 in cont2.items():
                                                if isinstance(v2, list):
                                                    for it in v2:
                                                        if not isinstance(it, dict):
                                                            continue
                                                        if allowed_lists is not None and (k2 not in allowed_lists and k2 != "relatedProfiles"):
                                                            allow_map = False
                                                            if "profiles" in allowed_lists:
                                                                if getattr(args, "filter_characters", False):
                                                                    ch = it.get("isCharacter")
                                                                    if ch is True or from_char_group is True:
                                                                        allow_map = True
                                                            if not allow_map:
                                                                continue
                                                        sl = ("profiles" if (k2 == "relatedProfiles" or (allowed_lists is not None and "profiles" in allowed_lists and (getattr(args, "filter_characters", False) and (it.get("isCharacter") is True or from_char_group is True)))) else k2)
                                                        rel_items.append({**it, "_source_list": sl})
                                # infer from mix
                                ch_total = sum(1 for it in rel_items if isinstance(it, dict))
                                ch_count = sum(1 for it in rel_items if isinstance(it, dict) and it.get("isCharacter") is True)
                                inferred_char_group = False
                                try:
                                    if ch_total > 0 and ch_count >= 2 and (ch_count / max(ch_total, 1)) >= 0.3:
                                        inferred_char_group = True
                                except Exception:
                                    inferred_char_group = False
                                from_char_group_final = bool(from_char_group or inferred_char_group)
                                for it in rel_items:
                                    if not isinstance(it, dict):
                                        continue
                                    if getattr(args, "filter_characters", False):
                                        ch = it.get("isCharacter")
                                        if not (ch is True or (from_char_group_final is True)):
                                            if not getattr(args, "characters_relaxed", False):
                                                continue
                                    expanded.append({**it, "_source": "v2_related_from_subcategory", "_query": qq, "_page": page, "_expanded_from": sid, "_from_character_group": from_char_group_final})
                            if expanded:
                                out = out + expanded
                    # progress tracking
                    page_keys: list[str] = []
                    for it in out:
                        oid = None
                        if isinstance(it, dict):
                            for kk in ("id", "profileId", "profileID", "profile_id"):
                                vv = it.get(kk)
                                if isinstance(vv, (int, str)):
                                    oid = str(vv)
                                    break
                        sl = it.get("_source_list") if isinstance(it, dict) else None
                        page_keys.append(f"{sl}:{oid}")
                    new_keys = [pk for pk in page_keys if pk not in seen_ident]
                    if new_keys:
                        seen_ident.update(new_keys)
                        no_progress = 0
                    else:
                        no_progress += 1
                    if not out:
                        if args.until_empty:
                            break
                    else:
                        # When verbose, print entity names grouped by list
                        def _display_name(it: dict) -> str:
                            for kk in ("name", "title", "display_name", "username"):
                                v = it.get(kk)
                                if isinstance(v, str) and v:
                                    return v
                            for kk in ("subcategory", "boardTitle", "board", "label"):
                                v = it.get(kk)
                                if isinstance(v, str) and v:
                                    return v
                            try:
                                return json.dumps({k: it.get(k) for k in ("name", "title", "username") if k in it}, ensure_ascii=False)
                            except Exception:
                                return "(unknown)"
                        if args.verbose:
                            by_list: dict[str, list[str]] = {}
                            for it in out:
                                if not isinstance(it, dict):
                                    continue
                                if args.filter_characters:
                                    if getattr(args, "characters_relaxed", False):
                                        ch = it.get("isCharacter")
                                        if not (ch is True or it.get("_from_character_group") is True):
                                            continue
                                    else:
                                        ch = it.get("isCharacter")
                                        if ch is not True:
                                            continue
                                lst = str(it.get("_source_list") or "")
                                nm = _display_name(it)
                                pid = None
                                for kk in ("id", "profileId", "profileID", "profile_id"):
                                    vv = it.get(kk)
                                    if isinstance(vv, (int, str)):
                                        pid = str(vv)
                                        break
                                label = f"{nm}" + (f" (id={pid})" if pid is not None else "")
                                by_list.setdefault(lst or "(list)", []).append(label)
                            header = f"keyword={qq!r} page {page}: {sum(len(v) for v in by_list.values())} items"
                            if args.dry_run:
                                total_would += sum(len(v) for v in by_list.values())
                                header += " (dry-run)"
                            print(header)
                            for lst, names in by_list.items():
                                print(f"  {lst}: {len(names)}")
                                for nm in names:
                                    print(f"    - {nm}")
                            # In verbose + dry-run, do not upsert; in verbose + real-run, upsert silently
                            if not args.dry_run:
                                to_write = out
                                if args.filter_characters:
                                    if getattr(args, "characters_relaxed", False):
                                        to_write = [
                                            it for it in out
                                            if isinstance(it, dict) and (
                                                it.get("isCharacter") is True or it.get("_from_character_group") is True
                                            )
                                        ]
                                    else:
                                        to_write = [it for it in out if isinstance(it, dict) and (it.get("isCharacter") is True)]
                                n, u = store.upsert_raw(to_write)
                                total_new += n
                                total_upd += u
                        else:
                            if args.dry_run:
                                n_items = len(out)
                                if args.filter_characters:
                                    try:
                                        if getattr(args, "characters_relaxed", False):
                                            n_items = sum(1 for it in out if isinstance(it, dict) and (it.get("isCharacter") is True or it.get("_from_character_group") is True))
                                        else:
                                            n_items = sum(1 for it in out if isinstance(it, dict) and (it.get("isCharacter") is True))
                                    except Exception:
                                        pass
                                total_would += n_items
                                print(f"keyword={qq!r} page {page}: would upsert {n_items} items (dry-run)")
                            else:
                                # apply filter when persisting
                                to_write = out
                                if args.filter_characters:
                                    if getattr(args, "characters_relaxed", False):
                                        to_write = [
                                            it for it in out
                                            if isinstance(it, dict) and (
                                                it.get("isCharacter") is True or it.get("_from_character_group") is True
                                            )
                                        ]
                                    else:
                                        to_write = [it for it in out if isinstance(it, dict) and (it.get("isCharacter") is True)]
                                n, u = store.upsert_raw(to_write)
                                total_new += n
                                total_upd += u
                                print(f"keyword={qq!r} page {page}: upserted {n} new, {u} updated")
                    # Respect page limit early to avoid unnecessary paging
                    if not args.until_empty and page >= max(local_pages_limit, 1):
                        break
                    # handle nextCursor
                    nc = container.get("nextCursor") if isinstance(container, dict) else None
                    if nc is None and isinstance(data, dict):
                        nc = data.get("nextCursor")
                    if isinstance(nc, (int, str)):
                        if nc == next_cursor_token:
                            break
                        next_cursor_token = nc
                    else:
                        next_cursor_token = None
                        if not args.until_empty:
                            break
                    if args.max_no_progress_pages > 0 and no_progress >= args.max_no_progress_pages:
                        print(f"keyword={qq!r}: stopping after {no_progress} no-progress pages")
                        break
            if args.dry_run:
                print(f"Done (dry-run). Would upsert {total_would} items across {len(keys)} keywords.")
                return
            print(f"Done. Total upserts: {total_new} new, {total_upd} updated across {len(keys)} keywords")
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
    elif args.cmd == "edges-report":
        import pandas as pd
        from collections import Counter
        try:
            from .pdb_edges import PdbEdgesStorage as _PdbEdges
        except Exception:
            print("Edges storage not available.")
            return
        es = _PdbEdges()
        path = es.edges_path
        if not path.exists():
            print(f"No edges parquet found at {path}")
            return
        df = pd.read_parquet(path)
        if df.empty:
            print("Edges parquet is empty.")
            return
        total_edges = len(df)
        nodes = set(df['from_pid'].astype(int).tolist() + df['to_pid'].astype(int).tolist())
        print(f"Edges: {total_edges}; Unique nodes: {len(nodes)}")
        out_deg = Counter(df['from_pid'].astype(int).tolist())
        in_deg = Counter(df['to_pid'].astype(int).tolist())
        k = max(getattr(args, 'top', 10), 1)
        if out_deg:
            print("Top out-degree:")
            for pid, deg in out_deg.most_common(k):
                print(f"  {pid}: {deg}")
        if in_deg:
            print("Top in-degree:")
            for pid, deg in in_deg.most_common(k):
                print(f"  {pid}: {deg}")
    elif args.cmd == "edges-analyze":
        import pandas as pd
        from collections import defaultdict, Counter
        try:
            from .pdb_edges import PdbEdgesStorage as _PdbEdges
        except Exception:
            print("Edges storage not available.")
            return
        es = _PdbEdges()
        path = es.edges_path
        if not path.exists():
            print(f"No edges parquet found at {path}")
            return
        df = pd.read_parquet(path)
        if df.empty:
            print("Edges parquet is empty.")
            return
        from_nodes = df['from_pid'].astype(int).tolist()
        to_nodes = df['to_pid'].astype(int).tolist()
        nodes = set(from_nodes + to_nodes)
        # Build undirected adjacency for component discovery
        adj: dict[int, set[int]] = defaultdict(set)
        for u, v in zip(from_nodes, to_nodes):
            if u == v:
                continue
            adj[u].add(v)
            adj[v].add(u)
        # BFS/DFS to find components
        visited: set[int] = set()
        components: list[set[int]] = []
        for n in nodes:
            if n in visited:
                continue
            comp: set[int] = set()
            stack = [n]
            visited.add(n)
            while stack:
                cur = stack.pop()
                comp.add(cur)
                for nx in adj.get(cur, ()):  # neighbors
                    if nx not in visited:
                        visited.add(nx)
                        stack.append(nx)
            components.append(comp)
        # Degree counters
        out_deg = Counter(from_nodes)
        in_deg = Counter(to_nodes)
        deg = Counter()
        for k, v in out_deg.items():
            deg[k] += v
        for k, v in in_deg.items():
            deg[k] += v
        print(f"Nodes: {len(nodes)}; Edges: {len(df)}; Components: {len(components)}")
        # Show top components
        topN = max(getattr(args, 'top', 3), 1)
        per = max(getattr(args, 'per_component_top', 5), 1)
        comps_sorted = sorted(components, key=lambda c: -len(c))[:topN]
        for idx, comp in enumerate(comps_sorted, start=1):
            sub_nodes = comp
            # edges within component (undirected unique pairs)
            comp_edges = 0
            # Count undirected edges by scanning df rows
            comp_set = sub_nodes
            for u, v in zip(from_nodes, to_nodes):
                if u in comp_set and v in comp_set and u != v:
                    comp_edges += 1
            # summarize degrees
            comp_deg = {n: deg.get(n, 0) for n in sub_nodes}
            top_nodes = sorted(comp_deg.items(), key=lambda x: -x[1])[:per]
            print(f"Component {idx}: size={len(sub_nodes)} edges={comp_edges}")
            if top_nodes:
                print("  Top degrees:")
                for n, d in top_nodes:
                    print(f"    {n}: {int(d)}")
    elif args.cmd == "edges-export":
        import pandas as pd
        from collections import defaultdict, Counter
        from pathlib import Path
        try:
            from .pdb_edges import PdbEdgesStorage as _PdbEdges
        except Exception:
            print("Edges storage not available.")
            return
        es = _PdbEdges()
        path = es.edges_path
        if not path.exists():
            print(f"No edges parquet found at {path}")
            return
        df = pd.read_parquet(path)
        if df.empty:
            print("Edges parquet is empty.")
            return
        from_nodes = df['from_pid'].astype(int).tolist()
        to_nodes = df['to_pid'].astype(int).tolist()
        nodes = set(from_nodes + to_nodes)
        # adjacency (undirected) for component id assignment
        adj: dict[int, set[int]] = defaultdict(set)
        for u, v in zip(from_nodes, to_nodes):
            if u == v:
                continue
            adj[u].add(v)
            adj[v].add(u)
        comp_id: dict[int, int] = {}
        cid = 0
        for n in nodes:
            if n in comp_id:
                continue
            cid += 1
            stack = [n]
            comp_id[n] = cid
            while stack:
                cur = stack.pop()
                for nx in adj.get(cur, ()):  # neighbors
                    if nx not in comp_id:
                        comp_id[nx] = cid
                        stack.append(nx)
        # degree stats
        out_deg = Counter(from_nodes)
        in_deg = Counter(to_nodes)
        deg = Counter()
        for k, v in out_deg.items():
            deg[k] += v
        for k, v in in_deg.items():
            deg[k] += v
        rows = []
        for n in nodes:
            rows.append({
                "pid": int(n),
                "component": int(comp_id.get(n, 0)),
                "out_degree": int(out_deg.get(n, 0)),
                "in_degree": int(in_deg.get(n, 0)),
                "degree": int(deg.get(n, 0)),
            })
        out = pd.DataFrame(rows)
        outp = Path(getattr(args, 'out', 'data/bot_store/pdb_profile_edges_components.parquet'))
        outp.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(outp, index=False)
        # summary
        comps = out['component'].value_counts().sort_values(ascending=False)
        print(f"Wrote {len(out)} rows to {outp}")
        if not comps.empty:
            print("Top components by size:")
            for comp, cnt in comps.head(5).items():
                print(f"  component {int(comp)}: {int(cnt)} nodes")
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
    elif args.cmd == "compact-raw":
        import pandas as pd
        from pathlib import Path
        import shutil
        from .pdb_cid import cid_from_object

        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        if df.empty:
            print("Raw parquet is empty; nothing to compact.")
            return
        seen_new: set[str] = set()
        rows: list[dict] = []
        old_cids = set()
        kept = 0
        for _, row in df.iterrows():
            old_cid = str(row.get("cid"))
            old_cids.add(old_cid)
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            base_obj = obj
            if isinstance(obj, dict):
                try:
                    base_obj = {k: v for k, v in obj.items() if not (isinstance(k, str) and k.startswith("_"))}
                except Exception:
                    base_obj = obj
            new_cid = cid_from_object(base_obj)
            if new_cid in seen_new:
                continue
            seen_new.add(new_cid)
            rows.append({"cid": new_cid, "payload_bytes": pb})
            kept += 1
        dropped = len(df) - kept
        print(f"Original rows: {len(df)}; unique old CIDs: {len(old_cids)}; unique new CIDs: {len(seen_new)}; dropped duplicates: {dropped}")
        if args.dry_run:
            return
        out_path = Path(args.out) if args.out else (raw_path.parent / "pdb_profiles_compacted.parquet")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_df = pd.DataFrame(rows)
        out_df.to_parquet(out_path, index=False)
        print(f"Wrote compacted parquet: {out_path}")
        if args.replace:
            backup = raw_path.with_suffix(".bak")
            shutil.move(str(raw_path), str(backup))
            shutil.move(str(out_path), str(raw_path))
            print(f"Replaced {raw_path} (backup at {backup})")
            if args.drop_vectors_on_replace:
                vec_path = store.vec_path
                if vec_path.exists():
                    vec_backup = vec_path.with_suffix(".bak")
                    shutil.move(str(vec_path), str(vec_backup))
                    print(f"Moved vectors parquet to backup: {vec_backup}. Re-embed to rebuild.")
    elif args.cmd == "diagnose-query":
        import pandas as pd
        store = PdbStorage()
        raw_path = store.raw_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        sources = {s.strip() for s in (args.sources or '').split(',') if s.strip()}
        rows = []
        for _, row in df.iterrows():
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            if sources and obj.get("_source") not in sources:
                continue
            q = obj.get("_query")
            if args.exact is not None:
                if str(q) != str(args.exact):
                    continue
            elif args.contains is not None:
                if not (isinstance(q, str) and (args.contains.lower() in q.lower())):
                    continue
            # collect
            pid = None
            for k in ("id", "profileId", "profileID", "profile_id"):
                v = obj.get(k)
                if isinstance(v, int):
                    pid = v
                    break
                if isinstance(v, str):
                    try:
                        pid = int(v)
                        break
                    except Exception:
                        pass
            rows.append({
                "pid": pid,
                "cid": row.get("cid"),
                "page": obj.get("_page"),
                "cursor": obj.get("_nextCursor"),
                "source_list": obj.get("_source_list"),
                "source": obj.get("_source"),
                "query": q,
            })
        if not rows:
            print("No matching rows found.")
            return
        out = pd.DataFrame(rows)
        total = len(out)
        unique_pids = out['pid'].dropna().nunique()
        pages = out['page'].value_counts().sort_index()
        cursors = out['cursor'].value_counts().head(min(args.limit, 50))
        print(f"Rows: {total}; unique profile IDs: {unique_pids}")
        if not pages.empty:
            print("Pages -> counts:")
            for p, c in pages.items():
                print(f"  {int(p) if pd.notna(p) else -1}: {int(c)}")
        if not cursors.empty:
            print("Top cursors:")
            for cur, c in cursors.items():
                print(f"  {cur}: {int(c)}")
        # potential duplication diagnosis
        dup_pages = out.groupby(['page']).pid.nunique().reset_index(name='uniq_pids').sort_values('page')
        if not dup_pages.empty:
            print("Unique pids per page (sample):")
            for _, r in dup_pages.head(min(len(dup_pages), args.limit)).iterrows():
                print(f"  page {int(r['page']) if pd.notna(r['page']) else -1}: {int(r['uniq_pids'])}")
    # end of diagnose-query

    elif args.cmd == "coverage":
        import pandas as pd
        store = PdbStorage()
        raw_path = store.raw_path
        vec_path = store.vec_path
        if not raw_path.exists():
            print(f"Missing raw parquet: {raw_path}")
            return
        df = pd.read_parquet(raw_path)
        total_rows = len(df)
        unique_cids = df['cid'].astype(str).nunique() if 'cid' in df.columns else 0
        # Identify v1 profiles by _source flag in payload
        v1_ids: set[int] = set()
        seen_ids: set[int] = set()
        for _, row in df.iterrows():
            pb = row.get("payload_bytes")
            try:
                obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
            except Exception:
                obj = None
            if not isinstance(obj, dict):
                continue
            # seen ids from v2 search/top and related entries that include a profile id
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
            if isinstance(pid, int):
                seen_ids.add(pid)
            if obj.get("_source") == "v1_profile":
                vp = obj.get("_profile_id")
                if isinstance(vp, int):
                    v1_ids.add(vp)
                elif isinstance(vp, str):
                    try:
                        v1_ids.add(int(vp))
                    except Exception:
                        pass
        v1_count = len(v1_ids)
        # vectors coverage
        vec_count = 0
        if vec_path.exists():
            try:
                vdf = pd.read_parquet(vec_path)
                vec_count = vdf.dropna(subset=["vector"]).shape[0] if "vector" in vdf.columns else len(vdf)
            except Exception:
                pass
        missing_v1 = sorted(list(seen_ids - v1_ids))
        print(f"Raw rows: {total_rows}; unique CIDs: {unique_cids}; vectors: {vec_count}; v1_profiles: {v1_count}; seen_ids: {len(seen_ids)}")
        if missing_v1:
            print(f"Missing v1 profiles (sample {min(args.sample, len(missing_v1))} of {len(missing_v1)}): {missing_v1[:max(0, min(args.sample, len(missing_v1)))]}")

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
            # Collect seed IDs from flags
            if args.seed_ids:
                try:
                    seeds.extend([int(x.strip()) for x in args.seed_ids.split(',') if x.strip()])
                except Exception:
                    print("Invalid --seed-ids; expected comma-separated integers")
                    return
            if getattr(args, "seed_file", None):
                try:
                    txt = _Path(args.seed_file).read_text(encoding="utf-8")
                    for tok in txt.replace('\n', ',').replace('\t', ',').replace(' ', ',').split(','):
                        tok = tok.strip()
                        if not tok:
                            continue
                        try:
                            seeds.append(int(tok))
                        except Exception:
                            pass
                except Exception as e:
                    print(f"Failed to read --seed-file: {e}")
                    return
            # Dedupe preserving order
            if seeds:
                seen_tmp = set()
                seeds = [x for x in seeds if (x not in seen_tmp and not seen_tmp.add(x))]
            # If still empty, infer from raw parquet
            if not seeds:
                if not raw_path.exists():
                    print(f"Missing raw parquet: {raw_path}. Provide --seed-ids/--seed-file or ingest some data first.")
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
            # Helper: robust profile id extraction
            def _get_pid(obj: dict) -> int | None:
                if not isinstance(obj, dict):
                    return None
                for k in ("id", "profileId", "profileID", "profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        return v
                    if isinstance(v, str):
                        try:
                            return int(v)
                        except Exception:
                            pass
                return None
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
                        vid = _get_pid(it)
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
                        prev_cursor: int | str | None = None
                        seen_ident: set[str] = set()
                        no_progress = 0
                        while True:
                            page += 1
                            params = {"keyword": name, "limit": args.limit}
                            if next_cursor is not None:
                                params["nextCursor"] = next_cursor
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
                            subcats: list[dict] = []
                            for fname, val in container.items():
                                if fname == "subcategories":
                                    if isinstance(val, list) and val:
                                        for it in val:
                                            if isinstance(it, dict):
                                                subcats.append(it)
                                                if allowed_lists is None or fname in allowed_lists:
                                                    out.append({**it, "_source": "v2_search_top_by_name", "_source_list": fname, "_query": name, "_page": page, "_nextCursor": next_cursor})
                                    continue
                                # Heuristic: capture subcategory-like objects inside 'profiles'
                                if fname == "profiles" and isinstance(val, list):
                                    for it in val:
                                        if isinstance(it, dict) and (it.get("subcatID") is not None or it.get("subcategory") is not None):
                                            subcats.append(it)
                                if allowed_lists is not None and fname not in allowed_lists:
                                    continue
                                if isinstance(val, list) and val:
                                    for it in val:
                                        if isinstance(it, dict):
                                            out.append({**it, "_source": "v2_search_top_by_name", "_source_list": fname, "_query": name, "_page": page, "_nextCursor": next_cursor})
                            # Optionally expand subcategories via related to surface profiles/characters
                            if args.expand_subcategories and subcats:
                                def _is_character_group(item: dict) -> bool:
                                    try:
                                        if item.get("isCharacter") is True:
                                            return True
                                        subn = item.get("subcategory")
                                        if isinstance(subn, str) and ("character" in subn.lower() or "characters" in subn.lower()):
                                            return True
                                    except Exception:
                                        pass
                                    return False
                                candidates = [it for it in subcats if isinstance(it, dict) and _is_character_group(it)] or [it for it in subcats if isinstance(it, dict)]
                                candidates = candidates[: max(getattr(args, "expand_max", 5), 0)]
                                expanded: list[dict] = []
                                for sc in candidates:
                                    sid = None
                                    try:
                                        v = sc.get("id") if isinstance(sc, dict) else None
                                        if isinstance(v, int):
                                            sid = v
                                        elif isinstance(v, str):
                                            sid = int(v)
                                    except Exception:
                                        sid = None
                                    if not isinstance(sid, int):
                                        continue
                                    try:
                                        data_rel = await client_v2.fetch_json(f"profiles/{sid}/related")
                                    except Exception:
                                        continue
                                    rel_items: list[dict] = []
                                    if isinstance(data_rel, dict):
                                        cont2 = data_rel.get("data") or data_rel
                                        if isinstance(cont2, dict):
                                            rp = cont2.get("relatedProfiles")
                                            if isinstance(rp, list):
                                                for it in rp:
                                                    if isinstance(it, dict):
                                                        rel_items.append({**it, "_source_list": "profiles"})
                                            else:
                                                for k2, v2 in cont2.items():
                                                    if isinstance(v2, list):
                                                        if allowed_lists is not None and (k2 not in allowed_lists and k2 != "relatedProfiles"):
                                                            continue
                                                        for it in v2:
                                                            if isinstance(it, dict):
                                                                rel_items.append({**it, "_source_list": ("profiles" if k2 == "relatedProfiles" else k2)})
                                    from_char_group = False
                                    try:
                                        subn = sc.get("subcategory") if isinstance(sc, dict) else None
                                        if sc.get("isCharacter") is True or (isinstance(subn, str) and ("character" in subn.lower() or "characters" in subn.lower())):
                                            from_char_group = True
                                    except Exception:
                                        from_char_group = False
                                    for it in rel_items:
                                        if not isinstance(it, dict):
                                            continue
                                        if args.filter_characters:
                                            ch = it.get("isCharacter")
                                            if not (ch is True or (getattr(args, "characters_relaxed", False) and from_char_group is True)):
                                                continue
                                        expanded.append({**it, "_source": "v2_related_from_subcategory_by_name", "_query": name, "_page": page, "_expanded_from": sid, "_from_character_group": from_char_group})
                                if expanded:
                                    out = out + expanded
                            # compute identity keys per item for progress tracking
                            page_keys: list[str] = []
                            for it2 in out:
                                oid = None
                                if isinstance(it2, dict):
                                    for kk in ("id", "profileId", "profileID", "profile_id"):
                                        vv = it2.get(kk)
                                        if isinstance(vv, (int, str)):
                                            oid = str(vv)
                                            break
                                sl = it2.get("_source_list") if isinstance(it2, dict) else None
                                page_keys.append(f"{sl}:{oid}")
                            new_keys = [pk for pk in page_keys if pk not in seen_ident]
                            if new_keys:
                                seen_ident.update(new_keys)
                                no_progress = 0
                            else:
                                no_progress += 1
                            if not out:
                                if args.until_empty:
                                    break
                            else:
                                if args.dry_run:
                                    print(f"name={name!r} page {page}: would upsert {len(out)} items (dry-run)")
                                else:
                                    to_write = out
                                    if args.filter_characters:
                                        if getattr(args, "characters_relaxed", False):
                                            to_write = [
                                                it for it in out
                                                if isinstance(it, dict) and (
                                                    it.get("isCharacter") is True or it.get("_from_character_group") is True
                                                )
                                            ]
                                        else:
                                            to_write = [it for it in out if isinstance(it, dict) and (it.get("isCharacter") is True)]
                                    new, upd = store.upsert_raw(to_write)
                                    print(f"name={name!r} page {page}: upserted {new} new, {upd} updated from search/top")
                            nc = container.get("nextCursor") if isinstance(container, dict) else None
                            if isinstance(nc, (int, str)):
                                if nc == prev_cursor:
                                    break
                                prev_cursor = nc
                                next_cursor = nc
                            else:
                                next_cursor = None
                                if not args.until_empty:
                                    break
                            if args.max_no_progress_pages > 0 and no_progress >= args.max_no_progress_pages:
                                print(f"name={name!r}: stopping after {no_progress} no-progress pages")
                                break
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
    elif args.cmd == "scan-all":
        import pandas as pd
        import json as _json
        from pathlib import Path as _Path

        async def _run():
            client_v2 = _make_client(args)
            # optional edges storage
            try:
                from .pdb_edges import PdbEdgesStorage as _PdbEdges
                edges_store = _PdbEdges()
            except Exception:
                edges_store = None
            # Optional v1 client for scraping
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
            seen_ids: set[int] = set()
            # Load persistent state if requested
            import json as _json
            from pathlib import Path as _Path
            state_path = _Path(args.state_file) if args.use_state else None
            state: dict[str, object] = {}
            processed_related: set[int] = set()
            processed_names: set[str] = set()
            processed_sweeps: set[str] = set()
            v1_failed: set[int] = set()
            if args.use_state:
                if args.state_reset:
                    pass
                else:
                    try:
                        if state_path and state_path.exists():
                            state = _json.loads(state_path.read_text(encoding="utf-8"))
                            for k, tgt in (
                                ("processed_related_pids", processed_related),
                                ("processed_names", processed_names),
                                ("processed_sweep_tokens", processed_sweeps),
                                ("v1_failed_pids", v1_failed),
                            ):
                                vals = state.get(k)
                                if isinstance(vals, list):
                                    for x in vals:
                                        try:
                                            if k == "processed_names" or k == "processed_sweep_tokens":
                                                if isinstance(x, str):
                                                    tgt.add(x)
                                            else:
                                                tgt.add(int(x))
                                        except Exception:
                                            pass
                    except Exception:
                        pass
            def _save_state() -> None:
                if not args.use_state or not state_path:
                    return
                try:
                    state_path.parent.mkdir(parents=True, exist_ok=True)
                    blob = {
                        "processed_related_pids": sorted(list(processed_related)),
                        "processed_names": sorted(list(processed_names)),
                        "processed_sweep_tokens": sorted(list(processed_sweeps)),
                        "v1_failed_pids": sorted(list(v1_failed)),
                    }
                    state_path.write_text(_json.dumps(blob), encoding="utf-8")
                except Exception:
                    pass
            # Helper for robust profile id extraction
            def _get_pid(obj: dict) -> int | None:
                if not isinstance(obj, dict):
                    return None
                for k in ("id", "profileId", "profileID", "profile_id"):
                    v = obj.get(k)
                    if isinstance(v, int):
                        return v
                    if isinstance(v, str):
                        try:
                            return int(v)
                        except Exception:
                            pass
                return None
            # Build initial seen set from raw parquet if available
            if raw_path.exists():
                df = pd.read_parquet(raw_path)
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
                        pid = obj.get("_profile_id") if isinstance(obj.get("_profile_id"), int) else None
                    if pid is None:
                        if obj.get("_source_list") == "profiles" or "profiles" in str(obj.get("_source_list", "")):
                            pid = _get_pid(obj)
                    if isinstance(pid, int):
                        seen_ids.add(pid)
            # Seeds
            frontier: list[int] = []
            # Seeds from CLI and/or file
            try:
                if args.seed_ids:
                    frontier.extend([int(x.strip()) for x in args.seed_ids.split(',') if x.strip()])
                if getattr(args, "seed_file", None):
                    txt = _Path(args.seed_file).read_text(encoding="utf-8")
                    for tok in txt.replace('\n', ',').replace('\t', ',').replace(' ', ',').split(','):
                        tok = tok.strip()
                        if not tok:
                            continue
                        try:
                            frontier.append(int(tok))
                        except Exception:
                            pass
                # dedupe preserving order
                if frontier:
                    seen_tmp = set()
                    frontier = [x for x in frontier if (x not in seen_tmp and not seen_tmp.add(x))]
            except Exception:
                print("Invalid seed inputs; expected integers in --seed-ids/--seed-file")
                return
            # Fallback to inferred seeds when none specified
            if not frontier:
                if not seen_ids:
                    print("No seeds found from raw parquet; provide --seed-ids/--seed-file or ingest some data first.")
                    return
                frontier = list(seen_ids)[: max(args.initial_frontier_size, 1)]

            allowed_lists = None
            if args.only_profiles:
                allowed_lists = {"profiles"}
            elif args.lists:
                allowed_lists = {s.strip() for s in args.lists.split(',') if s.strip()}

            total_related_upserts = 0
            total_search_upserts = 0
            total_new_ids = 0
            scraped_v1 = 0

            def _extract_name_list(items: list[dict]) -> list[str]:
                names: list[str] = []
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    for k in ("name", "title", "display_name", "username"):
                        v = it.get(k)
                        if isinstance(v, str) and v:
                            names.append(v)
                            break
                # dedupe & truncate
                out = []
                seen = set()
                for n in names:
                    if n in seen:
                        continue
                    seen.add(n)
                    out.append(n)
                return out

            async def _do_search_top_by_name(name: str) -> tuple[int, set[int]]:
                next_cursor = 0
                page = 0
                upserts = 0
                newids: set[int] = set()
                prev_cursor: int | str | None = None
                next_token: int | str | None = None
                seen_ident: set[str] = set()
                no_progress = 0
                while True:
                    page += 1
                    params = {"keyword": name, "limit": args.limit}
                    if next_token is not None:
                        params["nextCursor"] = next_token
                    try:
                        data = await client_v2.fetch_json("search/top", params)
                    except Exception as e:
                        print(f"search-top failed for name={name!r} page {page}: {e}")
                        break
                    container = data.get("data") if isinstance(data, dict) else None
                    if not isinstance(container, dict):
                        break
                    out = []
                    subcats: list[dict] = []
                    for fname, val in container.items():
                        if fname == "subcategories":
                            if isinstance(val, list) and val:
                                for it in val:
                                    if isinstance(it, dict):
                                        subcats.append(it)
                                        if allowed_lists is None or fname in allowed_lists:
                                            out.append({**it, "_source": "v2_search_top_by_name", "_source_list": fname, "_query": name, "_page": page, "_nextCursor": next_token})
                            continue
                        if fname == "profiles" and isinstance(val, list):
                            for it in val:
                                if isinstance(it, dict) and (it.get("subcatID") is not None or it.get("subcategory") is not None):
                                    subcats.append(it)
                        if allowed_lists is not None and fname not in allowed_lists:
                            continue
                        if isinstance(val, list) and val:
                            for it in val:
                                if isinstance(it, dict):
                                    out.append({**it, "_source": "v2_search_top_by_name", "_source_list": fname, "_query": name, "_page": page, "_nextCursor": next_token})
                                    vid = _get_pid(it)
                                    if isinstance(vid, int) and vid not in seen_ids:
                                        newids.add(vid)
                    # Expand subcategories if requested
                    if args.expand_subcategories and subcats:
                        def _is_character_group(item: dict) -> bool:
                            try:
                                if item.get("isCharacter") is True:
                                    return True
                                subn = item.get("subcategory")
                                if isinstance(subn, str) and ("character" in subn.lower() or "characters" in subn.lower()):
                                    return True
                            except Exception:
                                pass
                            return False
                        candidates = [it for it in subcats if isinstance(it, dict) and _is_character_group(it)] or [it for it in subcats if isinstance(it, dict)]
                        candidates = candidates[: max(getattr(args, "expand_max", 5), 0)]
                        expanded: list[dict] = []
                        for sc in candidates:
                            sid = None
                            try:
                                v = sc.get("id") if isinstance(sc, dict) else None
                                if isinstance(v, int):
                                    sid = v
                                elif isinstance(v, str):
                                    sid = int(v)
                            except Exception:
                                sid = None
                            if not isinstance(sid, int):
                                continue
                            try:
                                data_rel = await client_v2.fetch_json(f"profiles/{sid}/related")
                            except Exception:
                                continue
                            rel_items: list[dict] = []
                            if isinstance(data_rel, dict):
                                cont2 = data_rel.get("data") or data_rel
                                if isinstance(cont2, dict):
                                    rp = cont2.get("relatedProfiles")
                                    if isinstance(rp, list):
                                        for it in rp:
                                            if isinstance(it, dict):
                                                rel_items.append({**it, "_source_list": "profiles"})
                                    else:
                                        for k2, v2 in cont2.items():
                                            if isinstance(v2, list):
                                                if allowed_lists is not None and (k2 not in allowed_lists and k2 != "relatedProfiles"):
                                                    continue
                                                for it in v2:
                                                    if isinstance(it, dict):
                                                        rel_items.append({**it, "_source_list": ("profiles" if k2 == "relatedProfiles" else k2)})
                                    # infer character-group from related composition
                                    ch_total = sum(1 for it in rel_items if isinstance(it, dict))
                                    ch_count = sum(1 for it in rel_items if isinstance(it, dict) and it.get("isCharacter") is True)
                                    inferred_char_group = False
                                    try:
                                        if ch_total > 0 and ch_count >= 2 and (ch_count / max(ch_total, 1)) >= 0.3:
                                            inferred_char_group = True
                                    except Exception:
                                        inferred_char_group = False
                                    # label/flag heuristic on the subcategory itself
                                    from_char_group_label = False
                                    try:
                                        subn = sc.get("subcategory") if isinstance(sc, dict) else None
                                        if sc.get("isCharacter") is True or (isinstance(subn, str) and ("character" in subn.lower() or "characters" in subn.lower())):
                                            from_char_group_label = True
                                    except Exception:
                                        from_char_group_label = False
                                    from_char_group_final = bool(from_char_group_label or inferred_char_group)
                                    # annotate and filter expanded items
                                    for it in rel_items:
                                        if not isinstance(it, dict):
                                            continue
                                        if args.filter_characters:
                                            ch = it.get("isCharacter")
                                            if not (ch is True or (getattr(args, "characters_relaxed", False) and from_char_group_final is True)):
                                                continue
                                        expanded.append({**it, "_source": "v2_related_from_subcategory_by_name", "_query": name, "_page": page, "_from_character_group": from_char_group_final})
                                        vid = _get_pid(it)
                                        if isinstance(vid, int) and vid not in seen_ids:
                                            newids.add(vid)
                        if expanded:
                            out = out + expanded
                    # compute identity keys per item for progress tracking
                    page_keys: list[str] = []
                    for it2 in out:
                        oid = None
                        if isinstance(it2, dict):
                            for kk in ("id", "profileId", "profileID", "profile_id"):
                                vv = it2.get(kk)
                                if isinstance(vv, (int, str)):
                                    oid = str(vv)
                                    break
                        sl = it2.get("_source_list") if isinstance(it2, dict) else None
                        page_keys.append(f"{sl}:{oid}")
                    new_keys = [pk for pk in page_keys if pk not in seen_ident]
                    if new_keys:
                        seen_ident.update(new_keys)
                        no_progress = 0
                    else:
                        no_progress += 1
                    if not out:
                        if args.until_empty:
                            break
                    else:
                        if args.dry_run:
                            upserts += len(out)
                            print(f"name={name!r} page {page}: would upsert {len(out)} items (dry-run)")
                        else:
                            n, u = store.upsert_raw(out)
                            upserts += n + u
                            print(f"name={name!r} page {page}: upserted {n} new, {u} updated from search/top")
                    nc = container.get("nextCursor") if isinstance(container, dict) else None
                    if isinstance(nc, (int, str)):
                        if nc == prev_cursor:
                            break
                        prev_cursor = nc
                        next_token = nc
                    else:
                        next_token = None
                        if not args.until_empty:
                            break
                    if args.max_no_progress_pages > 0 and no_progress >= args.max_no_progress_pages:
                        print(f"name={name!r}: stopping after {no_progress} no-progress pages")
                        break
                    if not args.until_empty and page >= max(args.pages, 1):
                        break
                return upserts, newids

            async def _do_related(pid: int) -> tuple[int, list[dict]]:
                try:
                    data = await client_v2.fetch_json(f"profiles/{pid}/related")
                except Exception as e:
                    print(f"Fetch related failed for id={pid}: {e}")
                    return 0, []
                items: list[dict] = []
                if isinstance(data, dict):
                    container = data.get("data") or data
                    if isinstance(container, dict):
                        rp = container.get("relatedProfiles")
                        if isinstance(rp, list):
                            items = [it for it in rp if isinstance(it, dict)]
                        else:
                            for k, v in container.items():
                                if allowed_lists is not None and k not in allowed_lists:
                                    continue
                                if isinstance(v, list):
                                    for it in v:
                                        if isinstance(it, dict):
                                            items.append({**it, "_source_list": k})
                annotated = []
                for it in items:
                    if isinstance(it, dict):
                        annotated.append({**it, "_source": "v2_related", "_source_profile_id": pid})
                if not annotated:
                    return 0, []
                if args.dry_run:
                    print(f"seed id={pid}: would upsert {len(annotated)} related items")
                    return len(annotated), items
                else:
                    n, u = store.upsert_raw(annotated)
                    print(f"seed id={pid}: upserted {n} new, {u} updated related items")
                    return n + u, items

            # BFS over related + optional search-by-names
            iteration = 0
            # Treat 0 as unlimited until frontier exhausted and sweeps yield no new IDs
            max_iters = args.max_iterations
            while max_iters == 0 or iteration < max(max_iters, 1):
                iteration += 1
                print(f"Iteration {iteration}: frontier size={len(frontier)} seen={len(seen_ids)}")
                new_ids_this_iter: set[int] = set()
                next_frontier: list[int] = []
                for pid in list(frontier):
                    if args.use_state and pid in processed_related:
                        continue
                    rel_upserts, items = await _do_related(pid)
                    total_related_upserts += rel_upserts
                    # record edges from pid to related item ids
                    try:
                        if edges_store is not None and items:
                            edges: list[dict] = []
                            for it in items:
                                if isinstance(it, dict):
                                    vid = _get_pid(it)
                                    if isinstance(vid, int):
                                        edges.append({
                                            "from_pid": pid,
                                            "to_pid": vid,
                                            "relation": it.get("_source_list") or "related",
                                            "source": "v2_related"
                                        })
                            if edges:
                                edges_store.upsert_edges(edges)
                    except Exception:
                        pass
                    if args.use_state:
                        processed_related.add(pid)
                        _save_state()
                    # collect new ids from related items
                    for it in items:
                        vid = _get_pid(it) if isinstance(it, dict) else None
                        if isinstance(vid, int) and vid not in seen_ids:
                            new_ids_this_iter.add(vid)
                    if args.search_names and items:
                        names = _extract_name_list(items)
                        for name in names:
                            if args.use_state and name in processed_names:
                                continue
                            s_up, s_new = await _do_search_top_by_name(name)
                            total_search_upserts += s_up
                            new_ids_this_iter.update(s_new)
                            if args.use_state:
                                processed_names.add(name)
                                _save_state()
                # After related/name-search, optionally run sweep queries this iteration
                sweep_new_ids: set[int] = set()
                sweep_tokens = [t.strip() for t in (args.sweep_queries or '').split(',') if t.strip()]
                for tok in sweep_tokens:
                    if args.use_state and tok in processed_sweeps:
                        continue
                    page = 0
                    prev_cursor: int | str | None = None
                    next_token: int | str | None = None
                    no_progress = 0
                    while True:
                        page += 1
                        params = {"keyword": tok, "limit": args.limit}
                        if next_token is not None:
                            params["nextCursor"] = next_token
                        try:
                            data = await client_v2.fetch_json("search/top", params)
                        except Exception as e:
                            print(f"sweep token={tok!r} page {page}: fetch failed: {e}")
                            break
                        container = data.get("data") if isinstance(data, dict) else None
                        if not isinstance(container, dict):
                            break
                        out = []
                        subcats: list[dict] = []
                        prev_new_ids_count = len(sweep_new_ids)
                        for fname, val in container.items():
                            if fname == "subcategories":
                                if isinstance(val, list) and val:
                                    for it in val:
                                        if isinstance(it, dict):
                                            subcats.append(it)
                                            if allowed_lists is None or fname in allowed_lists:
                                                out.append({**it, "_source": "v2_search_top_sweep", "_source_list": fname, "_query": tok, "_page": page, "_nextCursor": next_token})
                                continue
                            if fname == "profiles" and isinstance(val, list):
                                for it in val:
                                    if isinstance(it, dict) and (it.get("subcatID") is not None or it.get("subcategory") is not None):
                                        subcats.append(it)
                            if allowed_lists is not None and fname not in allowed_lists:
                                continue
                            if isinstance(val, list) and val:
                                for it in val:
                                    if isinstance(it, dict):
                                        out.append({**it, "_source": "v2_search_top_sweep", "_source_list": fname, "_query": tok, "_page": page, "_nextCursor": next_token})
                                        vid = _get_pid(it)
                                        if isinstance(vid, int) and vid not in seen_ids:
                                            sweep_new_ids.add(vid)
                        # Expand subcategories if requested
                        if args.expand_subcategories and subcats:
                            def _is_character_group(item: dict) -> bool:
                                try:
                                    if item.get("isCharacter") is True:
                                        return True
                                    subn = item.get("subcategory")
                                    if isinstance(subn, str) and ("character" in subn.lower() or "characters" in subn.lower()):
                                        return True
                                except Exception:
                                    pass
                                return False
                            candidates = [it for it in subcats if isinstance(it, dict) and _is_character_group(it)] or [it for it in subcats if isinstance(it, dict)]
                            candidates = candidates[: max(getattr(args, "expand_max", 5), 0)]
                            expanded: list[dict] = []
                            for sc in candidates:
                                sid = None
                                try:
                                    v = sc.get("id") if isinstance(sc, dict) else None
                                    if isinstance(v, int):
                                        sid = v
                                    elif isinstance(v, str):
                                        sid = int(v)
                                except Exception:
                                    sid = None
                                if not isinstance(sid, int):
                                    continue
                                try:
                                    data_rel = await client_v2.fetch_json(f"profiles/{sid}/related")
                                except Exception:
                                    continue
                                rel_items: list[dict] = []
                                if isinstance(data_rel, dict):
                                    cont2 = data_rel.get("data") or data_rel
                                    if isinstance(cont2, dict):
                                        rp = cont2.get("relatedProfiles")
                                        if isinstance(rp, list):
                                            for it in rp:
                                                if isinstance(it, dict):
                                                    rel_items.append({**it, "_source_list": "profiles"})
                                        else:
                                            for k2, v2 in cont2.items():
                                                if isinstance(v2, list):
                                                    if allowed_lists is not None and (k2 not in allowed_lists and k2 != "relatedProfiles"):
                                                        continue
                                                    for it in v2:
                                                        if isinstance(it, dict):
                                                            rel_items.append({**it, "_source_list": ("profiles" if k2 == "relatedProfiles" else k2)})
                                for it in rel_items:
                                    if not isinstance(it, dict):
                                        continue
                                    if args.filter_characters and (it.get("isCharacter") is False):
                                        continue
                                    expanded.append({**it, "_source": "v2_related_from_subcategory_sweep", "_query": tok, "_page": page})
                                    vid = _get_pid(it)
                                    if isinstance(vid, int) and vid not in seen_ids:
                                        sweep_new_ids.add(vid)
                            if expanded:
                                out = out + expanded
                        # track progress by actual new IDs discovered this page
                        delta_new_ids = len(sweep_new_ids) - prev_new_ids_count
                        if delta_new_ids > 0:
                            no_progress = 0
                        else:
                            no_progress += 1
                        if not out:
                            if args.sweep_until_empty:
                                break
                        else:
                            if args.dry_run:
                                print(f"sweep token={tok!r} page {page}: would upsert {len(out)} items (dry-run); new_ids={delta_new_ids}")
                            else:
                                n, u = store.upsert_raw(out)
                                total_search_upserts += n + u
                                print(f"sweep token={tok!r} page {page}: upserted {n} new, {u} updated; new_ids={delta_new_ids}")
                        nc = container.get("nextCursor") if isinstance(container, dict) else None
                        if isinstance(nc, (int, str)):
                            if nc == prev_cursor:
                                break
                            prev_cursor = nc
                            next_token = nc
                        else:
                            next_token = None
                            if not args.sweep_until_empty:
                                break
                        if args.max_no_progress_pages > 0 and no_progress >= args.max_no_progress_pages:
                            print(f"sweep token={tok!r}: stopping after {no_progress} no-progress pages")
                            break
                        if not args.sweep_until_empty and page >= max(args.sweep_pages, 1):
                            break
                    if args.use_state:
                        processed_sweeps.add(tok)
                        _save_state()

                # update frontier with related/name-search discoveries and optionally sweep ids
                combined_new = set(new_ids_this_iter)
                if args.sweep_into_frontier:
                    combined_new.update(sweep_new_ids)
                newly_added = [i for i in combined_new if i not in seen_ids]
                total_new_ids += len(newly_added)
                if not newly_added:
                    print("No new profile IDs discovered in this iteration.")
                else:
                    next_frontier = newly_added
                    print(f"Discovered {len(newly_added)} new profile IDs.")
                # update seen and frontier
                seen_ids.update(combined_new)
                frontier = next_frontier
                # If no new IDs from related/name-search and (no sweep ids or not adding sweeps to frontier), stop to avoid endless sweeps
                if not newly_added and (not sweep_new_ids or not args.sweep_into_frontier):
                    print("No new IDs from related/name or sweep; stopping.")
                    break
                if not frontier and max_iters != 0:
                    print("Frontier exhausted.")
                    break

            # Optional v1 scraping of newly discovered ids (those not previously seen as v1)
            if args.scrape_v1:
                # Determine candidate ids as seen_ids that do not have v1_profile entries yet
                have_v1: set[int] = set()
                if raw_path.exists():
                    df = pd.read_parquet(raw_path)
                    for _, row in df.iterrows():
                        pb = row.get("payload_bytes")
                        try:
                            obj = orjson.loads(pb) if isinstance(pb, (bytes, bytearray)) else (json.loads(pb) if isinstance(pb, str) else (pb if isinstance(pb, dict) else None))
                        except Exception:
                            obj = None
                        if isinstance(obj, dict) and obj.get("_source") == "v1_profile":
                            v = obj.get("_profile_id")
                            if isinstance(v, int):
                                have_v1.add(v)
                to_fetch = [i for i in seen_ids if i not in have_v1]
                for pid in to_fetch:
                    if args.use_state and pid in v1_failed:
                        continue
                    try:
                        data = await client_v1.get_profile(pid)
                    except Exception as e:
                        print(f"v1 get_profile failed for id={pid}: {e}")
                        if args.use_state:
                            v1_failed.add(pid)
                            _save_state()
                        continue
                    if not isinstance(data, dict):
                        continue
                    main_obj = {**data, "_source": "v1_profile", "_profile_id": pid}
                    if args.dry_run:
                        scraped_v1 += 1
                    else:
                        n, u = store.upsert_raw([main_obj])
                        scraped_v1 += n + u
                        # success: ensure this id is not marked failed
                        if args.use_state and pid in v1_failed:
                            try:
                                v1_failed.remove(pid)
                                _save_state()
                            except Exception:
                                pass

            # Summary and optional vector ops
            if args.dry_run:
                print(
                    f"Scan-all complete (dry-run). Related_upserts~{total_related_upserts}, Search_upserts~{total_search_upserts}, New_ids~{total_new_ids}, v1_profiles~{scraped_v1}"
                )
                return
            print(
                f"Scan-all complete. Related_upserts={total_related_upserts}, Search_upserts={total_search_upserts}, New_ids={total_new_ids}, v1_profile_upserts={scraped_v1}"
            )
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
            # Final save of state
            _save_state()
        asyncio.run(_run())
    elif args.cmd == "expand-related":
        async def _run():
            client = _make_client(args)
            store = PdbStorage()
            # parse ids from --ids and optional --id-file
            id_tokens: list[str] = []
            if getattr(args, "ids", None):
                id_tokens.extend([s.strip() for s in str(args.ids).split(',') if s.strip()])
            if getattr(args, "id_file", None):
                try:
                    from pathlib import Path as _Path
                    txt = _Path(args.id_file).read_text(encoding="utf-8")
                    import csv as _csv
                    for line in txt.splitlines():
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        line = line.replace('\t', ',')
                        for row in _csv.reader([line], delimiter=',', quotechar='"'):
                            for tok in row:
                                tok = tok.strip()
                                if tok:
                                    id_tokens.append(tok)
                except Exception as e:
                    print(f"Failed to read --id-file: {e}")
                    return
            if not id_tokens:
                print("No IDs provided. Use --ids or --id-file.")
                return
            try:
                ids = [int(x) for x in id_tokens]
            except Exception:
                print("Invalid ID encountered; ensure all IDs are integers in --ids/--id-file")
                return
            # dedupe preserve order
            seen_ids_local = set()
            ids = [i for i in ids if (i not in seen_ids_local and not seen_ids_local.add(i))]
            allowed_lists = None
            if args.only_profiles:
                allowed_lists = {"profiles"}
            elif args.lists:
                allowed_lists = {s.strip() for s in args.lists.split(',') if s.strip()}
            total_new = total_upd = 0
            total_would = 0
            for sid in ids:
                try:
                    data = await client.fetch_json(f"profiles/{sid}/related")
                except Exception as e:
                    print(f"Fetch failed for id={sid}: {e}")
                    continue
                rel_items: list[dict] = []
                if isinstance(data, dict):
                    cont = data.get("data") or data
                    if isinstance(cont, dict):
                        rp = cont.get("relatedProfiles")
                        if isinstance(rp, list):
                            for it in rp:
                                if isinstance(it, dict):
                                    rel_items.append({**it, "_source_list": "profiles"})
                        else:
                            for k, v in cont.items():
                                if isinstance(v, list):
                                    if allowed_lists is not None and (k not in allowed_lists and k != "relatedProfiles"):
                                        continue
                                    for it in v:
                                        if isinstance(it, dict):
                                            rel_items.append({**it, "_source_list": ("profiles" if k == "relatedProfiles" else k)})
                if not rel_items:
                    print(f"id={sid}: no related items")
                    continue
                # character-group provenance
                ch_total = sum(1 for it in rel_items if isinstance(it, dict))
                ch_count = sum(1 for it in rel_items if isinstance(it, dict) and it.get("isCharacter") is True)
                inferred_char_group = False
                try:
                    if ch_total > 0 and ch_count >= 2 and (ch_count / max(ch_total, 1)) >= 0.3:
                        inferred_char_group = True
                except Exception:
                    inferred_char_group = False
                from_char_group_final = bool(args.force_character_group or inferred_char_group)
                out: list[dict] = []
                for it in rel_items:
                    if not isinstance(it, dict):
                        continue
                    if args.filter_characters:
                        ch = it.get("isCharacter")
                        if not (ch is True or (getattr(args, "characters_relaxed", False) and from_char_group_final is True)):
                            continue
                    out.append({**it, "_source": "v2_related", "_expanded_from": sid, "_from_character_group": from_char_group_final})
                if not out:
                    print(f"id={sid}: nothing to upsert after filtering")
                    continue
                if args.dry_run:
                    total_would += len(out)
                    print(f"id={sid}: would upsert {len(out)} items (dry-run)")
                else:
                    n, u = store.upsert_raw(out)
                    total_new += n; total_upd += u
                    print(f"id={sid}: upserted {n} new, {u} updated")
            if args.dry_run:
                print(f"Done (dry-run). Would upsert {total_would} items across {len(ids)} IDs")
                return
            print(f"Done. Total upserts: {total_new} new, {total_upd} updated across {len(ids)} IDs")
        asyncio.run(_run())


if __name__ == "__main__":
    main()
