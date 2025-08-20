# Socionics Discord Bot (v0.2)

**Updated**: 2025-08-16  
**Status**: Production-ready with privacy-first architecture

**Purpose**: Provide structured, safeguarded access to Socionics research information, educational explanations, and guided self-reflection prompts within the Discord research community.

## Bot Commands & Functionality

### Public Research Commands

### Participant Interaction  

### Administrative Commands (Role-Restricted)

## Out-of-Scope (Hard Guardrails)

## Privacy & Research Ethics Framework

### Core Safeguards

### Data Architecture (Privacy-First)
```
Discord Gateway → Command Router → Guardrail Pipeline → Intent Classifier → Tool/LLM Orchestrator → Response Formatter → Discord API
                                          ↓
                                 Audit Log (JSONL)
                                 Vector Store (Salted Hashes Only)
```

### Type Exploration Workflow (Non-Diagnostic)
1. **Self-Observation**: `/my_type_help` provides 6-dimension checklist (energy focus, information patterns, comfort cues, decision framing, discourse style, feedback sensitivity)
2. **Guided Questions**: Optional follow-up prompts (1–2 per dimension) for deeper reflection  
3. **Pattern Summary**: Bot provides neutral descriptors and suggests observational tasks
4. **Study Suggestions**: Recommends structured activities (e.g., monologue recording under different prompt categories)
5. **Resource Provision**: If users persist in seeking type assignment, bot redirects to methodology documentation

### Interpersonal Relations Framework
- **Theoretical Descriptions**: Standard relation categories (Duality, Activity, etc.) with empirical status labels
- **Evidence Transparency**: Clear marking of "unverified / limited / emerging" evidence status  
- **Research Questions**: Encouragement of testable hypotheses (e.g., "Measure coordination time on novel tasks vs. matched non-Dual pairs")

## Data Sources & Knowledge Base

**Primary Documentation Sources**:
- [`docs/intro_socionics_research.md`](../docs/intro_socionics_research.md) - Academic theory overview
- [`docs/operational_indicators.md`](../docs/operational_indicators.md) - Behavioral measurement framework  
- [`docs/annotation_protocol.md`](../docs/annotation_protocol.md) - Research methodology protocols
- [`docs/literature_review_matrix.md`](../docs/literature_review_matrix.md) - Quality-assessed research bibliography

**Vector Knowledge Base**: Embeddings from documentation with semantic search capabilities and retrieval-augmented generation for contextualized responses.

## Development Roadmap

### Current Features (v0.2) ✓
- ✓ Core command functionality with comprehensive guardrails
- ✓ Privacy-first vector storage with salted hash protection
- ✓ Multi-tier consent system with granular controls
- ✓ Rate limiting and abuse prevention mechanisms
- ✓ Comprehensive audit logging and transparency features

### Planned Enhancements 📋
- 📋 **Annotation Assistance**: Semi-automated suggestions for internal raters (`/annotator` mode)
- 📋 **Multilingual Support**: Translation capabilities with quality confidence scoring
- 📋 **Adaptive Dialogs**: Context-aware clarification questions for complex concepts  
### Use authenticated headers (recommended)

Personality DB v2 search endpoints often return limited MBTI-only lists unless you pass real browser headers with your logged-in session cookie. To unlock character results (subcategories/boards/recommendProfiles that survive `--only-profiles`), capture your headers and pass them to the CLI.

Steps:

1) In your browser while logged into personality-database.com, open DevTools → Network, load a search page, click a request to `api/v2/search/top`, and copy the Request Headers as JSON.

2) Paste into a file like `bot/headers.json`. Use `bot/headers.example.json` as a template. Ensure it includes at least:

```json
{
	"User-Agent": "Mozilla/5.0 ...",
	"Accept": "application/json, text/plain, */*",
	"Origin": "https://www.personality-database.com",
	"Referer": "https://www.personality-database.com/",
	"Cookie": "REPLACE_WITH_YOUR_SESSION_COOKIE"
}
```

Security notes:
- Do not commit your real headers/cookies. Add to your local `.git/info/exclude` if needed.
- Rotate the cookie if you suspect exposure.

Then run CLI commands with `--headers-file bot/headers.json`:

```bash
PYTHONPATH=bot/src python -m bot.pdb_cli search-keywords \
	--queries "harry potter,superman" \
	--only-profiles --filter-characters --characters-relaxed \
	--expand-subcategories --expand-boards --chase-hints \
	--headers-file bot/headers.json --verbose --dry-run
```

Remove `--dry-run` to persist and optionally add `--auto-embed --auto-index`.
- 📋 **Research Integration**: Enhanced integration with data collection workflows

### Relationship Edges and IO Optimizations

- Edges: Related-profile relationships are recorded to `data/bot_store/pdb_profile_edges.parquet` via the `related` and `scan-all` flows. Each edge captures `(from_pid, to_pid, relation, source)` and is deduped; this avoids overwriting payloads just to record a new linkage.
- Upsert optimizations: `upsert_raw` and `upsert_vectors` skip parquet writes if the incoming payload/vector is byte-identical to what’s already stored for that CID. This reduces needless IO and log noise when the same items are seen from multiple seeds.

#### edges-report
Summarize the relationship graph stored in `pdb_profile_edges.parquet`.

Usage:
```
PYTHONPATH=bot/src python -m bot.pdb_cli edges-report --top 15
```
Output includes total edges, unique node count, and the top `N` PIDs by out-degree and in-degree.
Example output:
```
Edges: 721; Unique nodes: 259
Top out-degree:
	67657: 8
	...
Top in-degree:
	67206: 18
	...
```

#### edges-analyze
Analyze the relationship graph to compute connected components (undirected) and show the largest components with top-degree nodes.

Usage:
```
PYTHONPATH=bot/src python -m bot.pdb_cli edges-analyze --top 5 --per-component-top 10
```
Output includes total nodes/edges/components and, for each of the top `N` components, its size, number of edges, and nodes with highest degrees.

#### edges-export
Export per-node component membership and degree stats to a Parquet file for downstream analysis.

Usage:
```
PYTHONPATH=bot/src python -m bot.pdb_cli edges-export \
	--out data/bot_store/pdb_profile_edges_components.parquet
```
The output has columns: `pid`, `component`, `out_degree`, `in_degree`, `degree`.
It also prints a summary of the top components by node count.

## Local Development
Install & Run Tests:
```
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
pytest -q
```

Run Bot (example):
```
export SOCIONICS_DISCORD_TOKEN=your_token_here
python -m bot.main
```

Salt Rotation (privacy maintenance):
```
export SOCIONICS_HASH_SALT=current_salt
python -m bot.maintenance NEW_LONG_RANDOM_SALT -y
```
This archives existing parquet hashed stores under data/bot_store/backup_<timestamp>/ and applies the new salt for subsequent ingests. Because original raw user IDs are never stored, historical hashed identifiers cannot be re-derived; rotation effectively resets the ingested vector corpus.

Structured JSON Logs:
```
export SOCIONICS_JSON_LOGS=true
python -m bot.main | jq '.'
```
Outputs fields: ts, level, logger, msg plus any extra contextual attributes.

## Admin & Maintenance
- Salt Rotation: `python -m bot.maintenance NEW_SALT -y` (archives old parquet stores under backup_<ts>/).
- Purge: `/purge_message <id>` removes vector + token hash entries.
- Admin Access: If SOCIONICS_ADMIN_ROLE_IDS set, role intersection required; else fallback to manage_messages permission.

## Privacy Summary
- Stored: embeddings, hashed user IDs (first 32 hex chars of salted SHA256), hashed tokens (first 16 hex chars), timestamps, channel/message IDs.
- Not Stored: raw message content, usernames, discriminators.
- Limitation: Salt rotation discards linkage to prior store (expected design for privacy); cannot migrate hashes.

## License
Inherits repository license.

## Data Ingestion CLI (PDB)

These commands support discovery-first ingestion from the Personality Database API with CID-keyed Parquet storage, embeddings, and FAISS search.

Install locally for development:
```
python -m venv .venv
source .venv/bin/activate
cd bot
pip install -e .[dev]
```

Tip: You can also run without install using the module path:
```
PYTHONPATH=bot/src python -m bot.pdb_cli <command> ...
```

### PDB API v2 Auth Setup

Most v2 endpoints require browser-like headers and an active cookie.

- Quickstart:
	```bash
	cp bot/headers.example.json bot/headers.json
	# edit bot/headers.json and paste your real Cookie and User-Agent
	```
	Then pass it with `--headers-file bot/headers.json`.

- Alternatively, create `.secrets/pdb_headers.json` and pass with `--headers "$(tr -d '\n' < .secrets/pdb_headers.json)"` or export `PDB_API_HEADERS`.

Ensure `PDB_API_BASE_URL=https://api.personality-database.com/api/v2`.

Global flags (override env):
- `--rpm`: Max requests per minute (overrides `PDB_RPM`)
- `--concurrency`: Parallel HTTP concurrency (overrides `PDB_CONCURRENCY`)
- `--timeout`: HTTP timeout in seconds (overrides `PDB_TIMEOUT_S`)
- `--base-url`: API base (overrides `PDB_API_BASE_URL`)
- `--headers`: Extra headers as JSON (merged last; overrides keys from `PDB_API_HEADERS`)

Example with globals:
```
PYTHONPATH=bot/src python -m bot.pdb_cli --rpm 120 --concurrency 8 --timeout 30 \
	search-top --pages 2 --limit 20 --query '' --auto-embed --auto-index
```
Example overriding base URL + headers:
```
PYTHONPATH=bot/src python -m bot.pdb_cli \
	--base-url https://api.personality-database.com/api/v2 \
	--headers-file bot/headers.json \
	search-top --pages 1 --limit 20 --query ''
```

Sanity check your headers quickly:
```
PYTHONPATH=bot/src python -m bot.pdb_cli auth-check --keyword "harry potter" --limit 10 --pages 1
```
If the output shows only `profiles:16` with `chars_in_profiles=0` and no `subcategories`/`boards`, you likely need to update cookies.

### follow-hot
Resolves trending hot queries via v2 `search/top`. Upserts list-valued fields (e.g., `profiles`) and supports pagination. When `--expand-subcategories` is used, items from `profiles/{id}/related` treat `relatedProfiles` as `profiles` so `--only-profiles` retains expanded results.

Flags:
- `--limit`: Max results per page per query (default 10)
- `--max-keys`: Max number of hot query keys to follow (default 10)
- `--pages`: Number of pages to fetch via `nextCursor` for each key (default 1)
- `--until-empty`: Keep paging per key until an empty page
- `--next-cursor`: Starting `nextCursor` (default 0)
- `--auto-embed`: Run embedding after ingestion
- `--auto-index`: Rebuild FAISS index after ingestion (implies `--auto-embed`)
- `--index-out`: Index output path
- `--lists`: Comma-separated list names to upsert (e.g., `profiles,boards`)
- `--only-profiles`: Shortcut for `--lists profiles`
- `--expand-subcategories`, `--expand-max`: Expand `subcategories` via related to surface actual profiles (maps `relatedProfiles` → `profiles`)
- `--expand-boards`, `--boards-max`: For board hits, run additional `search/top` calls on board names and merge their list results (bounded by `--boards-max` per page)
- `--chase-hints`, `--hints-max`: If payloads contain hint-like terms, issue `search/top` for those hint terms and merge results (bounded by `--hints-max` per page)
- `--filter-characters`: Keep only character entries when present
- `--characters-relaxed`: When filtering characters, allow expanded results marked via provenance
- `--force-character-group`: Treat expanded subcategories as character groups for relaxed filtering
- `--verbose`: Print entity names per page per key
- `--dry-run`: Preview results without writing/upserting or embedding/indexing

Example:
```
PYTHONPATH=bot/src PDB_CACHE=1 PDB_API_BASE_URL=https://api.personality-database.com/api/v2 \
PDB_API_HEADERS='{"User-Agent":"Mozilla/5.0 ...","Referer":"https://www.personality-database.com/","Origin":"https://www.personality-database.com","Cookie":"X-Lang=en-US; ..."}' \
python -m bot.pdb_cli --rpm 90 --concurrency 6 --timeout 25 \
	follow-hot --max-keys 15 --limit 20 --pages 3 --auto-embed --auto-index --index-out data/bot_store/pdb_faiss.index
```

Dry-run (no writes):
```
PYTHONPATH=bot/src python -m bot.pdb_cli follow-hot --only-profiles --pages 2 --limit 20 --expand-boards --boards-max 3 --chase-hints --hints-max 3 --dry-run
```

### search-top
Queries v2 `search/top` directly, with paging and list filtering. When `--expand-subcategories` is used, items from `profiles/{id}/related` treat `relatedProfiles` as `profiles` so `--only-profiles` retains expanded results.

Flags:
- `--query` / `--keyword`: Query parameter (use `--encoded` if already URL-encoded)
- `--encoded`: Treat `--query` as already URL-encoded (e.g., `Elon%2520Musk`)
- `--limit`, `--next-cursor`, `--pages`, `--until-empty`: Pagination controls
- `--auto-embed`, `--auto-index`, `--index-out`: Post-ingest vector/index ops
- `--lists`, `--only-profiles`: Restrict which list arrays to upsert
- `--verbose`: Print entity names per page (respects `--filter-characters`)
- `--expand-subcategories`, `--expand-max`: Expand `subcategories` via related to surface actual profiles
- `--expand-boards`, `--boards-max`: For board hits, expand by searching those board names and merge results
- `--chase-hints`, `--hints-max`: Chase hint-like payload terms with follow-up searches and merge results
- `--filter-characters`: Keep only character entries when present
- `--characters-relaxed`: When filtering characters, allow expanded results marked via provenance
- `--force-character-group`: Treat expanded subcategories as character groups for relaxed filtering
- `--dry-run`: Preview results without writing/upserting or embedding/indexing

Examples:
```
PYTHONPATH=bot/src python -m bot.pdb_cli search-top --query '' --only-profiles --pages 1 --limit 20 --dry-run
PYTHONPATH=bot/src python -m bot.pdb_cli search-top --query 'Elon%2520Musk' --encoded --only-profiles --pages 1 --limit 20 --dry-run
PYTHONPATH=bot/src python -m bot.pdb_cli search-top --query 'Elon Musk' --only-profiles --expand-subcategories --expand-max 5 --limit 20 --pages 1 --verbose --dry-run
PYTHONPATH=bot/src python -m bot.pdb_cli search-top --query 'Harry Potter' --only-profiles --expand-boards --boards-max 3 --chase-hints --hints-max 3 --pages 1 --limit 20 --verbose --dry-run
```

### search-keywords
Batch calls v2 `search/top` for many keywords (from `--queries` and/or `--query-file`). Supports optional expansion of `subcategories` into actual profiles via `profiles/{id}/related`. When expanding, `relatedProfiles` are treated as `profiles` so `--only-profiles` retains expanded results.

Flags:
- `--queries`, `--query-file`: Provide keywords (CSV/newline/tab separated when using file)
- `--limit`, `--pages`, `--until-empty`, `--next-cursor`: Pagination per keyword
- `--lists`, `--only-profiles`: Restrict which list arrays to upsert
- `--expand-subcategories`, `--expand-max`: Expand subcategory buckets via related to surface real profiles (default max 5)
- `--expand-boards`, `--boards-max`: For board hits, run follow-up searches on board names and merge results
- `--chase-hints`, `--hints-max`: Follow hint-like terms with additional searches and merge results
- `--filter-characters`: Keep only character entries when present
- `--characters-relaxed`: When filtering characters, allow expanded results marked via provenance
- `--force-character-group`: Treat expanded subcategories as character groups for relaxed filtering
- `--append-terms`: Comma-separated suffixes appended to each keyword (e.g., `characters,cast`)
- `--expand-characters`: Sweep A–Z and 0–9 suffix tokens for discovery
- `--expand-pages`: Pages per expanded token when `--expand-characters` is set
- `--html-fallback`: If v2 returns zero character items for a keyword, parse the public search page HTML and expand via `expand-from-url` to surface profiles that survive `--only-profiles`.
- `--render-js`: With `--html-fallback`, render the search page with a headless browser (Playwright) before parsing to capture dynamically injected results.
- `--html-limit`: Max number of profile links to follow from the HTML fallback per keyword (applies when `--html-fallback`).
- `--auto-embed`, `--auto-index`, `--index-out`: Post-ingest vector/index ops
- `--verbose`: Print entity names by list per page
- `--dry-run`: Preview without writes

Examples:
```
PYTHONPATH=bot/src python -m bot.pdb_cli \
	search-keywords --queries "Elon Musk,MrBeast,Taylor Swift" \
	--only-profiles --expand-subcategories --expand-max 5 \
	--limit 20 --pages 1 --verbose --dry-run

PYTHONPATH=bot/src python -m bot.pdb_cli \
	search-keywords --query-file data/bot_store/keywords/giant_keywords.txt \
	--only-profiles --expand-subcategories --expand-max 5 \
	--limit 20 --pages 1 --dry-run

# Expanded discovery examples
PYTHONPATH=bot/src python -m bot.pdb_cli \
		--headers-file data/bot_store/headers.json \
		search-keywords --queries "Harry Potter" \
		--only-profiles --expand-subcategories --force-character-group \
		--filter-characters --characters-relaxed \
		--append-terms "characters,cast" --limit 20 --pages 1 --dry-run

PYTHONPATH=bot/src python -m bot.pdb_cli \
		--headers-file data/bot_store/headers.json \
		search-keywords --queries "Harry Potter" \
		--only-profiles --expand-subcategories --force-character-group \
		--filter-characters --characters-relaxed \
		--expand-characters --expand-pages 1 --limit 20 --pages 1 --dry-run

# Boards/hints expansion example
PYTHONPATH=bot/src python -m bot.pdb_cli \
		--headers-file data/bot_store/headers.json \
		search-keywords --queries "superman,harry potter" \
		--only-profiles --expand-boards --boards-max 3 --chase-hints --hints-max 3 \
		--limit 15 --pages 1 --verbose --dry-run

# HTML fallback for sparse v2 results (recommended for character queries)
# Note: pass global flags (e.g., --headers-file) before the subcommand name.
PYTHONPATH=bot/src python -m bot.pdb_cli \
	--headers-file data/bot_store/headers.json \
	search-keywords --queries "superman,gandalf" \
	--only-profiles --filter-characters --characters-relaxed \
	--expand-subcategories --force-character-group \
	--html-fallback --render-js --html-limit 12 \
	--pages 1 --limit 20 --verbose --dry-run

# The HTML fallback automatically calls `expand-from-url` on the public
# search page and tags discovered rows with `--set-keyword <term>` so that
# subsequent `export-characters` can include high-signal aliases.

### hot-queries
Fetch trending search hot queries (v2) and optionally store raw items.

Flags:
- `--dry-run`: Preview without writing/upserting

Examples:
```
PYTHONPATH=bot/src python -m bot.pdb_cli \
	--base-url https://api.personality-database.com/api/v2 \
	--headers-file data/bot_store/headers.json \
	hot-queries --dry-run
```
```

### find-subcats
List subcategories returned by `search/top` for a keyword. This helps find character-group buckets to expand via `profiles/{id}/related`.

Usage:
```
PYTHONPATH=bot/src python -m bot.pdb_cli find-subcats --keyword "Harry Potter" --pages 1 --limit 40
# Example output line:
# id=123456 | name=Harry Potter Characters | isCharacterGroup=True
```
Then expand the subcategory id via:
```
PYTHONPATH=bot/src python -m bot.pdb_cli related --ids 123456
```

### coverage
Quick snapshot of ingestion and vector coverage, plus sample of missing v1 profiles.

```
PYTHONPATH=bot/src python -m bot.pdb_cli coverage --sample 10
```

### ingest-report
Summarizes items ingested via `search/top` and `follow-hot`, grouped by `_source_list` and top `_query` values.

Example:
```
PYTHONPATH=bot/src python -m bot.pdb_cli ingest-report --top-queries 10
```

### Character Utilities

Use these helpers to isolate character-like rows and build a character-only search index. Rows are considered character-like when either `isCharacter == True` or provenance `_from_character_group == True`, while obvious MBTI buckets are excluded by name.

Export character-like rows:
```
PYTHONPATH=bot/src python -m bot.pdb_cli export-characters \
	--out data/bot_store/pdb_characters.parquet --sample 10
```

Index only character rows:
```
PYTHONPATH=bot/src python -m bot.pdb_cli index-characters \
	--char-parquet data/bot_store/pdb_characters.parquet \
	--out data/bot_store/pdb_faiss_char.index
```

Search character-only index:
```
PYTHONPATH=bot/src python -m bot.pdb_cli search-faiss "elon musk" --top 10 \
	--index data/bot_store/pdb_faiss_char.index
```

Name fallback behavior:
- When v2 payloads omit names, `export-characters` derives a readable name from the profile URL slug (e.g., `/profile/1986/clark-kent-superman` → `Clark Kent Superman`). Common SEO tails like `-mbti-personality-type` are stripped.
- This improves retrieval and avoids `(unknown)` names in the character parquet.

Alias enrichment (retro-tagging) and alias-aware search:
- If you expanded from a URL and want substring searches to match aliases like "superman" or "harry potter", retro-tag `_search_keyword` on existing related rows, re-export characters, and rebuild the index.

```
# Tag related rows by seed profile id (e.g., 1986 for Superman) and source
PYTHONPATH=bot/src python -m bot.pdb_cli tag-keyword \
	--keyword superman \
	--seed-pids 1986 \
	--sources v2_related_from_url:profiles

# Re-export and rebuild index
PYTHONPATH=bot/src python -m bot.pdb_cli export-characters --out data/bot_store/pdb_characters.parquet
PYTHONPATH=bot/src python -m bot.pdb_cli index-characters --char-parquet data/bot_store/pdb_characters.parquet --out data/bot_store/pdb_faiss_char.index

# Alias-aware filtering: search-characters matches --contains against name and alt_names
PYTHONPATH=bot/src python -m bot.pdb_cli search-characters "superman" \
	--top 10 --index data/bot_store/pdb_faiss_char.index --contains superman
```

### Ingestion Cycle Script
To run a full ingestion cycle end-to-end with environment-configurable headers, rate, and concurrency, use:

```
./scripts/pdb_ingest_cycle.sh
```

Environment overrides supported by the script:
- `PDB_BASE` (default `https://api.personality-database.com/api/v2`)
- `PDB_HEADERS` (JSON for headers incl. User-Agent, Referer, Origin, Cookie)
- `PDB_RPM`, `PDB_CONCURRENCY`, `PDB_TIMEOUT_S`
- `MAX_KEYS`, `PAGES`, `LIMIT`, `INDEX_OUT`
- `ONLY_PROFILES` (non-empty to pass `--only-profiles`)
- `LISTS` (e.g., `profiles,boards` to pass `--lists`)
- `DRY_RUN` (non-empty to pass `--dry-run`)
- `UNTIL_EMPTY` (non-empty to pass `--until-empty`)

### scan-related
Traverse seeds to collect v2 related profiles, optionally search by related names, and scrape v1 profiles for the discovered IDs. Supports dry-run and post-scrape embedding/indexing.

Flags:
- `--seed-ids`: Comma-separated seed profile IDs; if omitted, seeds are inferred from current raw parquet (up to `--max-seeds`).
- `--max-seeds`: Limit number of inferred seeds (default 100).
- `--depth`: Traversal depth (currently depth=1 supported).
- `--v1-base-url`, `--v1-headers`: Override base URL and headers for v1 profile fetches.
- `--search-names`: For each related item, call v2 `search/top` using its name.
- `--limit`, `--pages`, `--until-empty`: Pagination for name search.
- `--lists`, `--only-profiles`: Restrict list arrays to upsert from name search.
- `--expand-subcategories`, `--expand-max`: Expand `subcategories` in name-search via related to surface profiles; map `relatedProfiles`→`profiles` so `--only-profiles` keeps them.
- `--filter-characters`: Keep only character items when present (applies to expansions and writes).
- `--auto-embed`, `--auto-index`, `--index-out`: Post-scrape vector/index ops.
- `--dry-run`: Preview without upserts/embedding/indexing.

Examples:
```
PYTHONPATH=bot/src python -m bot.pdb_cli scan-related --seed-ids 498239,12345 \
	--search-names --only-profiles --pages 1 --limit 20 \
	--expand-subcategories --expand-max 5 --filter-characters \
	--dry-run

# Infer seeds from parquet, collect related, then scrape v1 profiles and index
PYTHONPATH=bot/src python -m bot.pdb_cli \
	--base-url https://api.personality-database.com/api/v2 \
	--headers '{"User-Agent":"Mozilla/5.0 ...","Referer":"https://www.personality-database.com/","Origin":"https://www.personality-database.com","Cookie":"X-Lang=en-US; ..."}' \
	scan-related --max-seeds 50 --search-names --only-profiles \
	--pages 1 --limit 20 \
	--expand-subcategories --expand-max 5 --filter-characters \
	--auto-embed --auto-index
```

### scan-all
Iteratively expands coverage: pulls v2 related for a BFS-like traversal, optionally searches names via `search/top`, optionally sweeps generic tokens, and can scrape v1 profiles for discovered IDs. Supports persistent skip-state across runs and honors optional HTTP GET caching.

Key flags:
- `--max-iterations 0`: Run until exhaustion
- `--use-state` and `--state-file`: Persist and reuse progress between runs
- `--search-names`, `--limit`, `--pages`, `--until-empty`: Control name-search breadth
- `--sweep-queries a,b,c`, `--sweep-pages`, `--sweep-until-empty`, `--sweep-into-frontier`: Token sweeps to broaden discovery
- `--max-no-progress-pages 3`: Stop paging names/sweeps after N consecutive pages with no new items
- `--scrape-v1`, `--v1-base-url`, `--v1-headers`: Fetch v1 profiles for discovered IDs
- `--auto-embed`, `--auto-index`, `--index-out`: Maintain vectors and FAISS index
- `--expand-subcategories`, `--expand-max`: Expand `subcategories` in name-search and sweeps via related to surface profiles
- `--filter-characters`: Keep only character items when present (applies to expansions)

Example (stateful + cached):
```
export PDB_CACHE=1
PYTHONPATH=bot/src PDB_CACHE=1 python -m bot.pdb_cli \
	--rpm 90 --concurrency 3 --timeout 30 \
	--base-url https://api.personality-database.com/api/v2 \
	--headers "$(tr -d '\n' < .secrets/pdb_headers.json)" \
	scan-all --max-iterations 0 \
	--search-names --limit 20 --pages 1 --until-empty \
	--sweep-until-empty --sweep-into-frontier \
	--expand-subcategories --expand-max 5 \
	--filter-characters \
	--auto-embed --auto-index --index-out data/bot_store/pdb_faiss.index \
	--scrape-v1 --v1-base-url https://api.personality-database.com/api/v1 \
	--v1-headers "$(tr -d '\n' < .secrets/pdb_headers.json)" \
	--use-state
```

Or use the helper script with sensible defaults and environment overrides:

```
./scripts/pdb_scan_all_stateful.sh
```

Environment toggles for this script:
- `EXPAND_SUBCATEGORIES` (non-empty → pass `--expand-subcategories`)
- `FILTER_CHARACTERS` (non-empty → pass `--filter-characters`)
- `EXPAND_MAX` (integer → pass `--expand-max <N>`)

Troubleshooting:
- If name-search or sweeps loop without new IDs, set `--max-no-progress-pages` (default 3) to bound.
- Ensure v2 and v1 calls include browser-like headers (Referer/Origin/Cookie) and valid cookies if you get 401.
- Use `diagnose-query` to inspect pages/cursors collected for a specific keyword.
 - To start fresh with stateful scans, pass `--state-reset` or delete `data/bot_store/scan_state.json`.

---

## Summary

This Discord bot provides a comprehensive research platform for socionics studies with:

- **Privacy-First Design**: No raw content storage, salted hashes, forward secrecy
- **Research Integration**: Comprehensive PDB data pipeline with relationship analysis  
- **Educational Framework**: Theory explanations with empirical status transparency
- **Community Safety**: Multi-layered guardrails preventing diagnostic claims
- **Scalable Architecture**: Production-ready with monitoring and audit capabilities

**For installation, deployment, and configuration details, see the main project [README](../readme.md).**
