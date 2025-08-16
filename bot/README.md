# Socionics Discord Bot (v0.2)

**Updated**: 2025-08-16  
**Status**: Production-ready with privacy-first architecture

**Purpose**: Provide structured, safeguarded access to Socionics research information, educational explanations, and guided self-reflection prompts within the Discord research community.

## Bot Commands & Functionality

### Public Research Commands
- **`/about_socionics`**: Neutral overview of theory with empirical status caveats
- **`/theory <topic>`**: Concise, sourced explanations with rate limiting and citation tracking
- **`/intertype <type1> <type2>`**: Canonical relation descriptions with evidence gaps and falsifiable questions
- **`/explain_functions`**: High-level function definitions with methodological caveats
- **`/reflect`**: Randomized structured prompts for self-observation (logged with prompt_id)
- **`/privacy`**: Comprehensive data handling and logging transparency

### Participant Interaction  
- **`/consent`**: Multi-tier consent onboarding with granular data usage controls
- **`/my_type_help`**: Structured self-observation questionnaire (NO automatic type assignment)
- **`/search_vectors`**: Semantic similarity search across community knowledge base (rate-limited)
- **`/keyword_search`**: Hybrid hashed token + semantic search functionality

### Administrative Commands (Role-Restricted)
- **`/ingest_channel`**: Vector ingestion storing only embeddings + salted hashes
- **`/context_window`**: Context snippet metadata assembly for researchers  
- **`/purge_message`**: Privacy-compliant message deletion by ID
- **`/llm_context`**: RAG metadata assembly (returns JSON metadata only)

## Out-of-Scope (Hard Guardrails)
- Direct assignment of a user's Socionics type.
- Personalized coaching or psychological advice.
- Medical or diagnostic claims.

## Privacy & Research Ethics Framework

### Core Safeguards
- **Content Filtering**: Profanity and harassment detection before LLM processing
- **Red-Team Testing**: Startup validation ensuring blocked outputs for disallowed requests  
- **Response Provenance**: Clear labeling of empirically unvalidated theoretical claims
- **Audit Logging**: Comprehensive tracking (hashed user ID, command, timestamp, model version, guardrail flags)

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
	--headers '{"User-Agent":"Mozilla/5.0 ...","Referer":"https://www.personality-database.com/","Origin":"https://www.personality-database.com","Cookie":"X-Lang=en-US; ..."}' \
	search-top --pages 1 --limit 20 --query ''
```

### follow-hot
Resolves trending hot queries via v2 `search/top`. Upserts list-valued fields (e.g., `profiles`) and supports pagination.

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
PYTHONPATH=bot/src python -m bot.pdb_cli follow-hot --only-profiles --pages 2 --limit 20 --dry-run
```

### search-top
Queries v2 `search/top` directly, with paging and list filtering.

Flags:
- `--query` / `--keyword`: Query parameter (use `--encoded` if already URL-encoded)
- `--encoded`: Treat `--query` as already URL-encoded (e.g., `Elon%2520Musk`)
- `--limit`, `--next-cursor`, `--pages`, `--until-empty`: Pagination controls
- `--auto-embed`, `--auto-index`, `--index-out`: Post-ingest vector/index ops
- `--lists`, `--only-profiles`: Restrict which list arrays to upsert
- `--dry-run`: Preview results without writing/upserting or embedding/indexing

Examples:
```
PYTHONPATH=bot/src python -m bot.pdb_cli search-top --query '' --only-profiles --pages 1 --limit 20 --dry-run
PYTHONPATH=bot/src python -m bot.pdb_cli search-top --query 'Elon%2520Musk' --encoded --only-profiles --pages 1 --limit 20 --dry-run
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
- `--auto-embed`, `--auto-index`, `--index-out`: Post-scrape vector/index ops.
- `--dry-run`: Preview without upserts/embedding/indexing.

Examples:
```
PYTHONPATH=bot/src python -m bot.pdb_cli scan-related --seed-ids 498239,12345 --search-names --only-profiles --pages 1 --limit 20 --dry-run

# Infer seeds from parquet, collect related, then scrape v1 profiles and index
PYTHONPATH=bot/src python -m bot.pdb_cli \
	--base-url https://api.personality-database.com/api/v2 \
	--headers '{"User-Agent":"Mozilla/5.0 ...","Referer":"https://www.personality-database.com/","Origin":"https://www.personality-database.com","Cookie":"X-Lang=en-US; ..."}' \
	scan-related --max-seeds 50 --search-names --only-profiles --pages 1 --limit 20 --auto-embed --auto-index
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
	--auto-embed --auto-index --index-out data/bot_store/pdb_faiss.index \
	--scrape-v1 --v1-base-url https://api.personality-database.com/api/v1 \
	--v1-headers "$(tr -d '\n' < .secrets/pdb_headers.json)" \
	--use-state
```

Or use the helper script with sensible defaults and environment overrides:

```
./scripts/pdb_scan_all_stateful.sh
```

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
