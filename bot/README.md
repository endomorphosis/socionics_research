# Socionics Discord Bot (LLM-Assisted)

Purpose: Provide structured, safeguarded access to Socionics research information, educational explanations, and guided self-reflection prompts within the Discord community.

## Functional Scope (v0.1)
- /about_socionics: Neutral overview of theory + empirical status.
- /theory <topic>: Returns concise, sourced explanation (rate-limited).
- /intertype <type1> <type2>: Summarize canonical relation description + evidence gaps + suggested falsifiable questions.
- /reflect: Issues a randomized structured prompt (logged with prompt_id).
- /consent: Runs consent onboarding flow (integrates with repository governance service).
- /my_type_help: Provides a structured questionnaire to help users gather observations; DOES NOT assign a type automatically.
- /explain_functions: High-level definitions with caveats.
- /privacy: Displays data handling & logging details.
- /ingest_channel: Ingest recent messages (vectors + hashed metadata only) (admin).
- /search_vectors: Semantic similarity search (rate-limited).
- /keyword_search: Hybrid hashed token + semantic narrowing.
- /context_window: Builds context snippet metadata.
- /purge_message: Remove a specific message vector (admin).
- /llm_context: Returns JSON metadata for RAG assembly (no content).

## Out-of-Scope (Hard Guardrails)
- Direct assignment of a user's Socionics type.
- Personalized coaching or psychological advice.
- Medical or diagnostic claims.

## Conditional Guidance for Type Exploration
Workflow:
1. User runs /my_type_help.
2. Bot returns a 6-dimension self-observation checklist (energy focus, information seeking pattern, comfort/volition cues, decision framing, discourse style, feedback sensitivity).
3. User optionally answers follow-up questions (1–2 per dimension).
4. Bot summarizes patterns using neutral descriptors and suggests 2–3 candidate study tasks (e.g., record a monologue under two prompt categories) rather than naming a type.
5. If user persists in asking for a type, bot reiterates policy and offers resources: methodology doc link + explanatory article.

## Interpersonal Dynamics Explanation
- Provide standard relation category description (e.g., Duality) + highlight: "Empirical Evidence Status: unverified / limited / emerging".
- Encourage formulation of testable interactions: e.g., "Measure coordination time on novel tasks vs. matched non-Dual pairs." 

## Safeguards
- Profanity / harassment filter before LLM call.
- Red-team prompt tests at startup (ensure blocked outputs for disallowed requests).
- Response provenance: prepend banner if answer includes theoretical claims not yet empirically validated.
- Logging (hashed user ID, command, prompt_id, timestamp, model version, guardrail flags) to JSONL (rotated daily).

## Evaluation Metrics
- Average response latency < 2.5s (95th < 5s) excluding first-token cold start.
- Guardrail violation rate < 0.5% of requests (auto escalation if >1%).
- User satisfaction (thumb reaction ratio) > 70% positive on /theory.
- Escalations to human moderators < 1 per 500 commands (steady state).

## Architecture Overview
```
Discord Gateway → Command Router → Guardrail Pipeline → Intent Classifier → Tool/LLM Orchestrator → Response Formatter → Discord API
```

Guardrail Pipeline Components:
- Pattern Blocker (regex for explicit type assignment requests)
- Sensitive Topic Classifier (basic safety)
- Rate Limiter (token bucket per user + global concurrency)
- PII Scrubber (optional for logs)

Tool/LLM Orchestration:
- Static Knowledge: Pre-rendered markdown snippets (versioned).
- Dynamic Retrieval: Local vector store of theory documents (FAISS) for /theory queries.
- Template-fused prompt constructing disclaimers + citations.

## Data Sources
- `docs/intro_socionics_research.md`
- `docs/operational_indicators.md`
- `docs/annotation_protocol.md`

## Future Extensions
- Semi-automated annotation suggestion for internal raters (/annotator mode).
- Multi-language support with translation quality confidence scores.
- Adaptive clarification questions (dialog state machine) for deeper concept explanations.

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
