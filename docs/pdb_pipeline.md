# Personality Database (PDB) Pipeline

**Updated**: 2025-08-16  
**Status**: Production-ready data ingestion and analysis system

**Purpose**: Comprehensive pipeline for fetching, processing, and analyzing personality profiles from the Personality Database public API with IPFS content-addressed storage, embedding generation, and relationship network analysis.

## Data Storage Architecture

### Core Storage Files
- **`data/bot_store/pdb_profiles.parquet`**: Primary storage with `cid` (IPFS content ID) and `payload_bytes` (canonical JSON)
- **`data/bot_store/pdb_profile_vectors.parquet`**: Semantic embeddings indexed by `cid` for vector similarity search
- **`data/bot_store/pdb_profile_edges.parquet`**: Relationship network data with `(from_pid, to_pid, relation, source)` tuples

### Content Addressing
- Uses IPFS CID (Content Identifier) system for deduplication and integrity verification
- Enables efficient storage with automatic deduplication of identical profile content
- Provides cryptographic verification of data integrity

## Command Line Interface (CLI)

### Installation
```bash
# from repository root
python -m venv .venv
source .venv/bin/activate
cd bot
pip install -e .[dev]
```

### Basic Operations

**Profile Dumping with Filters**:
```bash
pdb-cli dump --cid 15 --pid 1 --max 500 --rpm 60 --concurrency 4
```

**API Exploration**:
```bash
pdb-cli peek profiles --params '{"limit":3}'
```

**Field Discovery** (after authentication):
```bash
pdb-cli discover profiles --params '{"limit":200}' --keys cid,pid,cat_id,property_id
```

**ID Pair Discovery**:
```bash
pdb-cli discover-cidpid --path profiles --sample 300 --limit 20
```

> Dev note: When running without installing, you can invoke the CLI via module path:
> `PYTHONPATH=bot/src python -m bot.pdb_cli <command> ...`

### Authentication & Configuration

**Environment Variables**:
- `PDB_API_TOKEN`: Sets `Authorization: Bearer <token>`
- `PDB_API_HEADERS`: JSON string of extra headers, e.g. `'{"X-API-Key":"..."}'`  
- `PDB_API_BASE_URL`: Override base URL if needed (default: personality-database.com API)

**Configuration File** (`.env` in repo root):
```bash
PDB_API_TOKEN=your_token_here
# Alternative: PDB_API_HEADERS={"X-API-Key":"your_key"}  
# PDB_API_BASE_URL=https://api.personality-database.com/api/v1
PDB_CACHE=true
PDB_CACHE_DIR=data/bot_store/pdb_api_cache
```

Tips:
- v2 endpoints often require browser-like headers and valid cookies. Store them in `.secrets/pdb_headers.json` and pass via `--headers "$(tr -d '\n' < .secrets/pdb_headers.json)"` or set `PDB_API_HEADERS`.
- Enable `PDB_CACHE=1` to reduce duplicate GETs during exploration; clear with `pdb-cli cache-clear`.

### Cache Management
```bash
pdb-cli cache-clear  # Clear API response cache
```

### Data Operations

**Bulk Profile Ingestion**:
```bash
pdb-cli dump-any --max 100 --rpm 60 --concurrency 4
```

**Embedding Generation**:
```bash
# Use lightweight embeddings (development)
pdb-cli embed

# Use full embeddings (production)
export SOCIONICS_LIGHTWEIGHT_EMBEDDINGS=0
export SOCIONICS_EMBED_MODEL=sentence-transformers/all-MiniLM-L6-v2
pdb-cli embed
```

**Vector Search**:
```bash
pdb-cli search "curious introverted detective" --top 5
```

**FAISS Index** (optimized for large datasets):
```bash
pdb-cli index --out data/bot_store/pdb_faiss.index
pdb-cli search-faiss "curious introverted detective" --top 5 --index data/bot_store/pdb_faiss.index
```

**Coverage Snapshot**:
```bash
# Basic coverage report
pdb-cli coverage --sample 10

# Detailed coverage with vector status
pdb-cli coverage --sample 50 --verbose

# Coverage after discovery run
export PDB_CACHE=1
pdb-cli scan-all --max-iterations 1 --search-names --pages 2
pdb-cli coverage --sample 20  # check progress
```
Reports raw rows, unique CIDs, vector coverage, v1 coverage, and sample missing IDs.

### Data Export & Analysis

**Normalized Profile Export**:
```bash
pdb-cli export --out data/bot_store/pdb_profiles_normalized.parquet
```
Exports structured data: `cid`, `name`, `description`, `mbti`, `socionics`, `big5`

**Dataset Summary**:
```bash
pdb-cli summarize --normalized data/bot_store/pdb_profiles_normalized.parquet
```

## Advanced Features

### API v2 Integration

**Related Profile Networks**:
```bash
export PDB_API_BASE_URL=https://api.personality-database.com/api/v2
pdb-cli related --ids 498239,12345
```

**Key Features**:
- Prefers `data.relatedProfiles` when available
- Fallback to list-valued fields (`works`, `acting`) with source annotation
- Content-addressed storage prevents duplication across relationship sources

**Search-Top Examples**:
```bash
# Basic search-top (personality keywords)
pdb-cli search-top "introvert" --pages 3 --limit 20

# Only include profiles (filter out other entities)
pdb-cli search-top "INTJ" --only-profiles --pages 5 --limit 50

# Search with caching enabled
export PDB_CACHE=1
pdb-cli search-top "psychology" --pages 2 --until-empty

# Batch search multiple terms
for term in "ENFP" "ISFJ" "psychology"; do
  pdb-cli search-top "$term" --pages 1 --only-profiles
done
```

**Trending Query Discovery**:
```bash
# Fetch trending queries 
pdb-cli hot-queries

# Follow trending queries with discovery
pdb-cli follow-hot --pages 3 --limit 30
```
Fetches and stores trending search terms for community interest analysis.

### Discovery-First Scanning (scan-all)

`scan-all` iteratively expands coverage using v2 related profiles, optional name-based `search/top`, and optional generic token sweeps. It supports persistent skip-state across runs and GET caching for efficiency.

Key behaviors:
- BFS over seeds: fetch `profiles/{id}/related`, record edges, upsert payloads.
- Optional name search: for each related item, call `search/top` with its name.
- Optional sweeps: page `search/top` with generic tokens (e.g., a–z, 0–9).
- No-progress guards: stop paging for names and sweeps after N pages with no new identities discovered (configurable via `--max-no-progress-pages`).
- Persistent state: `--use-state --state-file data/bot_store/scan_state.json` tracks processed seeds, names, and sweep tokens to avoid rework between runs.

Example (stateful + cached):
```bash
export PDB_CACHE=1
pdb-cli \
	--rpm 90 --concurrency 3 --timeout 30 \
	--base-url https://api.personality-database.com/api/v2 \
	--headers "$(tr -d '\n' < .secrets/pdb_headers.json)" \
	scan-all --max-iterations 0 \
	--search-names --limit 20 --pages 1 --until-empty \
	--sweep-queries a,b,c --sweep-pages 10 --sweep-until-empty --sweep-into-frontier \
	--max-no-progress-pages 3 \
	--auto-embed --auto-index --index-out data/bot_store/pdb_faiss.index \
	--scrape-v1 --v1-base-url https://api.personality-database.com/api/v1 \
	--v1-headers "$(tr -d '\n' < .secrets/pdb_headers.json)" \
	--use-state
```

**Additional scan-all Examples**:

```bash
# Minimal discovery run (BFS only, no sweeps)
pdb-cli scan-all --max-iterations 5

# Name search only (no sweeps, with state persistence)
pdb-cli scan-all \
  --search-names --pages 3 --limit 50 \
  --max-no-progress-pages 2 \
  --use-state

# Full sweep with early termination guards
export PDB_CACHE=1
pdb-cli scan-all \
  --search-names --pages 2 \
  --sweep-queries "psychology,personality,mbti,jung" \
  --sweep-pages 5 --sweep-until-empty \
  --max-no-progress-pages 3 \
  --use-state --auto-embed

# State reset + fresh discovery
pdb-cli scan-all --state-reset --max-iterations 3 --search-names
```

Troubleshooting:
- If you see repeated pages with “0 new, N updated”, set or lower `--max-no-progress-pages`.
- If v1 scraping returns 401, verify cookies in `--v1-headers`.
- Use `diagnose-query` to inspect collected pages and cursors for a given keyword.

### Comprehensive Troubleshooting Guide

#### Authentication Issues

**v1/v2 API 401/403 Errors**:
- Ensure `Referer`, `Origin`, and `Cookie` are present in headers
- Refresh cookies from an active browser session
- Pass headers via `--headers "$(tr -d '\n' < .secrets/pdb_headers.json)"` or set `PDB_API_HEADERS`
- For v2 endpoints, verify the base URL: `https://api.personality-database.com/api/v2`
- Check that your headers include a valid `User-Agent` string

**Example Valid Headers** (`.secrets/pdb_headers.json`):
```json
{
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
  "Accept": "application/json, text/plain, */*",
  "Referer": "https://www.personality-database.com/",
  "Origin": "https://www.personality-database.com",
  "Cookie": "session_id=abc123..."
}
```

#### Paging and Discovery Issues

**Endless Paging Loops**:
- Use `--max-no-progress-pages 3` (default) to stop low-yield paging
- Reduce `--pages` and `--sweep-pages` for initial runs
- Run `pdb-cli diagnose-query --contains <token>` to inspect pagination behavior
- Check for cursor loops: `diagnose-query` will show repeated cursors

**Low Discovery Yield**:
```bash
# Debug specific search terms
pdb-cli diagnose-query --contains "psychology" --verbose

# Check coverage before/after runs
pdb-cli coverage --sample 20
pdb-cli scan-all --max-iterations 1
pdb-cli coverage --sample 20  # compare results
```

#### Cache and State Management

**Clear API Cache**:
```bash
pdb-cli cache-clear  # removes all cached HTTP responses
```

**Reset Scan State**:
```bash
# Complete reset
rm data/bot_store/scan_state.json
pdb-cli scan-all --max-iterations 5

# Or use built-in reset
pdb-cli scan-all --state-reset --max-iterations 5
```

**State File Corruption**:
```bash
# Backup and reset if state file is corrupted
mv data/bot_store/scan_state.json data/bot_store/scan_state.json.backup
pdb-cli scan-all --use-state  # will create fresh state
```

#### Performance and Rate Limiting

**Rate Limit Debugging**:
```bash
# Reduce concurrency and RPM for debugging
pdb-cli --rpm 30 --concurrency 1 --timeout 60 scan-all --max-iterations 1

# Monitor logs for rate limit responses
tail -f scan_all_run.log
```

**Memory Issues with Large Datasets**:
```bash
# Process in smaller batches
pdb-cli scan-all --max-iterations 1 --auto-embed
# Then continue:
pdb-cli scan-all --max-iterations 1 --use-state --auto-embed
```

#### Data Consistency Issues

**Vector/Embedding Problems**:
```bash
# Regenerate all vectors
rm data/bot_store/pdb_profile_vectors.parquet
pdb-cli embed

# Rebuild FAISS index after vector regeneration  
pdb-cli index --out data/bot_store/pdb_faiss.index
```

**CID Integrity Check**:
```bash
# Export normalized data and check for CID consistency
pdb-cli export --out test_export.parquet
pdb-cli summarize --normalized test_export.parquet
```

### Statistical Analysis

**KL/JS Divergence Analysis** for survey response patterns:

**Input Schema** (CSV/Parquet):
- `subject_id`: Unique respondent identifier  
- `type_label`: Type category (socionics, MBTI, etc.)
- `question_id`: Question identifier
- `answer_value`: Integer-coded response (e.g., Likert 1-5)

**Analysis Execution**:
```bash
pdb-cli analyze --file responses.parquet --top 40 --smoothing 1e-4 --format table
```

**Output**: Questions ranked by Jensen-Shannon divergence between type pairs, with directional KL divergence metrics for asymmetry analysis.

## Integration with Research Pipeline

This PDB pipeline serves as a foundational data source for socionics research, providing:
- **Large-scale personality data** for statistical analysis
- **Network relationship mapping** for social validation studies  
- **Community interest tracking** via trending query analysis
- **Statistical divergence tools** for identifying discriminative survey questions

### Relationship Graph Tools

Relationships discovered via `related`/`scan-all` are stored in `data/bot_store/pdb_profile_edges.parquet`. Analyze and export with:

```bash
# Summary of edges and top degrees
pdb-cli edges-report --top 15

# Connected components and top-degree nodes per component
pdb-cli edges-analyze --top 5 --per-component-top 10

# Export per-node component membership and degrees
pdb-cli edges-export --out data/bot_store/pdb_profile_edges_components.parquet
```

Artifacts:
- `pdb_profile_edges.parquet`: from_pid, to_pid, relation, source
- `pdb_profile_edges_components.parquet`: pid, component, out_degree, in_degree, degree

**For complete CLI reference and advanced usage, see the bot [README](../bot/README.md#data-ingestion-cli-pdb).**

### Ingestion Cycle Script

Automate a basic discovery cycle with headers, rate/concurrency, and optional flags via `scripts/pdb_ingest_cycle.sh`:

```bash
PDB_API_HEADERS='{"User-Agent":"...","Referer":"...","Origin":"...","Cookie":"..."}' \
./scripts/pdb_ingest_cycle.sh
```
This runs `hot-queries → follow-hot → export → summarize → ingest-report → index` with caching enabled.
