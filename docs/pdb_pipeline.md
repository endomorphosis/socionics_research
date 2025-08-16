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
pip install -e bot[dev]
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

**Trending Query Discovery**:
```bash
pdb-cli hot-queries
```
Fetches and stores trending search terms for community interest analysis.

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

**For complete CLI reference and advanced usage, see the bot [README](../bot/README.md#personality-database-pdb-integration).**
