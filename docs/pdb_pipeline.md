# Personality DB Pipeline

This pipeline fetches profiles from the Personality Database public API, stores raw records in Parquet with IPFS CID primary keys, computes embeddings, runs vector search, and supports KL/JS divergence analysis over survey responses.

## Files
- `data/bot_store/pdb_profiles.parquet`: `cid`, `payload_bytes` (canonical JSON of the raw record)
- `data/bot_store/pdb_profile_vectors.parquet`: `cid`, `vector`

## CLI
Install:
```
pip install -e bot[dev]
```

Dump:
```
pdb-cli dump --cid 15 --pid 1 --max 500 --rpm 60 --concurrency 4
```

Dump without filters (useful to probe API):
pdb-cli peek profiles --params '{"limit":3}'

Discover frequent field values to pick filters (after auth):
```
pdb-cli discover profiles --params '{"limit":200}' --keys cid,pid,cat_id,property_id
```

Probe combinations to find working cid/pid pairs:
```
pdb-cli discover-cidpid --path profiles --sample 300 --limit 20
```

Auth environment variables:
- `PDB_API_TOKEN`: sets `Authorization: Bearer <token>`
- `PDB_API_HEADERS`: JSON string of extra headers, e.g. `'{"X-API-Key":"..."}'`
- `PDB_API_BASE_URL`: override base URL if needed

Optionally, create a `.env` in the repo root (auto-loaded by CLI):
```
PDB_API_TOKEN=your_token
# or
# PDB_API_HEADERS={"X-API-Key":"your_key"}
# PDB_API_BASE_URL=https://api.personality-database.com/api/v1
PDB_CACHE=true
PDB_CACHE_DIR=data/bot_store/pdb_api_cache
```

Manage cache:
```
pdb-cli cache-clear
```
```
pdb-cli dump-any --max 100 --rpm 60 --concurrency 4
```

Embed:
```
pdb-cli embed
```

Search:
```
pdb-cli search "curious introverted detective" --top 5
```

FAISS index (fast search on large datasets):
```
pdb-cli index --out data/bot_store/pdb_faiss.index
pdb-cli search-faiss "curious introverted detective" --top 5 --index data/bot_store/pdb_faiss.index
```

Export normalized profiles (cid, name, description, mbti, socionics, big5):
```
pdb-cli export --out data/bot_store/pdb_profiles_normalized.parquet
```

Summarize dataset (row counts, top types):
```
pdb-cli summarize --normalized data/bot_store/pdb_profiles_normalized.parquet
```

Use real embeddings:
```
export SOCIONICS_LIGHTWEIGHT_EMBEDDINGS=0
export SOCIONICS_EMBED_MODEL=sentence-transformers/all-MiniLM-L6-v2
pdb-cli embed
```

## v2 Related Profiles
Some endpoints in API v2 return related content under nested keys. To fetch and store related profiles for specific profile IDs:
```
export PDB_API_BASE_URL=https://api.personality-database.com/api/v2
# You may need realistic browser-like headers; set via PDB_API_HEADERS as JSON
pdb-cli related --ids 498239,12345
```
Notes:
- The CLI prefers `data.relatedProfiles` when present; otherwise it collects list-valued fields (e.g., `works`, `acting`) and annotates each item with `_source_list` and `_source_profile_id`.
- Records are upserted into the same raw Parquet and keyed by IPFS CID of the record JSON payload.

## v2 Hot Queries
Fetch trending searches from v2 and store them for discovery and seeding:
```
export PDB_API_BASE_URL=https://api.personality-database.com/api/v2
pdb-cli hot-queries
```
Notes:
- Extracts `data.queries` (each item has keys like `key`, `jumpTo`).
- Annotates each with `_source: search_hot_queries` and upserts into raw Parquet using CID.

## KL/JS Divergence Analysis
Input file schema (CSV or Parquet):
- `subject_id`: unique respondent identifier
- `type_label`: type category label (e.g., socionics, MBTI)
- `question_id`: question identifier
- `answer_value`: integer-coded answer (e.g., Likert 1..5)

Run analysis:
```
pdb-cli analyze --file responses.parquet --top 40 --smoothing 1e-4 --format table
```

The output ranks questions by Jensen-Shannon divergence between pairs of types with KL_ab and KL_ba reported for directionality.
