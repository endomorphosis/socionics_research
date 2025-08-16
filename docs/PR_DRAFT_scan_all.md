PDB Discovery-First Ingestion + Stateful Scan Helper

Summary
- Builds a discovery-first ingestion pipeline for Personality Database (PDB) keyed by IPFS CID.
- Stores raw payloads and embeddings in Parquet; maintains a FAISS index aligned by CID.
- Adds stateful, idempotent scan-all with BFS over related profiles, optional name search and token sweeps, GET cache, and no-progress guards.
- Provides edges capture + analytics/exports to study profile graph structure.

How To Run
- One-command stateful scan with auto-embed/index:
  - `./scripts/pdb_scan_all_stateful.sh`
  - Environment overrides (examples):
    - `RPM=90 CONCURRENCY=3 PAGES=3 SWEEP_PAGES=20 ./scripts/pdb_scan_all_stateful.sh`
  - Requires `./.secrets/pdb_headers.json` (browser-like headers + cookies).
- Manual, fully explicit example:
  - `PYTHONPATH=bot/src python -m bot.pdb_cli --rpm 90 --concurrency 3 --timeout 30 --base-url https://api.personality-database.com/api/v2 --headers "$(tr -d '\n' < .secrets/pdb_headers.json)" scan-all --max-iterations 0 --initial-frontier-size 1000 --search-names --limit 20 --pages 3 --until-empty --sweep-queries a,b,c,d,e,f,g,h --sweep-pages 20 --sweep-until-empty --sweep-into-frontier --max-no-progress-pages 3 --auto-embed --auto-index --index-out data/bot_store/pdb_faiss.index --scrape-v1 --v1-base-url https://api.personality-database.com/api/v1 --v1-headers "$(tr -d '\n' < .secrets/pdb_headers.json)" --use-state`

Monitoring
- Tail the latest scan log:
  - `./scripts/pdb_tail_scan_log.sh` (after `chmod +x`)
  - or manually: `tail -f $(ls -t /tmp/pdb_scan_all_*.log | head -n 1)`
- Quick health snapshots:
  - Coverage: `PYTHONPATH=bot/src python -m bot.pdb_cli coverage --sample 10`
  - Edges: `PYTHONPATH=bot/src python -m bot.pdb_cli edges-analyze --top 3 --per-component-top 5`
  - Ingest report: `PYTHONPATH=bot/src python -m bot.pdb_cli ingest-report --top-queries 10`

Current Metrics (snapshot)
- Coverage: Raw rows 1114; unique CIDs 1114; vectors 1078; v1_profiles 506; seen_ids 538; missing v1 (sample 5 of 32): [21489, 74976, 77522, 100638, 105143]
- Graph: Nodes 527; Edges 3889; Components 28
  - Component 1 size 81 (top degrees ~88)
  - Component 2 size 78 (top degrees ~84)
  - Component 3 size 57 (top degrees ~64)
- Ingest list counts: profiles: 14 (top query: Elon%2520Musk)

Artifacts & Paths
- Raw: `data/bot_store/pdb_profiles.parquet`
- Vectors: `data/bot_store/pdb_profile_vectors.parquet`
- FAISS index: `data/bot_store/pdb_faiss.index` (+ `.cids`)
- Normalized export: `data/bot_store/pdb_profiles_normalized.parquet`
- Edges: `data/bot_store/pdb_profile_edges.parquet` (+ components export)
- GET cache: `data/bot_store/pdb_api_cache/`
- Scan state: `data/bot_store/scan_state.json`

Notes
- The helper script now runs Python unbuffered (`-u`) for timely logs. If an older run is active, restart to benefit from unbuffered logging.
- No-progress guards cap low-yield name pages and token sweeps to prevent stalls and redundant reads.
- The pipeline is discovery-first; broad v1 scraping is gated behind validated coverage and state to minimize wasted IO.
