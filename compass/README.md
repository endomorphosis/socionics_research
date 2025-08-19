# Socionics Compass (SPA)

Single-port Express + Vite app hosting a Three.js 3D compass with client-side RAG-like search and in-browser Parquet/DuckDB ingestion.

## Features
- Client-side search over normalized profiles (hash embedding) and vector KNN (cosine).
- Vectors: PCA projection to 3D; accelerated KNN via WASM HNSW with IndexedDB caching.
- Data sources: JSON by default; optional Parquet via DuckDB-Wasm from `/dataset`.
- Dataset selectors and "Auto Parquet" with persisted preferences.
- HNSW controls: rebuild, clear cache, export/import index.
- Progress UI with percentages, status line, bar, and toasts.
- Debug panel (toggle in UI): shows HNSW ctor variant, keys, build steps, cache events, and errors; includes Clear/Copy and auto-scroll.

## Development

From `compass/`:
```bash
npm install
npm run dev
```
App runs on http://localhost:5173.

## Build + Run (single port)

```bash
npm run build
npm start
```
Open http://localhost:3000/.

Health check:

```bash
curl -s http://localhost:3000/health
```

## Data
- Default JSON endpoints: `/pdb_profiles.json`, `/pdb_profile_vectors.json` (served from `public/`).
- Dataset directory mapped to `/dataset` (see server log for host path). Place Parquet files (e.g., `pdb_profile_vectors.parquet`, `pdb_profiles_normalized.parquet`) in `../data/bot_store`.

## UI Tips
- Search: type a name or tags; use `cid:Qm...` to search vector neighbors by CID.
- Index: use Rebuild to rebuild HNSW, Clear cache to remove the current dataset’s cached index.
- Import/Export: export index to a `.bin` file; import to restore without rebuilding.
- Progress: watch both the text and percentage bar for PCA/HNSW phases.
- Debug panel: enable "Debug" to view live logs (ctor variant, keys, cache: loaded/saved, errors). Use Clear to reset and Copy to share logs.

## Troubleshooting
- Parquet read errors: ensure `/dataset/...` paths are reachable and schema matches expected columns.
- DuckDB-Wasm CSP/CDN: if blocked, JSON fallback remains usable; disable Auto Parquet.
- Large bundles: Vite warns on wasm-related chunk sizes; acceptable for local use.
- HNSW cache key: based on dataset signature (url|N|D|firstId). Clear cache if switching datasets dramatically.
- ctor variant not showing: click Force rebuild, or ensure cache is loaded; the UI emits ctor details on build, cache load, and import.

## Internals
- Worker: `vec_worker.js` handles PCA and HNSW building off-thread.
- Progress events: `vec:pca:*`, `vec:hnsw:*` update the UI; worker emits phases and chunked progress.
- Legacy scripts: `search_csv.js` provides a Node CSV search baseline; `rag_api.js` is deprecated.
