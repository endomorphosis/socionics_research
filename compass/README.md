# Socionics Compass (SPA)

Single-port Express + Vite app hosting a Three.js 3D compass and globe visualization with client-side RAG-like search, in-browser Parquet/DuckDB ingestion, and advanced personality analysis features.

## Features
- **3D Globe Visualization ("Personality Planet")**: Interactive globe mode with K-means clustering of search results, surface modes (MBTI, Reinin, prismatic), and celestial personality exploration
- **Client-side Vector Search**: Hash embedding and vector KNN (cosine similarity) over normalized personality profiles
- **Advanced Indexing**: PCA projection to 3D space with accelerated KNN via WASM HNSW and IndexedDB caching
- **Multiple Data Sources**: JSON endpoints by default; optional Parquet ingestion via DuckDB-Wasm from `/dataset`
- **Smart Clustering**: K-means clustering with automatic cluster suggestion and interactive placement controls
- **Comprehensive Export Suite**: Export vectors, profiles, projections, manifests, ID maps, health reports, and combined datasets (JSON/CSV/Parquet formats)
- **Real-time Progress Tracking**: Progress bars, percentages, status updates, and toast notifications
- **Advanced Controls**: HNSW tuning (efSearch, efConstruction), dataset selectors, Auto Parquet mode
- **Debug & Monitoring**: Toggle debug panel with HNSW constructor info, build steps, cache events, error logging, copy/clear functionality

## Development

From `compass/`:
```bash
npm install
npm run dev
```
App runs on http://localhost:5173.

Note: Dataset files (Parquet/CSV) are served by the Express server on port 3000. If you plan to use Parquet in dev, also run:

```bash
npm start
```

Then the app will auto-resolve dataset URLs to http://localhost:3000/dataset/...

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

## UI Tips & Features
- **Search**: Type names or tags; use `cid:Qm...` to search vector neighbors by CID; supports semantic similarity matching
- **Globe Visualization**: Toggle between compass and globe modes; explore personality clusters on a 3D planet surface
- **Surface Modes**: Switch between MBTI, Reinin, and prismatic surface visualizations for different theoretical perspectives
- **K-means Clustering**: Automatically cluster search results with smart cluster count suggestion; visualize personality groups
- **Index Management**: 
  - **Rebuild**: Rebuild HNSW index with current settings
  - **Force Rebuild**: Rebuild without using cached data (one-shot)
  - **Clear Cache**: Remove current dataset's cached index
- **Import/Export Suite**:
  - **Export Index**: Save HNSW index as `.bin` file for fast loading
  - **Export Vectors**: Export normalized vectors (Alt: CSV format)
  - **Export Profiles**: Export enriched profiles (Alt: CSV, Ctrl: Parquet)
  - **Export Projections**: Export PCA projections (Alt: CSV, Ctrl: Parquet)
  - **Export Manifest**: Export dataset metadata including sources, dimensions, PCA variance
  - **Export ID Map**: Export CID to profile ID mapping (Alt: CSV, Ctrl: Parquet)
  - **Export Combined**: Export unified profiles+vectors dataset (Alt: CSV, Ctrl: Parquet)
  - **Health Report**: Export data quality and completeness report (JSON)
- **Progress Monitoring**: Real-time progress bars, percentage indicators, and detailed status messages for all operations
- **Debug Panel**: Enable "Debug" to view live system logs including:
  - HNSW constructor variant detection
  - Cache load/save events  
  - Build phases and timing
  - Error reporting and diagnostics
  - Copy logs to clipboard or clear history
- **Advanced Controls**:
  - **efSearch**: Tune HNSW recall vs speed (4-1024, default 64)
  - **efConstruction**: Set build-time quality (8-2048, default 200)
  - **Auto Parquet**: Automatically detect and load Parquet datasets
  - **No Cache**: Skip IndexedDB caching for debugging
  - **Worker Controls**: Toggle main thread vs worker execution for PCA, HNSW, and K-means

## Troubleshooting
- **Parquet read errors**: Ensure `/dataset/...` paths are reachable and schema matches expected columns
- **DuckDB-Wasm CSP/CDN**: If blocked, JSON fallback remains usable; disable Auto Parquet
- **Large bundles**: Vite warns on wasm-related chunk sizes; acceptable for local use
- **HNSW cache issues**: Cache key based on dataset signature (url|N|D|firstId); clear cache when switching datasets dramatically
- **Constructor variant not showing**: Click Force rebuild, or ensure cache is loaded; the UI emits constructor details on build, cache load, and import
- **Performance issues**: Toggle worker execution off for debugging; use "Inline PCA", "Inline HNSW", or "Inline KMeans" checkboxes
- **Memory issues**: Large datasets may require closing other browser tabs; monitor browser task manager## Troubleshooting
- Parquet read errors: ensure `/dataset/...` paths are reachable and schema matches expected columns.
- DuckDB-Wasm CSP/CDN: if blocked, JSON fallback remains usable; disable Auto Parquet.
- Large bundles: Vite warns on wasm-related chunk sizes; acceptable for local use.
- HNSW cache key: based on dataset signature (url|N|D|firstId). Clear cache if switching datasets dramatically.
- ctor variant not showing: click Force rebuild, or ensure cache is loaded; the UI emits ctor details on build, cache load, and import.

## Internals
- Worker: `vec_worker.js` handles PCA and HNSW building off-thread.
- Progress events: `vec:pca:*`, `vec:hnsw:*` update the UI; worker emits phases and chunked progress.
- Legacy scripts: `search_csv.js` provides a Node CSV search baseline; `rag_api.js` is deprecated.
