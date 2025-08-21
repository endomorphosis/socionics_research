# Personality Planet – Implementation Plan (Compass Evolution)

This document captures the plan to evolve the existing Compass app into a "Personality Planet" experience, while keeping the current app intact and functional.

## Summary
- Keep the current scatter/PCA experience intact.
- Add a Planet mode (globe view) that clusters search results via K-Means (cosine) and places them on a sphere, so users can navigate a "personality planet" and find the cluster closest to them.

## Backup (Completed)
A timestamped backup of the current app has been created:
- `/home/devel/socionics_research/compass_backup_20250819_211107`

## Milestones and Tasks
1) Branch, guardrail, and UI toggle
- Create a feature branch (e.g., `vibe-globe`).
- Add a Planet mode toggle and container; keep globe code behind it.
- Ensure no regressions to current flow.

2) Data contract for clustering and placement
- Inputs: records (cid, displayName, ...), and vectors for each cid via `VEC.getVector(cid)`.
- Outputs: labels per record, centroid vectors and display metadata.
- Error modes: missing vectors (filter and warn), `N < k` (reduce k), empty results (skip).

3) K-Means module (client-side)
- New `kmeans.js` with API:
  - `kmeans(vectors: Float32Array[], k: number, opts?: { maxIters?: number, tol?: number })` → `{ labels, centroids, inertia }`
  - Cosine distance on L2-normalized vectors; KMeans++ seeding.
  - Heuristic `suggestK(n)`; default `k ∈ [3,12]`.

4) Globe renderer
- New `globe.js` (Three.js):
  - `init(containerEl)`, `setData({ centroids, points })`, `dispose()`
  - Sphere geometry for an abstract planet; uniform centroid placement with spherical Fibonacci lattice.
  - Members jittered around their centroid; colors per cluster; OrbitControls.

5) Search integration
- Add "Show on Globe" in search panel:
  - Collect top N results; resolve vectors; run K-Means with k slider.
  - Map clusters to globe positions; place members; open Planet mode.
- Controls for k and N.

6) "My position" and nearest-cluster UX (next)
- Expose `KNN.embed(text)` for a user vector.
- Allow user to set their description/name → compute nearest centroid → highlight cluster.
- Optionally choose an existing profile as "me".

7) Performance and workers
- Inline K-Means for N ≤ 1k; consider worker for larger sets.
- Keep globe draw efficient (InstancedMesh/sprites); lazy label rendering on hover.

8) Testing and acceptance
- Unit: K-Means convergence on synthetic data; handles `N < k`; input normalization.
- Integration: Searching "singers" clusters sensibly; nearest-cluster highlight works.
- UI: Toggle between scatter and globe; smooth interactions; console clean.
- Performance: clustering < 50ms for N=200, k≤12 on a typical laptop; smooth render.

9) Rollout/Docs
- Feature flag default on for dev; off for prod until validated.
- README update: controls, Planet mode overview, demo searches.

## File Changes (scaffolded)
- Added:
  - `compass/kmeans.js` – cosine K-Means
  - `compass/globe.js` – globe renderer
- Updated:
  - `compass/knn_client.js` – exported `embed(text)`
  - `compass/search.js` – Planet controls and Show/Hide on Globe
  - `compass/main.js` – imports for new modules

## Data/placement contract
- Compute `k` centroids from vectors.
- Generate `k` spherical Fibonacci points for uniform centroid placement.
- Place members near centroids; jitter scaled by similarity; option for in-cluster local PCA later.

## Risks and mitigations
- Sparse vectors: filter and show a warning about excluded results.
- Over-clustering small result sets: auto-reduce `k` or lock `k ≥ 3`.
- Performance on very large `N`: cap N in UI; add worker path if needed.

## Success criteria
- Planet mode toggle works with no regressions to existing scatter/ANN.
- Searching e.g., "singers" shows 3–12 clusters; “my position” highlights nearest.
- Smooth three.js interaction; hover/click tooltips; no major console errors.

## Next Enhancements
- Labels/billboards for points and centroids; hover tooltips.
- "Set My Position" control; highlight nearest cluster; smooth camera focus.
- Optional: move K-Means and/or TF-IDF embedding to a worker for larger sets.
