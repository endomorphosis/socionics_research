# Personality Planet (Compass v2)

This globe mode clusters the current search results with K-Means and places them on a 3D globe. You can then compare your own position and explore clusters.

## Quick start
1. Start server (single port): `npm start` (after `npm run build`).
2. Open http://localhost:3000/.
3. Load profiles and vectors (Auto Parquet can help). You can use the default JSONs.
4. Search for something (e.g., "singers").
5. Click "Show on Globe" to cluster the top N results (N, k adjustable).
6. Click on points to see an info card; add to chart or highlight in the list.
7. Enter your text in "My position" and click Set to mark your position and highlight the nearest cluster.
8. Toggle Links/Labels; Export Planet to save placements.

### 4D Surface Overlays
- Contours: toggles great-circle guides that represent the four MBTI/Socionics dichotomies.
- Width: adjusts line thickness of the guides.
- Colors: choose Vivid, Soft, or Mono presets for the contour palette.
- Intensity: adjusts how strongly contour colors blend onto the surface.
- Axis + θ: rotate the polar orientation of the guides so hemispherical borders align to your preferred view; Reset Axis returns to 0°.
- Normals: choose Globe/MBTI/Orthogonal/Tetrahedron presets, or pick Custom to enter four plane normals (nE, nN, nF, nP). Values can be comma or space separated (x,y,z) and are normalized automatically. These define which great circles are drawn and how they rotate with the polar axis.
	- Equivariant (Tetrahedron): uses a symmetric set of four normals arranged like a regular tetrahedron. This yields highly symmetric partitions on the sphere and can help compare regions without axis bias.

#### Settings portability
- Export Settings: saves your current globe/guide configuration (links, labels, grid, contours, colors, intensity, normals preset or custom normals, axis and θ) as a JSON file you can share.
- Import Settings: loads a previously exported JSON and applies it immediately.

#### Custom normals presets
- Inside the Custom normals section you can Save a named preset, then Load or Delete it later. Presets persist in your browser and can be Exported/Imported as a JSON bundle.

These overlays apply to both Prismatic and MBTI Region surface modes, and rotate in sync with the axis gizmo.

### Reinin partition and legend
- Reinin (Tetrakis) surface partitions the globe into 24 faces with a configurable color palette.
- A compact legend appears when Reinin is active. It supports:
	- Density toggle: switch between Compact and Cozy spacing (Cozy uses larger font and swatches); preference persists and is included in Export Settings.
	- Click a swatch to focus that face on the globe; hover a swatch/name to highlight the face.
	- Edit: open the legend editor to rename the 24 faces; names are used in globe hover tooltips.
	- Names are applied globally and pushed to the globe immediately, even if you’re on another surface; when you switch to Reinin, hover tooltips will reflect your custom names.
	- Copy/Import: export/import names+colors as JSON; also included in globe settings export/import.
- Hovering the globe on the Reinin surface shows the face name (either your custom legend name or a default label).

#### Default Reinin face mapping (24 faces)
Faces are grouped by axis with opposites on opposite faces and a stable 4-triangle order per face (consistent in shader and hover logic):

- +X: Extravert, Declaring, Result-oriented, Obstinate
- −X: Introvert, Asking, Process-oriented, Yielding
- +Y: Ethical, Emotive, Irrational, Strategic
- −Y: Logical, Constructive, Rational, Tactical
- +Z: Sensing, Dynamic, Negative, Farsighted
- −Z: Intuitive, Static, Positive, Carefree

You can rename any face via Legend… (persisted and used in hover tooltips), or fully customize the initial defaults in `search.js` (function `defaultReininNames24`).

### Function-aware placement (4D Projection)
- In 4D Projection mode, placements are derived from MBTI/Socionics type via a Hopf map to the sphere. When a Normals preset is selected (MBTI/Orthogonal/Tetrahedron/Custom), a small function-aware offset is applied along the surface based on each type’s function stack and role weights. This nudges points within their region without breaking topology and can reveal within-type tendencies.
- Controls: toggle Function-aware on/off; set Offset (max degrees), Attitude roll (advanced, degrees of roll around the surface tangent for intro/extra functions), and per-role weights (Dominant/Aux/Ter/Inf). Set Normals to Globe to disable the offset entirely.

## Auto Globe
- Turn on "Auto" in planet controls and set the "Auto N≥" threshold.
- After each search, if results meet the threshold, the app will automatically show and place the results on the globe.
- Manual "Show on Globe" still works; preferences are saved in localStorage.

## Notes
- Clustering uses cosine K-Means with KMeans++ seeding.
- Large-N runs use a Web Worker with live progress and a Cancel button; completion shows iteration count. You can force inline KMeans via the Inline KMeans toggle in the planet controls (useful for debugging/comparing).
- The ANN index (HNSW) and PCA run in-browser; both have worker-first paths with fallbacks.
- Preferences (planet toggles, ef values, cache use) persist in localStorage.
- The globe is always rendered in a centered square (letterboxed) viewport so the sphere remains perfectly spherical and centered regardless of window aspect ratio.
- Datasets: `/dataset/*.parquet` via server, or default JSONs in `public/`.
	- Tip: if Parquet loads fail when using `/dataset/...`, ensure the server is running (`npm start`) so those URLs return binary data instead of an HTML page.
