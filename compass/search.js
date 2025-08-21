  // Remove legend and add-personality-form if present
  window.addEventListener('DOMContentLoaded', () => {
    const legend = document.getElementById('legend');
    if (legend && legend.parentElement) legend.parentElement.removeChild(legend);
    const addForm = document.getElementById('add-personality-form');
    if (addForm && addForm.parentElement) addForm.parentElement.removeChild(addForm);
  });
// Search UI and logic for highlighting personalities

import { DEFAULT_NORMALS as MBTI_DEFAULT_NORMALS } from './mbti_data.js';
import { computeFunctionOffset, DEFAULT_ROLE_WEIGHTS, TETRAHEDRAL_NORMALS } from './mbti_functions.js';

window.addEventListener('DOMContentLoaded', () => {
  // RAG Search Panel (fixed, always visible, styled)
  const searchDiv = document.createElement('div');
  searchDiv.id = 'search-controls';
  searchDiv.style.position = 'fixed';
  searchDiv.style.top = '2em';
  searchDiv.style.left = '2em';
  searchDiv.style.width = '340px';
  searchDiv.style.maxWidth = '90vw';
  searchDiv.style.background = 'rgba(255,255,255,0.98)';
  searchDiv.style.borderRadius = '12px';
  searchDiv.style.boxShadow = '0 2px 16px rgba(0,0,0,0.13)';
  searchDiv.style.padding = '1.2em 1.2em 1em 1.2em';
  searchDiv.style.zIndex = 1005;
  searchDiv.style.display = 'flex';
  searchDiv.style.flexDirection = 'column';
  searchDiv.style.alignItems = 'stretch';
  searchDiv.innerHTML = `
  <label for="search-input" style="font-weight:bold;font-size:1.1em;margin-bottom:0.5em;">Semantic Search (client-side)</label>
    <input id="search-input" type="text" placeholder="Search for a celebrity..." autocomplete="off" style="padding:0.7em 1em;font-size:1.1em;border-radius:8px;border:1.5px solid #bbb;margin-bottom:0.7em;" />
  <small style="color:#666">Tip: prefix with cid:Qm... or id:VALUE (any id) to use vector KNN; names come from Profiles</small>
    <div id="search-status" style="color:#666;margin:0.25em 0 0.25em 0;font-size:0.85em;"></div>
    <div id="search-progress" style="color:#888;margin:0 0 0.5em 0;font-size:0.8em;height:1.2em;"></div>
    <div id="search-actions" style="display:flex;gap:0.5em;margin-bottom:0.5em;flex-wrap:wrap;align-items:center;">
  <button id="btn-rebuild-index" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Rebuild index</button>
  <button id="btn-rebuild-force" title="Rebuild without using cache (one-shot)" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Force rebuild</button>
      <button id="btn-cancel-build" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;" disabled>Cancel</button>
      <button id="btn-clear-cache" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Clear cache</button>
  <label title="HNSW efSearch (higher=more recall)">ef <input id="num-efsearch" type="number" min="4" max="1024" step="1" value="64" style="width:72px;padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;" /></label>
  <label title="HNSW efConstruction (build-time quality)">efC <input id="num-efc" type="number" min="8" max="2048" step="1" value="200" style="width:76px;padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;" /></label>
      <select id="sel-vectors" title="Choose vectors parquet" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;min-width:200px;"></select>
      <button id="btn-load-parquet" title="Load vectors via DuckDB-Wasm" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Load Parquet</button>
      <select id="sel-profiles" title="Choose profiles parquet" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;min-width:200px;"></select>
      <button id="btn-load-profiles" title="Load profiles for KNN" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Load Profiles</button>
      <button id="btn-export-index" title="Export HNSW index" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Export Index</button>
  <button id="btn-export-vectors" title="Export normalized vectors (Alt: CSV)" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Export Vectors</button>
  <button id="btn-export-profiles" title="Export enriched profiles (Alt: CSV, Ctrl: Parquet)" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Export Profiles</button>
  <button id="btn-export-proj" title="Export PCA projections (Alt: CSV, Ctrl: Parquet)" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Export Proj</button>
  <button id="btn-export-manifest" title="Export dataset manifest (sources, dims, PCA variance)" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Export Manifest</button>
  <button id="btn-export-idmap" title="Export ID map (Alt: CSV, Ctrl: Parquet)" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Export IdMap</button>
  <button id="btn-export-combined" title="Export combined dataset (profiles+vectors) (Alt: CSV, Ctrl: Parquet)" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Export Combined</button>
  <button id="btn-health-report" title="Export data health report (JSON)" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Health Report</button>
      <label for="file-import-index" style="display:inline-flex;align-items:center;gap:0.3em;border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">
        Import Index <input type="file" id="file-import-index" accept="application/octet-stream,.bin" style="display:none" />
      </label>
      <label style="display:flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
        <input type="checkbox" id="chk-auto-parquet" /> Auto Parquet
      </label>
      <label style="display:flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;" title="Skip load/save HNSW index to IndexedDB">
        <input type="checkbox" id="chk-no-cache" /> No cache
      </label>
      <span style="display:flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
        <label title="Show internal debug messages" style="display:inline-flex;align-items:center;gap:0.3em;">
          <input type="checkbox" id="chk-debug" /> Debug
        </label>
        <label title="Compute PCA on main thread (disable worker)" style="display:inline-flex;align-items:center;gap:0.3em;">
          <input type="checkbox" id="chk-inline-pca" /> Inline PCA
        </label>
        <label title="Build HNSW on main thread (disable worker)" style="display:inline-flex;align-items:center;gap:0.3em;">
          <input type="checkbox" id="chk-disable-worker" /> Inline HNSW
        </label>
        <label title="Compute KMeans on main thread (disable worker)" style="display:inline-flex;align-items:center;gap:0.3em;">
          <input type="checkbox" id="chk-inline-kmeans" /> Inline KMeans
        </label>
        <button id="btn-clear-debug" title="Clear debug log" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Clear</button>
        <button id="btn-copy-debug" title="Copy debug log to clipboard" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Copy</button>
      </span>
    </div>
  <div id="progress-bar" style="height:6px;background:#eee;border-radius:4px;overflow:hidden;margin:-0.25em 0 0.5em 0;">
    <div id="progress-bar-fill" style="height:100%;width:0;background:#4363d8;transition:width 0.12s ease;"></div>
  </div>
  <pre id="debug-log" style="display:none;white-space:pre-wrap;background:#fafafa;border:1px solid #eee;border-radius:6px;padding:0.5em;color:#444;max-height:160px;overflow:auto;margin:0 0 0.5em 0;"></pre>
  <div id="planet-controls" style="display:none;gap:0.5em;align-items:center;margin:0 0 0.5em 0;">
    <label title="Placement mode" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      Placement
      <select id="sel-placement" style="padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;">
        <option value="clusters" selected>Clusters (KMeans)</option>
  <option value="4d">4D Projection (Socionics/MBTI)</option>
      </select>
    </label>
      <span style="margin-left:.6em">Surface</span>
      <select id="sel-surface" title="Surface coloring" style="padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;">
        <option value="prismatic">Prismatic</option>
        <option value="mbti">MBTI Regions</option>
  <option value="reinin">Reinin (Tetrakis)</option>
      </select>
  <button id="btn-reinin-palette" title="Edit Reinin 24-color palette" style="display:none;border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Palette…</button>
  <button id="btn-reinin-legend" title="Edit Reinin legend (24 names)" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Legend…</button>
    <label title="Preset for great-circle orientations (4D planes)" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      normals
      <select id="sel-normals-preset" style="padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;">
        <option value="globe" selected>Globe default</option>
        <option value="mbti">MBTI defaults</option>
        <option value="orthogonal">Orthogonal-ish</option>
  <option value="tetra">Equivariant (Tetrahedron)</option>
        <option value="custom">Custom…</option>
      </select>
    </label>
    <div id="custom-normals" style="display:none;flex-basis:100%;padding:0.5em 0.6em;border:1px dashed rgba(0,0,0,0.15);border-radius:8px;background:rgba(255,255,255,0.45);">
      <div style="font-size:0.86em;color:#333;margin-bottom:0.4em;">Custom normals (x,y,z). Values are normalized automatically.</div>
      <div style="display:grid;grid-template-columns:auto 1fr;gap:0.4em 0.6em;align-items:center;">
        <label for="norm-e" title="E vs I plane normal" style="color:#444;">nE</label>
        <input id="norm-e" type="text" placeholder="1,0.2,0.1" style="padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;" />
        <label for="norm-n" title="N vs S plane normal" style="color:#444;">nN</label>
        <input id="norm-n" type="text" placeholder="0.1,1,0.2" style="padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;" />
        <label for="norm-f" title="F vs T plane normal" style="color:#444;">nF</label>
        <input id="norm-f" type="text" placeholder="0.2,0.1,1" style="padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;" />
        <label for="norm-p" title="P vs J plane normal" style="color:#444;">nP</label>
        <input id="norm-p" type="text" placeholder="-0.7,0.6,0.1" style="padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;" />
      </div>
    </div>
    <label title="Nudge positions within regions using function stacks" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      <input type="checkbox" id="chk-func-aware" checked/> Function-aware
    </label>
    <label title="Max surface offset in degrees (role-weighted)" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      offset <input type="range" id="range-func-offset" min="0" max="15" step="0.5" value="10" />
      <span id="val-func-offset" style="min-width:3.0em;text-align:right;color:#333;">10.0°</span>
    </label>
    <div id="func-weights" style="display:grid;grid-template-columns:auto 1fr auto;gap:0.3em 0.6em;align-items:center;">
      <span title="Dominant role weight" style="color:#444;font-size:0.9em;">Dom</span>
      <input type="range" id="range-w-dominant" min="0" max="1" step="0.05" value="0.5" />
      <span id="val-w-dominant" style="min-width:2.8em;text-align:right;color:#333;">0.50</span>
      <span title="Auxiliary role weight" style="color:#444;font-size:0.9em;">Aux</span>
      <input type="range" id="range-w-aux" min="0" max="1" step="0.05" value="0.3" />
      <span id="val-w-aux" style="min-width:2.8em;text-align:right;color:#333;">0.30</span>
      <span title="Tertiary role weight" style="color:#444;font-size:0.9em;">Ter</span>
      <input type="range" id="range-w-ter" min="0" max="1" step="0.05" value="0.15" />
      <span id="val-w-ter" style="min-width:2.8em;text-align:right;color:#333;">0.15</span>
      <span title="Inferior role weight" style="color:#444;font-size:0.9em;">Inf</span>
      <input type="range" id="range-w-inf" min="0" max="1" step="0.05" value="0.05" />
      <span id="val-w-inf" style="min-width:2.8em;text-align:right;color:#333;">0.05</span>
    </div>
    <label title="Attitude roll around surface tangent (advanced)" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      att roll <input type="range" id="range-func-roll" min="0" max="20" step="0.5" value="6" />
      <span id="val-func-roll" style="min-width:3.0em;text-align:right;color:#333;">6.0°</span>
    </label>
    <label title="Show 4D contour guides on the surface" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      <input type="checkbox" id="chk-contours" checked/> Contours
    </label>
    <label title="Contour line width" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
  width <input type="range" id="range-contour-width" min="0.02" max="0.2" step="0.005" value="0.09" />
  <span id="val-contour-width" style="min-width:3.5em;text-align:right;color:#333;">0.09</span>
    </label>
    <label title="Contour blend intensity" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
  intensity <input type="range" id="range-contour-intensity" min="0" max="1.2" step="0.02" value="0.55" />
  <span id="val-contour-intensity" style="min-width:3.5em;text-align:right;color:#333;">0.55</span>
    </label>
    <label title="Contour color preset" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      colors
      <select id="sel-contour-preset" style="padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;">
        <option value="vivid" selected>Vivid</option>
        <option value="soft">Soft</option>
        <option value="mono">Mono</option>
      </select>
    </label>
    <label title="Axis about which the polar orientation rotates (affects contour great-circles)" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      Axis
      <select id="sel-polar-axis" style="padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;">
        <option value="z">Z</option>
        <option value="y" selected>Y</option>
        <option value="x">X</option>
      </select>
    </label>
    <label title="Polar rotation angle (degrees)" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      θ <input type="range" id="range-polar-angle" min="-180" max="180" step="1" value="0" />
    </label>
    <button id="btn-reset-axis" title="Reset polar axis rotation" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Reset Axis</button>
  <button id="btn-reset-guides" title="Reset contour guides and normals presets to defaults" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Reset Guides</button>
    <button id="btn-export-globe-settings" title="Export globe/guide settings as JSON" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Export Settings</button>
    <label for="file-import-globe-settings" title="Import globe/guide settings from JSON" style="display:inline-flex;align-items:center;gap:0.3em;border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">
      Import Settings <input type="file" id="file-import-globe-settings" accept="application/json,.json" style="display:none" />
    </label>
    <label title="Number of clusters k" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">k <input type="number" id="num-k" min="3" max="24" step="1" value="6" style="width:64px;padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;"/></label>
    <label title="Max results placed on planet" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">N <input type="number" id="num-n" min="20" max="2000" step="10" value="200" style="width:72px;padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;"/></label>
  <button id="btn-show-globe" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Show on Globe</button>
    <button id="btn-hide-globe" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Hide Globe</button>
  <button id="btn-export-planet" title="Export current globe placements" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Export Planet</button>
  <input id="my-text" placeholder="My position text (e.g., 'INTP researcher')" style="flex:1 1 auto;min-width:120px;padding:0.25em 0.5em;border:1px solid #bbb;border-radius:6px;"/>
  <button id="btn-my-pos" title="Embed text and show nearest cluster" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Set My Position</button>
    <label title="Show lines from centroids to points" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      <input type="checkbox" id="chk-globe-links"/> Links
    </label>
    <label title="Show cluster labels" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      <input type="checkbox" id="chk-globe-labels"/> Labels
    </label>
    <label title="Show latitude/longitude grid" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      <input type="checkbox" id="chk-globe-grid" checked/> Grid
    </label>
    <label title="Auto place results on globe after search" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      <input type="checkbox" id="chk-auto-globe"/> Auto
    </label>
    <label title="Auto Globe threshold (min results)" style="display:inline-flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
      Auto N≥ <input type="number" id="num-auto-threshold" min="20" max="2000" step="10" value="100" style="width:72px;padding:0.25em 0.4em;border:1px solid #bbb;border-radius:6px;"/>
    </label>
  </div>
  <div id="planet-container" style="display:none;position:fixed;inset:0 0 0 0;background:linear-gradient(180deg, #07121f 0%, #0b1526 100%);z-index:50;">
    <div id="planet-sidebar" style="position:absolute;right:16px;top:16px;bottom:16px;width:360px;max-width:80vw;background:rgba(15,23,42,0.88);backdrop-filter: blur(6px);border:1px solid rgba(255,255,255,0.08);border-radius:12px;overflow:auto;padding:12px;box-shadow:0 8px 24px rgba(0,0,0,0.35);"></div>
  </div>
  <div id="toast" style="position:fixed;bottom:16px;left:16px;max-width:60vw;background:#333;color:#fff;padding:0.6em 0.8em;border-radius:6px;opacity:0;transform:translateY(10px);transition:opacity 0.2s, transform 0.2s;z-index:1006;"></div>
  <div id="search-results" style="max-height:260px;overflow-y:auto;"></div>
  `;
  document.body.appendChild(searchDiv);
  // Keep search panel above planet overlay
  try { searchDiv.style.zIndex = 1005; } catch {}

  const input = searchDiv.querySelector('#search-input');
  const resultsDiv = searchDiv.querySelector('#search-results');
  const statusDiv = searchDiv.querySelector('#search-status');
  const progressDiv = searchDiv.querySelector('#search-progress');
  const btnRebuild = searchDiv.querySelector('#btn-rebuild-index');
  const btnCancel = searchDiv.querySelector('#btn-cancel-build');
  const btnClear = searchDiv.querySelector('#btn-clear-cache');
  const btnRebuildForce = searchDiv.querySelector('#btn-rebuild-force');
  const btnParquet = searchDiv.querySelector('#btn-load-parquet');
  const btnProfiles = searchDiv.querySelector('#btn-load-profiles');
  const btnExportIdx = searchDiv.querySelector('#btn-export-index');
  const btnExportVec = searchDiv.querySelector('#btn-export-vectors');
  const btnExportProf = searchDiv.querySelector('#btn-export-profiles');
  const btnExportProj = searchDiv.querySelector('#btn-export-proj');
  const btnExportManifest = searchDiv.querySelector('#btn-export-manifest');
  const btnExportIdMap = searchDiv.querySelector('#btn-export-idmap');
  const btnExportCombined = searchDiv.querySelector('#btn-export-combined');
  const btnHealthReport = searchDiv.querySelector('#btn-health-report');
  const numEf = searchDiv.querySelector('#num-efsearch');
  const numEfc = searchDiv.querySelector('#num-efc');
  const fileImportIdx = searchDiv.querySelector('#file-import-index');
  const selVectors = searchDiv.querySelector('#sel-vectors');
  const selProfiles = searchDiv.querySelector('#sel-profiles');
  const chkAuto = searchDiv.querySelector('#chk-auto-parquet');
  const chkNoCache = searchDiv.querySelector('#chk-no-cache');
  const chkDebug = searchDiv.querySelector('#chk-debug');
  const chkInlinePca = searchDiv.querySelector('#chk-inline-pca');
  const chkDisableWorker = searchDiv.querySelector('#chk-disable-worker');
  const chkInlineKMeans = searchDiv.querySelector('#chk-inline-kmeans');
  const debugPre = searchDiv.querySelector('#debug-log');
  const btnClearDebug = searchDiv.querySelector('#btn-clear-debug');
  const btnCopyDebug = searchDiv.querySelector('#btn-copy-debug');
  const progressFill = searchDiv.querySelector('#progress-bar-fill');
  // Move planet-controls into the sidebar and apply modern styles after DOM is ready
  window.addEventListener('DOMContentLoaded', () => {
    setTimeout(() => {
      const planetSidebar = document.getElementById('planet-sidebar');
      const planetControls = document.getElementById('planet-controls');
      if (planetControls && planetSidebar && planetControls.parentElement !== planetSidebar) {
        planetSidebar.insertBefore(planetControls, planetSidebar.firstChild);
        planetControls.style.marginBottom = '1.2em';
        planetControls.style.background = 'linear-gradient(120deg, rgba(255,255,255,0.18) 0%, rgba(200,220,255,0.22) 100%)';
        planetControls.style.borderRadius = '14px';
        planetControls.style.padding = '1em 1.2em 1em 1.2em';
        planetControls.style.boxShadow = '0 4px 24px 0 rgba(120,180,255,0.13), 0 1.5px 6px rgba(0,0,0,0.07)';
        planetControls.style.flexWrap = 'wrap';
        planetControls.style.gap = '1em';
        planetControls.style.display = 'flex';
        planetControls.style.alignItems = 'center';
        planetControls.style.justifyContent = 'flex-start';
        planetControls.style.backdropFilter = 'blur(6px)';
        planetControls.style.border = '1.5px solid rgba(255,255,255,0.18)';
      }
      // Sidebar: glassy opalescent effect
      if (planetSidebar) {
        planetSidebar.style.background = 'linear-gradient(120deg, rgba(255,255,255,0.22) 0%, rgba(180,220,255,0.18) 100%)';
        planetSidebar.style.backdropFilter = 'blur(12px)';
        planetSidebar.style.border = '1.5px solid rgba(255,255,255,0.18)';
        planetSidebar.style.boxShadow = '0 8px 32px 0 rgba(120,180,255,0.18), 0 1.5px 6px rgba(0,0,0,0.07)';
      }
      // Background: vibrant prismatic gradient
      const planetContainer = document.getElementById('planet-container');
      if (planetContainer) {
        planetContainer.style.background = 'linear-gradient(135deg, #a8edea 0%, #fed6e3 40%, #fcb69f 70%, #a1c4fd 100%)';
      }
    }, 0);
  });
  const numK = searchDiv.querySelector('#num-k');
  const numN = searchDiv.querySelector('#num-n');
  const selPlacement = searchDiv.querySelector('#sel-placement');
    const selSurface = searchDiv.querySelector('#sel-surface');
  const btnReininPalette = searchDiv.querySelector('#btn-reinin-palette');
  const btnReininLegend = searchDiv.querySelector('#btn-reinin-legend');
  const btnShowGlobe = searchDiv.querySelector('#btn-show-globe');
  const btnHideGlobe = searchDiv.querySelector('#btn-hide-globe');
  const btnExportPlanet = searchDiv.querySelector('#btn-export-planet');
  const myText = searchDiv.querySelector('#my-text');
  const btnMyPos = searchDiv.querySelector('#btn-my-pos');
  const chkGlobeLinks = searchDiv.querySelector('#chk-globe-links');
  const chkGlobeLabels = searchDiv.querySelector('#chk-globe-labels');
  const chkGlobeGrid = searchDiv.querySelector('#chk-globe-grid');
  const chkAutoGlobe = searchDiv.querySelector('#chk-auto-globe');
  const numAutoThreshold = searchDiv.querySelector('#num-auto-threshold');
  const chkContours = searchDiv.querySelector('#chk-contours');
  const rangeContourWidth = searchDiv.querySelector('#range-contour-width');
  const selContourPreset = searchDiv.querySelector('#sel-contour-preset');
  const selPolarAxis = searchDiv.querySelector('#sel-polar-axis');
  const rangePolarAngle = searchDiv.querySelector('#range-polar-angle');
  const btnResetAxis = searchDiv.querySelector('#btn-reset-axis');
  const selNormalsPreset = searchDiv.querySelector('#sel-normals-preset');
  const chkFuncAware = searchDiv.querySelector('#chk-func-aware');
  const rangeFuncOffset = searchDiv.querySelector('#range-func-offset');
  const valFuncOffset = searchDiv.querySelector('#val-func-offset');
  const rangeWDom = searchDiv.querySelector('#range-w-dominant');
  const valWDom = searchDiv.querySelector('#val-w-dominant');
  const rangeWAux = searchDiv.querySelector('#range-w-aux');
  const valWAux = searchDiv.querySelector('#val-w-aux');
  const rangeWTer = searchDiv.querySelector('#range-w-ter');
  const valWTer = searchDiv.querySelector('#val-w-ter');
  const rangeWInf = searchDiv.querySelector('#range-w-inf');
  const valWInf = searchDiv.querySelector('#val-w-inf');
  const rangeFuncRoll = searchDiv.querySelector('#range-func-roll');
  const valFuncRoll = searchDiv.querySelector('#val-func-roll');
  const rangeContourIntensity = searchDiv.querySelector('#range-contour-intensity');
  const valContourWidth = searchDiv.querySelector('#val-contour-width');
  const valContourIntensity = searchDiv.querySelector('#val-contour-intensity');
  const btnResetGuides = searchDiv.querySelector('#btn-reset-guides');
  const btnExportGlobeSettings = searchDiv.querySelector('#btn-export-globe-settings');
  const fileImportGlobeSettings = searchDiv.querySelector('#file-import-globe-settings');
  const customNormalsWrap = searchDiv.querySelector('#custom-normals');
  const inpNormE = searchDiv.querySelector('#norm-e');
  const inpNormN = searchDiv.querySelector('#norm-n');
  const inpNormF = searchDiv.querySelector('#norm-f');
  const inpNormP = searchDiv.querySelector('#norm-p');
  let planetContainer = document.getElementById('planet-container');
  let planetSidebar = document.getElementById('planet-sidebar');
  // Ensure the planet overlay lives at <body> level so it never occludes the search panel unintentionally
  try {
    if (planetContainer && planetContainer.parentElement !== document.body) {
      planetContainer.parentElement.removeChild(planetContainer);
      document.body.appendChild(planetContainer);
    }
    // Always ensure sidebar exists and is visible
    if (planetContainer && !planetSidebar) {
      planetSidebar = document.createElement('div');
      planetSidebar.id = 'planet-sidebar';
      planetSidebar.style.position = 'absolute';
      planetSidebar.style.right = '16px';
      planetSidebar.style.top = '16px';
      planetSidebar.style.bottom = '16px';
      planetSidebar.style.width = '360px';
      planetSidebar.style.maxWidth = '80vw';
      planetSidebar.style.background = 'linear-gradient(120deg, rgba(255,255,255,0.22) 0%, rgba(180,220,255,0.18) 100%)';
      planetSidebar.style.backdropFilter = 'blur(12px)';
      planetSidebar.style.border = '1.5px solid rgba(255,255,255,0.18)';
      planetSidebar.style.borderRadius = '12px';
      planetSidebar.style.overflow = 'auto';
      planetSidebar.style.padding = '12px';
      planetSidebar.style.boxShadow = '0 8px 32px 0 rgba(120,180,255,0.18), 0 1.5px 6px rgba(0,0,0,0.07)';
      planetContainer.appendChild(planetSidebar);
    }
    if (planetContainer) planetContainer.style.zIndex = '50';
    if (planetSidebar) planetSidebar.style.display = 'block';
  } catch {}
  // Info card for globe selections
  const planetInfo = (() => {
    try {
      const el = document.createElement('div');
      el.id = 'planet-infocard';
      el.style.position = 'relative';
      el.style.background = 'rgba(255,255,255,0.04)';
      el.style.color = '#fff';
      el.style.padding = '10px 12px';
      el.style.borderRadius = '8px';
      el.style.fontSize = '12px';
      el.style.lineHeight = '1.35';
      el.style.boxShadow = 'inset 0 0 0 1px rgba(255,255,255,0.05)';
      el.style.display = 'none';
      if (planetSidebar) planetSidebar.appendChild(el);
      return el;
    } catch { return null; }
  })();
  // Busy overlay for globe clustering
  const globeBusy = (() => {
    try {
      if (!planetContainer) return null;
      const el = document.createElement('div');
      el.id = 'planet-busy';
      el.style.position = 'absolute';
      el.style.inset = '0 0 0 0';
      el.style.background = 'rgba(0,0,0,0.45)';
      el.style.display = 'none';
      el.style.alignItems = 'center';
      el.style.justifyContent = 'center';
      el.style.zIndex = '2';
      const box = document.createElement('div');
      box.style.background = 'rgba(20,20,28,0.92)';
      box.style.color = '#fff';
      box.style.padding = '10px 12px';
      box.style.borderRadius = '8px';
      box.style.display = 'flex';
      box.style.alignItems = 'center';
      box.style.gap = '10px';
      const sp = document.createElement('div');
      sp.style.width = '16px'; sp.style.height = '16px';
      sp.style.border = '2px solid rgba(255,255,255,0.3)';
      sp.style.borderTopColor = '#fff';
      sp.style.borderRadius = '50%';
      sp.style.animation = 'spin 0.8s linear infinite';
      const style = document.createElement('style');
      style.textContent = '@keyframes spin{to{transform:rotate(360deg)}}';
      el.appendChild(style);
      const txt = document.createElement('div');
      txt.id = 'planet-busy-text';
      txt.textContent = 'Clustering…';
      const cancel = document.createElement('button');
      cancel.textContent = 'Cancel';
      cancel.id = 'btn-cancel-kmeans';
      cancel.style.border = '1px solid #bbb';
      cancel.style.background = '#fff';
      cancel.style.color = '#000';
      cancel.style.borderRadius = '6px';
      cancel.style.padding = '0.2em 0.5em';
      cancel.style.cursor = 'pointer';
      box.appendChild(sp); box.appendChild(txt); box.appendChild(cancel);
      el.appendChild(box);
      planetContainer.appendChild(el);
      return el;
    } catch { return null; }
  })();
  function showGlobeBusy(message) { try { if (globeBusy) { const t = globeBusy.querySelector('#planet-busy-text'); if (t && message) t.textContent = message; globeBusy.style.display = 'flex'; } } catch {} }
  function updateGlobeBusy(message){ try { if (globeBusy) { const t = globeBusy.querySelector('#planet-busy-text'); if (t && message) t.textContent = message; } } catch {} }
  function hideGlobeBusy(){ try { if (globeBusy) globeBusy.style.display = 'none'; } catch {} }
  let kmeansWorkerRef = null;
  const toastEl = searchDiv.querySelector('#toast');
  let currentResults = [];
  let selectedIndex = -1;
  let planetVisible = false;
  let lastKMeans = null; // {centroids: Float32Array[] , labels: Int32Array, placements: [{id,lat,lon}], meta:[{cid,label}], vecs: Float32Array[], cidToCluster: Map}
  let lastGlobeSig = '';
  // Persisted/customizable Reinin palette (24 RGB triplets in 0..1)
  let reininColorsLocal = null;
  // Persisted/customizable Reinin names (24 strings)
  let reininNamesLocal = null;
  // Density preference for compact Reinin legend: 'compact' | 'cozy'
  let reininLegendDensity = (function(){ try { return localStorage.getItem('compass_reinin_legend_density') || 'compact'; } catch { return 'compact'; } })();

  // Default Reinin dichotomy names (user-provided), grouped across axes.
  // Positive poles (for +faces):
  const REININ_POS = [
    'Extravert',          // 0
    'Ethical',            // 1
    'Sensing',            // 2
    'Dynamic',            // 3
    'Negative',           // 4
    'Declaring',          // 5
    'Strategic',          // 6
    'Emotive',            // 7
    'Irrational',         // 8
    'Result-oriented',    // 9
    'Obstinate',          // 10
    'Farsighted'          // 11
  ];
  // Negative poles (for -faces), order-aligned with REININ_POS
  const REININ_NEG = [
    'Introvert',          // 0
    'Logical',            // 1
    'Intuitive',          // 2
    'Static',             // 3
    'Positive',           // 4
    'Asking',             // 5
    'Tactical',           // 6
    'Constructive',       // 7
    'Rational',           // 8
    'Process-oriented',   // 9
    'Yielding',           // 10
    'Carefree'            // 11
  ];
  // Face order matches globe.js: 0:+X,1:-X,2:+Y,3:-Y,4:+Z,5:-Z.
  // Each face has tri indices 0..3. We map four dichotomies per axis with a stable tri order.
  function defaultReininNames24() {
    const names = new Array(24);
    // +X group: Extravert, Declaring, Result-oriented, Obstinate
    const Xp = [0, 5, 9, 10];
    // -X group: Introvert, Asking, Process-oriented, Yielding
    const Xn = [0, 5, 9, 10];
    // +Y group: Ethical, Emotive, Irrational, Strategic
    const Yp = [1, 7, 8, 6];
    // -Y group: Logical, Constructive, Rational, Tactical
    const Yn = [1, 7, 8, 6];
    // +Z group: Sensing, Dynamic, Negative, Farsighted
    const Zp = [2, 3, 4, 11];
    // -Z group: Intuitive, Static, Positive, Carefree
    const Zn = [2, 3, 4, 11];
    // Helper to assign four names for a face
    const put = (face, idxs, arr) => {
      for (let t = 0; t < 4; t++) names[face*4 + t] = arr[idxs[t]];
    };
    put(0, Xp, REININ_POS); // +X
    put(1, Xn, REININ_NEG); // -X
    put(2, Yp, REININ_POS); // +Y
    put(3, Yn, REININ_NEG); // -Y
    put(4, Zp, REININ_POS); // +Z
    put(5, Zn, REININ_NEG); // -Z
    return names;
  }

  // Helper: face label for Reinin 24 faces (kept consistent across UI)
  function reininFaceLabel(idx){
    try {
      const base = Math.floor(idx/4); const q = idx%4;
      const axis = base===0?'+X':base===1?'-X':base===2?'+Y':base===3?'-Y':base===4?'+Z':'-Z';
      const quad = q===0?'Q1':q===1?'Q2':q===2?'Q3':'Q4';
      return `${axis} · ${quad}`;
    } catch { return `Face ${idx}`; }
  }

  // Compact, read-only Reinin legend under controls
  function getOrCreateReininLegendView(){
    try {
      let v = document.getElementById('reinin-legend-view');
      if (!v) {
        v = document.createElement('div');
        v.id = 'reinin-legend-view';
        v.style.marginTop = '0.6em';
        v.style.padding = '0.75em 0.9em';
        v.style.border = '1px solid rgba(255,255,255,0.14)';
        v.style.borderRadius = '10px';
        v.style.background = 'rgba(255,255,255,0.10)';
        v.style.display = 'none';
        const h = document.createElement('div');
        h.style.display = 'flex';
        h.style.alignItems = 'center';
        h.style.justifyContent = 'space-between';
        h.style.marginBottom = '6px';
        const t = document.createElement('div'); t.textContent = 'Reinin faces'; t.style.color = '#fff'; t.style.fontWeight = '600'; t.style.fontSize = '13px'; t.style.cursor = 'pointer';
        // Collapse toggle button
        const c = document.createElement('button');
        c.id = 'reinin-legend-collapse';
        c.title = 'Collapse/expand';
        c.textContent = '▾';
        c.style.border = '1px solid #bbb';
        c.style.background = '#fff';
        c.style.color = '#000';
        c.style.borderRadius = '6px';
        c.style.padding = '0.1em 0.45em';
        c.style.cursor = 'pointer';
        c.style.fontSize = '12px';
  const e = document.createElement('button'); e.textContent = 'Edit'; e.style.border = '1px solid #bbb'; e.style.background = '#fff'; e.style.color = '#000'; e.style.borderRadius = '6px'; e.style.padding = '0.15em 0.5em'; e.style.cursor = 'pointer'; e.style.fontSize = '12px'; e.onclick = () => { try { const b = document.getElementById('btn-reinin-legend'); if (b) b.click(); } catch {} };
  const dBtn = document.createElement('button'); dBtn.id = 'reinin-legend-density'; dBtn.textContent = (reininLegendDensity === 'compact' ? 'Cozy' : 'Compact'); dBtn.title = 'Toggle legend density'; dBtn.style.border = '1px solid #bbb'; dBtn.style.background = '#fff'; dBtn.style.color = '#000'; dBtn.style.borderRadius = '6px'; dBtn.style.padding = '0.1em 0.5em'; dBtn.style.cursor = 'pointer'; dBtn.style.fontSize = '12px'; dBtn.onclick = () => { try { reininLegendDensity = (reininLegendDensity === 'compact' ? 'cozy' : 'compact'); localStorage.setItem('compass_reinin_legend_density', reininLegendDensity); renderReininLegendView(); savePrefs(); } catch {} };
  const btnWrap = document.createElement('div'); btnWrap.style.display = 'flex'; btnWrap.style.gap = '6px';
        const r = document.createElement('button');
  r.textContent = 'Reset';
  r.title = 'Reset collapse preference';
  r.style.border = '1px solid #bbb'; r.style.background = '#fff'; r.style.color = '#000'; r.style.borderRadius = '6px'; r.style.padding = '0.1em 0.5em'; r.style.cursor = 'pointer'; r.style.fontSize = '12px';
  r.onclick = () => { try { localStorage.removeItem('compass_reinin_legend_collapsed'); applyCollapsed(); if (typeof toast === 'function') toast('Legend collapse reset'); } catch {} };
        const copyBtn = document.createElement('button');
        copyBtn.textContent = 'Copy';
        copyBtn.title = 'Copy Reinin legend (names+colors) as JSON';
        copyBtn.style.border = '1px solid #bbb'; copyBtn.style.background = '#fff'; copyBtn.style.color = '#000'; copyBtn.style.borderRadius = '6px'; copyBtn.style.padding = '0.1em 0.5em'; copyBtn.style.cursor = 'pointer'; copyBtn.style.fontSize = '12px';
        copyBtn.onclick = async () => {
          try {
            ensureReininNames(); ensureReininColors();
            const payload = { names: reininNamesLocal, colors: reininColorsLocal };
            await navigator.clipboard.writeText(JSON.stringify(payload));
            if (typeof toast === 'function') toast('Legend copied');
          } catch { if (typeof toast === 'function') toast('Copy failed', false); }
        };
  btnWrap.appendChild(c); btnWrap.appendChild(e); btnWrap.appendChild(dBtn); btnWrap.appendChild(copyBtn); btnWrap.appendChild(r);
        h.appendChild(t); h.appendChild(btnWrap); v.appendChild(h);
        const grid = document.createElement('div'); grid.id = 'reinin-legend-grid-view'; grid.style.display = 'grid'; grid.style.gridTemplateColumns = 'auto 1fr'; grid.style.gap = '6px 10px'; v.appendChild(grid);
        // Toggle behavior (title also toggles)
        const applyCollapsed = () => {
          try {
            let collapsedPref = localStorage.getItem('compass_reinin_legend_collapsed');
            // Default to collapsed on small screens if no pref exists
            if (collapsedPref == null) {
              try {
                const small = (window.matchMedia && window.matchMedia('(max-width: 900px)').matches) || (window.innerWidth && window.innerWidth < 900);
                if (small) {
                  localStorage.setItem('compass_reinin_legend_collapsed', '1');
                  collapsedPref = '1';
                }
              } catch {}
            }
            const collapsed = collapsedPref === '1';
            grid.style.display = collapsed ? 'none' : 'grid';
            c.textContent = collapsed ? '▸' : '▾';
          } catch {}
        };
        const toggleCollapsed = () => {
          try {
            const collapsed = localStorage.getItem('compass_reinin_legend_collapsed') === '1';
            localStorage.setItem('compass_reinin_legend_collapsed', collapsed ? '0' : '1');
            applyCollapsed();
          } catch {}
        };
        c.onclick = toggleCollapsed;
        t.onclick = toggleCollapsed;
        applyCollapsed();
        if (planetSidebar) planetSidebar.appendChild(v); else document.body.appendChild(v);
      }
      // If sidebar exists now, ensure legend lives inside it
      try { if (planetSidebar && v.parentElement !== planetSidebar) { v.parentElement && v.parentElement.removeChild(v); planetSidebar.appendChild(v); } } catch {}
      return v;
    } catch { return null; }
  }

  function ensureReininNames(){
    try {
      if (!Array.isArray(reininNamesLocal) || reininNamesLocal.length !== 24) {
        // Seed with trait-based defaults mapped across ±X/±Y/±Z faces.
        reininNamesLocal = defaultReininNames24();
      }
    } catch {}
  }

  function ensureReininColors(){
    try {
      if (!Array.isArray(reininColorsLocal) || reininColorsLocal.length !== 24) {
        // Seed with default palette consistent with ensureReininSurface
        const hex = [
          0x1f77b4,0xff7f0e,0x2ca02c,0xd62728,
          0x9467bd,0x8c564b,0xe377c2,0x7f7f7f,
          0xbcbd22,0x17becf,0x8dd3c7,0xffffb3,
          0xbebada,0xfb8072,0x80b1d3,0xfdb462,
          0xa6cee3,0x1f78b4,0xb2df8a,0x33a02c,
          0xfb9a99,0xe31a1c,0xfdbf6f,0xff7f00
        ];
        reininColorsLocal = hex.map(h => [((h>>16)&255)/255, ((h>>8)&255)/255, (h&255)/255]);
      }
    } catch {}
  }

  function renderReininLegendView(){
    try {
      const v = getOrCreateReininLegendView();
      if (!v) return;
  const on = (selSurface && selSurface.value === 'reinin');
  v.style.display = on ? 'block' : 'none';
  // Always ensure names/colors and push names to the globe, even if Reinin surface isn't active yet
  ensureReininNames(); ensureReininColors();
  try { if (window.GLOBE && window.GLOBE.setReininFaceNames) window.GLOBE.setReininFaceNames(reininNamesLocal); } catch {}
  if (!on) return;
      const grid = v.querySelector('#reinin-legend-grid-view');
      if (!grid) return;
      // Ensure collapsed state applied
      try {
        const collapsed = localStorage.getItem('compass_reinin_legend_collapsed') === '1';
        grid.style.display = collapsed ? 'none' : 'grid';
        const c = v.querySelector('#reinin-legend-collapse'); if (c) c.textContent = collapsed ? '▸' : '▾';
  const d = v.querySelector('#reinin-legend-density'); if (d) d.textContent = (reininLegendDensity === 'compact' ? 'Cozy' : 'Compact');
      } catch {}
      // Apply density styles (compact vs cozy)
      const dense = (reininLegendDensity === 'compact');
      v.style.lineHeight = dense ? '1.15' : '1.4';
      v.style.fontSize = dense ? '12px' : '14px';
      grid.style.gap = dense ? '5px 10px' : '9px 14px';
      grid.innerHTML = '';
  for (let i = 0; i < 24; i++) {
        const sw = document.createElement('div');
        const sz = dense ? 16 : 20;
        sw.style.width = sz+'px'; sw.style.height = sz+'px'; sw.style.borderRadius = '4px'; sw.style.border = '1px solid rgba(255,255,255,0.2)';
        sw.style.cursor = 'pointer';
        const c = reininColorsLocal[i] || [0.5,0.5,0.5];
        const hex = `#${[0,1,2].map(j => Math.round((c[j]||0)*255).toString(16).padStart(2,'0')).join('')}`;
        sw.style.background = hex;
        const row = document.createElement('div');
        row.style.display = 'flex'; row.style.alignItems = 'center'; row.style.gap = dense ? '6px' : '10px';
  const idx = document.createElement('span'); idx.textContent = String(i+1).padStart(2,'0'); idx.style.color = 'rgba(255,255,255,0.7)'; idx.style.fontSize = dense ? '10px' : '12px'; idx.style.minWidth = dense ? '18px' : '24px';
        const name = document.createElement('div');
        name.textContent = reininNamesLocal[i] || reininFaceLabel(i);
        name.style.color = '#fff'; name.style.fontSize = dense ? '12px' : '14px'; name.style.cursor = 'pointer';
        const openEditorAt = () => {
          try {
            const b = document.getElementById('btn-reinin-legend'); if (b) b.click();
            // Focus the corresponding input after the editor is created
            setTimeout(() => {
              try {
                const ed = document.getElementById('reinin-legend-editor');
                const gridEd = ed && ed.querySelector('#reinin-legend-grid');
                const inp = gridEd && gridEd.querySelector(`input[type="text"][data-index="${i}"]`);
                if (inp) { inp.focus(); inp.select && inp.select(); }
              } catch {}
            }, 0);
          } catch {}
        };
  // Click the swatch to focus the globe on this face; click the name to open editor
  sw.onclick = () => { try { if (window.GLOBE && window.GLOBE.focusReininFace) window.GLOBE.focusReininFace(i, true); } catch {} };
  name.onclick = openEditorAt; row.onclick = null;
  // Hover highlight this face on the globe when in Reinin mode
  const setHighlight = (val) => { try { if (window.GLOBE && window.GLOBE.setReininHighlight && (selSurface && selSurface.value === 'reinin')) window.GLOBE.setReininHighlight(val ? i : -1); } catch {} };
  sw.onmouseenter = () => setHighlight(true);
  sw.onmouseleave = () => setHighlight(false);
  name.onmouseenter = () => setHighlight(true);
  name.onmouseleave = () => setHighlight(false);
        grid.appendChild(sw);
        row.appendChild(idx); row.appendChild(name);
        grid.appendChild(row);
      }
    } catch {}
  }

  // Small helper to show a busy state on buttons consistently
  function setBusy(btn, text = 'Working…') {
    if (!btn) return () => {};
    if (!btn.dataset) btn.dataset = {};
    if (!btn.dataset.originalText) btn.dataset.originalText = btn.textContent || '';
    btn.disabled = true;
    btn.textContent = text;
    btn.style.opacity = '0.8';
    return () => {
      btn.disabled = false;
      btn.textContent = btn.dataset.originalText || '';
      btn.style.opacity = '1';
    };
  }

  function savePrefs() {
    try {
      const prefs = {
        vectors: selVectors && selVectors.value || '',
        profiles: selProfiles && selProfiles.value || '',
  autoParquet: !!(chkAuto && chkAuto.checked),
  noCache: !!(chkNoCache && chkNoCache.checked),
  debug: !!(chkDebug && chkDebug.checked),
  efSearch: numEf ? (parseInt(numEf.value, 10) || 64) : 64,
  efConstruction: numEfc ? (parseInt(numEfc.value, 10) || 200) : 200,
  globe: {
    links: !!(chkGlobeLinks && chkGlobeLinks.checked),
  labels: !!(chkGlobeLabels && chkGlobeLabels.checked),
  grid: !!(chkGlobeGrid && chkGlobeGrid.checked),
  contours: !!(chkContours && chkContours.checked),
  contourWidth: rangeContourWidth ? (parseFloat(rangeContourWidth.value) || 0.09) : 0.09,
  contourIntensity: rangeContourIntensity ? (parseFloat(rangeContourIntensity.value) || 0.55) : 0.55,
  contourPreset: selContourPreset ? (selContourPreset.value || 'vivid') : 'vivid',
  normalsPreset: selNormalsPreset ? (selNormalsPreset.value || 'globe') : 'globe',
  funcAware: !!(chkFuncAware && chkFuncAware.checked),
  funcOffsetDeg: rangeFuncOffset ? (parseFloat(rangeFuncOffset.value) || 10) : 10,
  funcRoleWeights: {
    dominant: rangeWDom ? (parseFloat(rangeWDom.value)||0.5) : 0.5,
    auxiliary: rangeWAux ? (parseFloat(rangeWAux.value)||0.3) : 0.3,
    tertiary: rangeWTer ? (parseFloat(rangeWTer.value)||0.15) : 0.15,
    inferior: rangeWInf ? (parseFloat(rangeWInf.value)||0.05) : 0.05
  },
  funcAttitudeDeg: rangeFuncRoll ? (parseFloat(rangeFuncRoll.value) || 6) : 6,
  customNormals: (function(){
    try {
      if (!selNormalsPreset || selNormalsPreset.value !== 'custom') return undefined;
      const v = getCustomNormalsFromInputs();
      return v && v.length === 4 ? v : undefined;
    } catch { return undefined; }
  })(),
  polarAxis: selPolarAxis ? (selPolarAxis.value || 'y') : 'y',
  polarAngle: rangePolarAngle ? (parseInt(rangePolarAngle.value, 10) || 0) : 0,
  auto: !!(chkAutoGlobe && chkAutoGlobe.checked),
  autoThreshold: numAutoThreshold ? (parseInt(numAutoThreshold.value, 10) || 100) : 100,
  surface: selSurface ? (selSurface.value || 'prismatic') : 'prismatic',
  reininColors: (function(){ try { return Array.isArray(reininColorsLocal) && reininColorsLocal.length===24 ? reininColorsLocal : undefined; } catch { return undefined; } })(),
  reininNames: (function(){ try { return Array.isArray(reininNamesLocal) && reininNamesLocal.length===24 ? reininNamesLocal : undefined; } catch { return undefined; } })(),
  reininLegendCollapsed: (function(){ try { return localStorage.getItem('compass_reinin_legend_collapsed') === '1'; } catch { return undefined; } })(),
  reininLegendDensity: (function(){ try { return reininLegendDensity || localStorage.getItem('compass_reinin_legend_density') || 'compact'; } catch { return undefined; } })()
  }
  ,
  inlineKMeans: !!(chkInlineKMeans && chkInlineKMeans.checked)
      };
      localStorage.setItem('compass_parquet_prefs', JSON.stringify(prefs));
    } catch {}
  }

  function loadPrefs() {
    try {
      const raw = localStorage.getItem('compass_parquet_prefs');
      return raw ? JSON.parse(raw) : null;
    } catch { return null; }
  }

  function baseName(p){ try{ return String(p||'').split('/').pop(); }catch{ return String(p||''); } }

  function logDebug(msg) {
    try {
      if (!debugPre) return;
      const prior = (debugPre.textContent || '').split('\n').filter(Boolean);
      const ts = new Date().toISOString().slice(11, 19);
      prior.push(`[${ts}] ${msg}`);
      const kept = prior.slice(-200);
      debugPre.textContent = kept.join('\n');
      if (chkDebug && chkDebug.checked) {
        debugPre.style.display = 'block';
        // auto-scroll to bottom when visible
        try { debugPre.scrollTop = debugPre.scrollHeight; } catch {}
      }
    } catch {}
  }

  async function populateDatasetSelectors() {
    try {
      const resp = await fetch('/dataset');
      if (!resp.ok) throw new Error(String(resp.status));
      const js = await resp.json();
      const files = Array.isArray(js.files) ? js.files : [];
      const vecCandidates = files.filter(f => /vector|emb|profile_vectors/i.test(f) && /\.parquet$/i.test(f));
      const profCandidates = files.filter(f => /profile/i.test(f) && /normal/i.test(f) && /\.parquet$/i.test(f));
      selVectors.innerHTML = '';
      selProfiles.innerHTML = '';
      const addOpt = (sel, name) => {
        const o = document.createElement('option');
        o.value = `/dataset/${name}`;
        o.textContent = name;
        sel.appendChild(o);
      };
      // Prepend JSON defaults
      if (selVectors) {
        const o = document.createElement('option'); o.value = '/pdb_profile_vectors.json'; o.textContent = 'JSON: pdb_profile_vectors.json'; selVectors.appendChild(o);
      }
      if (selProfiles) {
        const o = document.createElement('option'); o.value = '/pdb_profiles.json'; o.textContent = 'JSON: pdb_profiles.json'; selProfiles.appendChild(o);
      }
      if (vecCandidates.length) vecCandidates.forEach(n => addOpt(selVectors, n));
      else addOpt(selVectors, 'pdb_profile_vectors.parquet');
      if (profCandidates.length) profCandidates.forEach(n => addOpt(selProfiles, n));
      else addOpt(selProfiles, 'pdb_profiles_normalized.parquet');
      // Default selections to saved prefs or common names
      const prefs = loadPrefs();
      if (prefs && prefs.vectors) selVectors.value = prefs.vectors;
      else {
        const prefVec = Array.from(selVectors.options).find(o => /pdb_profile_vectors\.parquet$/i.test(o.value));
        if (prefVec) selVectors.value = prefVec.value;
      }
      if (prefs && prefs.profiles) selProfiles.value = prefs.profiles;
      else {
        const prefProf = Array.from(selProfiles.options).find(o => /pdb_profiles_normalized\.parquet$/i.test(o.value));
        if (prefProf) selProfiles.value = prefProf.value;
      }
  if (chkAuto) chkAuto.checked = !!(prefs && prefs.autoParquet);
  if (chkNoCache) chkNoCache.checked = !!(prefs && prefs.noCache);
  if (chkDebug) chkDebug.checked = !!(prefs && prefs.debug);
  if (numEf && prefs && typeof prefs.efSearch === 'number') numEf.value = String(prefs.efSearch);
  if (numEfc && prefs && typeof prefs.efConstruction === 'number') numEfc.value = String(prefs.efConstruction);
      // If first run (no prefs) and we have valid options, default-enable auto parquet
      if (!prefs && selVectors.options.length && selProfiles.options.length) {
        if (chkAuto) chkAuto.checked = true;
      }
      if (debugPre) debugPre.style.display = (chkDebug && chkDebug.checked) ? 'block' : 'none';
      // Globe options
      if (prefs && prefs.globe) {
        if (chkGlobeLinks) chkGlobeLinks.checked = !!prefs.globe.links;
        if (chkGlobeLabels) chkGlobeLabels.checked = !!prefs.globe.labels;
  if (chkAutoGlobe) chkAutoGlobe.checked = !!prefs.globe.auto;
  if (numAutoThreshold && typeof prefs.globe.autoThreshold === 'number') numAutoThreshold.value = String(prefs.globe.autoThreshold);
  // Contour/axis UI defaults
  if (chkContours) chkContours.checked = (Object.prototype.hasOwnProperty.call(prefs.globe, 'contours') ? !!prefs.globe.contours : true);
  if (rangeContourWidth && typeof prefs.globe.contourWidth === 'number') rangeContourWidth.value = String(prefs.globe.contourWidth);
  if (valContourWidth && rangeContourWidth) valContourWidth.textContent = Number(rangeContourWidth.value).toFixed(3);
  if (rangeContourIntensity && typeof prefs.globe.contourIntensity === 'number') rangeContourIntensity.value = String(prefs.globe.contourIntensity);
  if (valContourIntensity && rangeContourIntensity) valContourIntensity.textContent = Number(rangeContourIntensity.value).toFixed(2);
  if (selContourPreset && prefs.globe.contourPreset) selContourPreset.value = prefs.globe.contourPreset;
  if (selNormalsPreset && prefs.globe.normalsPreset) selNormalsPreset.value = prefs.globe.normalsPreset;
  if (typeof prefs.globe.funcAware === 'boolean' && chkFuncAware) chkFuncAware.checked = !!prefs.globe.funcAware;
  if (typeof prefs.globe.funcOffsetDeg === 'number' && rangeFuncOffset) { rangeFuncOffset.value = String(prefs.globe.funcOffsetDeg); if (valFuncOffset) valFuncOffset.textContent = `${Number(prefs.globe.funcOffsetDeg).toFixed(1)}°`; }
  if (prefs.globe.funcRoleWeights) {
    if (rangeWDom && typeof prefs.globe.funcRoleWeights.dominant==='number') { rangeWDom.value = String(prefs.globe.funcRoleWeights.dominant); if (valWDom) valWDom.textContent = Number(rangeWDom.value).toFixed(2); }
    if (rangeWAux && typeof prefs.globe.funcRoleWeights.auxiliary==='number') { rangeWAux.value = String(prefs.globe.funcRoleWeights.auxiliary); if (valWAux) valWAux.textContent = Number(rangeWAux.value).toFixed(2); }
    if (rangeWTer && typeof prefs.globe.funcRoleWeights.tertiary==='number') { rangeWTer.value = String(prefs.globe.funcRoleWeights.tertiary); if (valWTer) valWTer.textContent = Number(rangeWTer.value).toFixed(2); }
    if (rangeWInf && typeof prefs.globe.funcRoleWeights.inferior==='number') { rangeWInf.value = String(prefs.globe.funcRoleWeights.inferior); if (valWInf) valWInf.textContent = Number(rangeWInf.value).toFixed(2); }
  }
  if (typeof prefs.globe.funcAttitudeDeg === 'number' && rangeFuncRoll) { rangeFuncRoll.value = String(prefs.globe.funcAttitudeDeg); if (valFuncRoll) valFuncRoll.textContent = `${Number(prefs.globe.funcAttitudeDeg).toFixed(1)}°`; }
  // Populate custom normals inputs if available
  try {
    const cn = prefs.globe.customNormals;
    if (Array.isArray(cn) && cn.length === 4) setCustomNormalsInputs(cn);
  } catch {}
  if (selPolarAxis && prefs.globe.polarAxis) selPolarAxis.value = prefs.globe.polarAxis;
  if (rangePolarAngle && typeof prefs.globe.polarAngle === 'number') rangePolarAngle.value = String(prefs.globe.polarAngle);
      }
  if (chkInlineKMeans) chkInlineKMeans.checked = !!(prefs && prefs.inlineKMeans);
  // Reflect worker toggles from localStorage
  try { if (chkInlinePca) chkInlinePca.checked = (localStorage.getItem('vec_disable_pca_worker') === '1'); } catch {}
  try { if (chkDisableWorker) chkDisableWorker.checked = (localStorage.getItem('vec_disable_worker') === '1'); } catch {}
    } catch (e) {
      // Fallback to defaults
      if (selVectors && !selVectors.options.length) {
        const o = document.createElement('option'); o.value = '/dataset/pdb_profile_vectors.parquet'; o.textContent = 'pdb_profile_vectors.parquet'; selVectors.appendChild(o);
      }
      if (selProfiles && !selProfiles.options.length) {
        const o = document.createElement('option'); o.value = '/dataset/pdb_profiles_normalized.parquet'; o.textContent = 'pdb_profiles_normalized.parquet'; selProfiles.appendChild(o);
      }
      try {
        if (statusDiv) statusDiv.textContent = 'Tip: /dataset endpoint not reachable. Start the dataset server so Parquet files can be listed and loaded.';
      } catch {}
    }
  }

  async function renderResults(query) {
    const q = query.trim();
    if (!q) {
      resultsDiv.innerHTML = '';
      return;
    }
    resultsDiv.innerHTML = '<div style="color:#888;padding:0.5em;">Searching...</div>';
    try {
      if (!window.KNN || !window.KNN.search) {
        resultsDiv.innerHTML = '<div style="color:#c00;padding:0.5em;">KNN not ready. Loading dataset…</div>';
        return;
      }
      let filtered;
      let isVector = false;
      // Accept multiple id prefixes and IPFS-like CIDs without prefix
      const prefix = q.match(/^(cid|id|pid|uuid|uid):(.+)$/i);
      const ipfsLike = (!prefix && /^(Qm|bafy)[a-zA-Z0-9]+$/i.test(q));
      if ((prefix || ipfsLike) && window.VEC && window.VEC.similarByCid) {
        isVector = true;
        let targetCid = null;
        if (prefix) {
          const rawId = (prefix[2] || '').trim();
          // Use resolver to get canonical CID from any id
          const rec = (window.KNN && window.KNN.resolveId) ? window.KNN.resolveId(rawId) : (window.KNN && window.KNN.getByCid ? window.KNN.getByCid(rawId) : null);
          targetCid = rec && rec.cid ? rec.cid : (prefix[1].toLowerCase() === 'cid' ? rawId : null);
        } else if (ipfsLike) {
          targetCid = q.trim();
        }
        if (!targetCid) {
          resultsDiv.innerHTML = '<div style="color:#888;padding:0.5em;">ID not found in profiles.</div>';
          try { console.warn('[search] id resolution failed for', q, 'IDMAP size=', window.KNN && window.KNN.getIdMapSize ? window.KNN.getIdMapSize() : '?'); } catch {}
          return;
        }
        const hits = window.VEC.similarByCid(targetCid, 20);
        filtered = hits.map(h => {
          const rec = window.KNN && window.KNN.getByCid ? window.KNN.getByCid(h.cid) : null;
          const nm = (rec?.displayName) || (rec?.name) || h.cid;
          return { cid: h.cid, name: nm, displayName: nm, mbti: rec?.mbti, socionics: rec?.socionics, big5: rec?.big5, _score: h._score };
        });
      } else {
        // Resolve names for all results as well (not just vector neighbors)
        const base = window.KNN.search(q, 20);
        filtered = base.map(r => {
          const rec = (r && r.cid && window.KNN && window.KNN.getByCid) ? window.KNN.getByCid(r.cid) : null;
          const nm = (rec?.displayName) || (rec?.name) || r.displayName || r.name || r.cid || 'Unknown';
          // preserve original scoring/fields, but ensure name/displayName are set consistently
          return { ...r, name: nm, displayName: nm };
        });
      }
  resultsDiv.innerHTML = '';
      if (!filtered.length) {
        resultsDiv.innerHTML = '<div style="color:#888;padding:0.5em;">No results found.</div>';
        return;
      }
  currentResults = filtered;
  selectedIndex = 0;
      // Optional header with Add All when in vector mode
      if (isVector) {
        const header = document.createElement('div');
        header.style.display = 'flex';
        header.style.justifyContent = 'space-between';
        header.style.alignItems = 'center';
        header.style.padding = '0.4em 0.2em 0.6em 0.2em';
        const title = document.createElement('div');
        title.textContent = 'Vector neighbors';
        title.style.fontWeight = '600';
        title.style.color = '#333';
        const addAll = document.createElement('button');
        addAll.textContent = 'Add all';
        addAll.style.border = '1px solid #bbb';
        addAll.style.background = '#fff';
        addAll.style.borderRadius = '6px';
        addAll.style.padding = '0.3em 0.6em';
        addAll.style.cursor = 'pointer';
        addAll.onclick = () => {
          if (!window.personalityData) window.personalityData = [];
          const seen = new Set(window.personalityData.map(p => p.cid || p.label));
          let added = 0;
          for (const row of filtered) {
            const label = row.displayName || row.name || row.cid || 'Unknown';
            const key = row.cid || label;
            if (seen.has(key)) continue;
            const p = computePersonVisual(row);
            window.personalityData.push(p);
            seen.add(key);
            added++;
          }
          if (added && window.refreshCompass) window.refreshCompass();
          renderResults(input.value);
        };
        header.appendChild(title);
        header.appendChild(addAll);
        resultsDiv.appendChild(header);
      }
  filtered.forEach((row, idx) => {
        const person = computePersonVisual(row);
  const rowEl = document.createElement('div');
  rowEl.style.display = 'flex';
  rowEl.style.alignItems = 'center';
  rowEl.style.gap = '0.7em';
  rowEl.style.padding = '0.4em 0.7em';
  rowEl.style.cursor = 'pointer';
  rowEl.style.borderRadius = '6px';
  rowEl.style.transition = 'background 0.15s';
        rowEl.dataset.index = String(idx);
        const setHover = (hover) => {
          const sel = Number(rowEl.dataset.index) === selectedIndex;
          rowEl.style.background = sel ? '#e8f0ff' : (hover ? '#f5f5f5' : 'none');
        };
  rowEl.onmouseenter = () => setHover(true);
  rowEl.onmouseleave = () => setHover(false);
        rowEl.onclick = () => {
          if (!window.personalityData) window.personalityData = [];
          const exists = window.personalityData.some(p => (p.cid && person.cid) ? p.cid === person.cid : p.label === person.label);
          if (!exists) {
            window.personalityData.push(person);
            if (window.refreshCompass) window.refreshCompass();
          }
          try {
            if (planetVisible && window.GLOBE) {
              if (window.GLOBE.pulseCid) window.GLOBE.pulseCid(person.cid);
              if (lastKMeans && lastKMeans.cidToCluster) {
                const c = lastKMeans.cidToCluster.get(person.cid);
                if (typeof c === 'number' && window.GLOBE.highlightCluster) window.GLOBE.highlightCluster(c);
              }
            }
          } catch {}
          renderResults(input.value); // update highlight
        };
        // Icon
        const icon = document.createElement('div');
        icon.style.width = '28px';
        icon.style.height = '28px';
        icon.style.borderRadius = '50%';
        icon.style.background = '#fff';
        icon.style.display = 'flex';
        icon.style.alignItems = 'center';
        icon.style.justifyContent = 'center';
        icon.style.fontWeight = 'bold';
        icon.style.fontSize = '1em';
        icon.style.color = person.color;
        icon.textContent = person.label ? person.label[0] : '?';
        rowEl.appendChild(icon);
        // Name
        const nameWrap = document.createElement('div');
        nameWrap.style.display = 'flex';
        nameWrap.style.flexDirection = 'column';
        nameWrap.style.flex = '1 1 auto';
        const name = document.createElement('span');
        name.textContent = person.label;
        name.style.fontWeight = '500';
        name.style.color = person.color;
        const meta = document.createElement('div');
        meta.style.fontSize = '0.85em';
        meta.style.color = '#555';
        const tags = [row.mbti, row.socionics, row.big5].filter(Boolean).join(' · ');
        const sim = (typeof row._score === 'number' && isVector) ? ` · ${(row._score * 100).toFixed(0)}%` : '';
        meta.textContent = tags ? (tags + sim) : (sim ? sim.slice(3) : '');
        nameWrap.appendChild(name);
        nameWrap.appendChild(meta);
        rowEl.appendChild(nameWrap);
        // Status (added?)
        const added = document.createElement('div');
        const already = (window.personalityData || []).some(p => (p.cid && person.cid) ? p.cid === person.cid : p.label === person.label);
        added.textContent = already ? 'Added' : '';
        added.style.fontSize = '0.8em';
        added.style.color = already ? '#2ca02c' : '#999';
        rowEl.appendChild(added);
        resultsDiv.appendChild(rowEl);
      });
      highlightSelected();
  // reveal planet controls when we have results
  if (planetControls) planetControls.style.display = 'flex';

      // Auto Globe: if enabled and enough results, place them once per render
      try {
        const prefs = loadPrefs();
        const auto = !!(prefs && prefs.globe && prefs.globe.auto);
        const threshold = (prefs && prefs.globe && typeof prefs.globe.autoThreshold === 'number') ? prefs.globe.autoThreshold : 100;
        if (auto && currentResults.length >= threshold) {
          // debounce auto trigger slightly
          clearTimeout(renderResults.__tAuto);
          renderResults.__tAuto = setTimeout(() => { placeOnGlobe(currentResults); }, 50);
        }
      } catch {}
    } catch (err) {
      resultsDiv.innerHTML = `<div style='color:#c00;padding:0.5em;'>Error: ${err.message}</div>`;
    }
  }

  // Visually highlight the currently selected result row
  function highlightSelected() {
    try {
      const rows = resultsDiv ? resultsDiv.querySelectorAll('div[data-index]') : [];
      rows.forEach((el) => {
        const idx = parseInt(el.dataset.index || '-1', 10);
        const isSel = idx === selectedIndex;
        el.style.background = isSel ? '#e8f0ff' : 'none';
      });
    } catch {}
  }

  let tSearch;
  input.addEventListener('input', () => {
    clearTimeout(tSearch);
    tSearch = setTimeout(() => renderResults(input.value), 120);
  });

  if (chkAuto) chkAuto.addEventListener('change', savePrefs);

  input.addEventListener('keydown', (e) => {
    if (!currentResults || !currentResults.length) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      selectedIndex = Math.min(currentResults.length - 1, selectedIndex + 1);
      highlightSelected();
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      selectedIndex = Math.max(0, selectedIndex - 1);
      highlightSelected();
    } else if (e.key === 'Enter') {
      e.preventDefault();
      const row = resultsDiv.querySelector(`div[data-index="${selectedIndex}"]`);
      if (row) row.click();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      resultsDiv.innerHTML = '';
      currentResults = [];
      selectedIndex = -1;
    }
  });

  // Kick off dataset load once
  (async () => {
    try {
      resultsDiv.innerHTML = '<div style="color:#888;padding:0.5em;">Loading dataset…</div>';
      await populateDatasetSelectors();
      // Apply ef on boot if available
      const prefs0 = loadPrefs();
      if (numEf && prefs0 && typeof prefs0.efSearch === 'number' && window.VEC && window.VEC.setEfSearch) {
        window.VEC.setEfSearch(prefs0.efSearch);
        numEf.value = String(prefs0.efSearch);
      }
      if (numEfc && prefs0 && typeof prefs0.efConstruction === 'number' && window.VEC && window.VEC.setEfConstruction) {
        window.VEC.setEfConstruction(prefs0.efConstruction);
        numEfc.value = String(prefs0.efConstruction);
      }
  if (chkNoCache && window.VEC && window.VEC.setUseCache) window.VEC.setUseCache(!(chkNoCache.checked));
  const n = await window.KNN.load('/pdb_profiles.json');
      try { await window.VEC.load('/pdb_profile_vectors.json'); } catch {}
      // Auto-load parquet replacements if enabled
      const prefs = loadPrefs();
      if (prefs && prefs.autoParquet) {
        try {
          if (prefs.vectors) await window.VEC.loadFromParquet(prefs.vectors);
        } catch {}
        try {
          if (prefs.profiles) await window.KNN.loadFromParquet(prefs.profiles);
        } catch {}
      }
  updateStatus(n);
  resultsDiv.innerHTML = `<div style="color:#888;padding:0.5em;">Loaded ${n} profiles. Type to search.</div>`;
  try { renderReininLegendView(); } catch {}
  try { ensureReininNames(); if (window.GLOBE && window.GLOBE.setReininFaceNames) window.GLOBE.setReininFaceNames(reininNamesLocal); } catch {}
    } catch (e) {
      resultsDiv.innerHTML = `<div style='color:#c00;padding:0.5em;'>Dataset load failed: ${e.message}</div>`;
    }
  })();

  function computePersonVisual(row) {
    // Ensure we resolve a friendly name when only a CID is present
    let label = row.displayName || row.name || '';
    if (!label && row && row.cid && window.KNN && window.KNN.getByCid) {
      const rec = window.KNN.getByCid(row.cid);
      if (rec) label = rec.displayName || rec.name || '';
    }
    if (!label) label = row.cid || 'Unknown';
    const color = '#4363d8';
    // Prefer vector PCA projection if available
    let x = 0, y = 0, z = 0;
    const p = (window.VEC && window.VEC.projectCid && row.cid) ? window.VEC.projectCid(row.cid) : null;
    if (p) {
      [x, y, z] = p;
    } else {
      function hashf(s) {
        let h = 2166136261 >>> 0;
        for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619) >>> 0; }
        return (h % 2000) / 1000 - 1; // [-1,1)
      }
      const key = String(row.cid || label);
      x = hashf(key + ':x'); y = hashf(key + ':y'); z = hashf(key + ':z');
    }
    return { label, x, y, z, color, size: 0.09, type: 'pdb', cid: row.cid };
  }

  function updateStatus(nProfiles) {
    const hnsw = (window.VEC && window.VEC.isHnswReady && window.VEC.isHnswReady());
    const vecN = (window.VEC && window.VEC.size ? window.VEC.size() : 0);
    const cache = (window.VEC && window.VEC.getCacheState ? window.VEC.getCacheState() : 'none');
    const src = (window.VEC && window.VEC.getSource ? window.VEC.getSource() : '/pdb_profile_vectors.json');
    const srcShort = src.startsWith('/dataset') ? `parquet:${baseName(src)}` : 'json';
  const psrc = (window.KNN && window.KNN.getSource ? window.KNN.getSource() : '/pdb_profiles.json');
  const pShort = psrc.startsWith('/dataset') ? `parquet:${baseName(psrc)}` : 'json';
  const building = (window.VEC && window.VEC.isBuilding && window.VEC.isBuilding());
  const ef = (window.VEC && window.VEC.getEfSearch) ? window.VEC.getEfSearch() : 64;
  const useCache = (window.VEC && window.VEC.getUseCache) ? window.VEC.getUseCache() : true;
  const ctor = window.__HNSW_CTOR || '';
  // PCA explained variance (top 3) if available
  let pcaTxt = '';
  try {
    const info = (window.VEC && window.VEC.getPcaInfo && window.VEC.getPcaInfo());
    if (info && Array.isArray(info.explained) && info.explained.length) {
      const pct = info.explained.slice(0,3).map(x => `${Math.round((x||0)*100)}%`).join('/');
      if (pct) pcaTxt = ` · PCA ${pct}`;
    }
  } catch {}
  if (statusDiv) statusDiv.textContent = `Profiles: ${nProfiles} (${pShort}) · Vectors: ${vecN} (${srcShort}) · HNSW: ${hnsw ? 'ready' : 'off'}${ctor ? ' ('+ctor+')' : ''} · ef ${ef} · Cache: ${cache}${useCache ? '' : ' (disabled)'}${building ? ' · Building…' : ''}${pcaTxt}`;
  }

  function setProgress(msg) { if (progressDiv) progressDiv.textContent = msg || ''; }
  function setProgressPct(pct){ if (progressFill) progressFill.style.width = `${Math.max(0,Math.min(100, pct|0))}%`; }
  function resetProgressSoon(){ setTimeout(()=> setProgressPct(0), 800); }
  function toast(msg, ok=true){ if(!toastEl) return; toastEl.textContent = msg; toastEl.style.background = ok ? '#2ca02c' : '#c0392b'; toastEl.style.opacity = '1'; toastEl.style.transform = 'translateY(0)'; setTimeout(()=>{ toastEl.style.opacity='0'; toastEl.style.transform='translateY(10px)'; }, 2200); }

  function setBuildingUI(on){
    try{
      if (btnRebuild) btnRebuild.disabled = !!on;
      if (btnCancel) btnCancel.disabled = !on;
  if (btnRebuildForce) btnRebuildForce.disabled = !!on;
  if (btnClear) btnClear.disabled = !!on;
  if (btnParquet) btnParquet.disabled = !!on;
  if (btnProfiles) btnProfiles.disabled = !!on;
  if (numEf) numEf.disabled = !!on;
  if (numEfc) numEfc.disabled = !!on;
  if (selVectors) selVectors.disabled = !!on;
  if (selProfiles) selProfiles.disabled = !!on;
    }catch{}
  }

  window.addEventListener('vec:pca:start', (e) => {
    const d = e.detail || {};
    if (typeof d.i === 'number' && typeof d.n === 'number') {
      const pct = Math.max(0, Math.min(100, Math.floor((d.i / Math.max(1, d.n)) * 100)));
      setProgress(`PCA [${d.phase || 'phase'}]: ${pct}%`);
  setProgressPct(pct);
    } else if (d.phase) {
      setProgress(`PCA [${d.phase}]…`);
    } else {
      setProgress(`PCA: computing (${d.count || '?'}×${d.dim || '?'})…`);
    }
    setBuildingUI(true);
  });
  window.addEventListener('vec:pca:end', () => { setProgress('PCA: done.'); resetProgressSoon(); });
  window.addEventListener('vec:pca:error', () => { setProgress('PCA: failed.'); resetProgressSoon(); setBuildingUI(false); toast('PCA failed', false); });
  window.addEventListener('vec:hnsw:build:start', (e) => {
    const d = e.detail || {};
    if (typeof d.added === 'number' && typeof d.count === 'number') {
      const pct = Math.max(0, Math.min(100, Math.floor((d.added / Math.max(1, d.count)) * 100)));
      setProgress(`HNSW: ${pct}% (${d.added}/${d.count})`);
  setProgressPct(pct);
    } else {
      setProgress(`HNSW: building (${d.count || '?'}×${d.dim || '?'})…`);
    }
    setBuildingUI(true);
  });
  window.addEventListener('vec:hnsw:build:end', () => { setProgress('HNSW: built.'); resetProgressSoon(); setBuildingUI(false); updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0); });
  window.addEventListener('vec:hnsw:ctor', (e) => { try { const which = e.detail && e.detail.which ? e.detail.which : ''; window.__HNSW_CTOR = which; updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0); if (which) logDebug(`ctor: ${which}`); } catch {} });
  window.addEventListener('vec:hnsw:debug', (e) => {
    try {
      const d = e.detail || {};
  if (d.message) { setProgress(`HNSW: ${d.message}`); logDebug(d.message); }
    } catch {}
  });
  window.addEventListener('vec:hnsw:cache:loaded', () => { setProgress('HNSW: loaded from cache.'); resetProgressSoon(); toast('HNSW cache loaded'); try { logDebug('cache: loaded'); } catch {} });
  window.addEventListener('vec:hnsw:cache:saved', () => { setProgress('HNSW: cached.'); resetProgressSoon(); try { logDebug('cache: saved'); } catch {} });
  window.addEventListener('vec:hnsw:error', (e) => {
    const d = (e && e.detail) || {};
    const msg = d.message ? `HNSW: error (fallback). ${d.message}` : 'HNSW: error (fallback).';
    setProgress(msg);
    resetProgressSoon();
    setBuildingUI(false);
    updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0);
  toast('HNSW error; using fallback', false);
  try { if (d && d.message) logDebug(`error: ${d.message}`); } catch {}
  });

  if (btnRebuild) btnRebuild.onclick = async () => {
    if (window.VEC && window.VEC.rebuildIndex) {
      resultsDiv.innerHTML = '<div style="color:#888;padding:0.5em;">Rebuilding index…</div>';
      setProgress('HNSW: rebuilding…');
      setBuildingUI(true);
      const done = setBusy(btnRebuild, 'Rebuilding…');
      try {
        await window.VEC.rebuildIndex();
        updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0);
        resultsDiv.innerHTML = '<div style="color:#2ca02c;padding:0.5em;">Index rebuilt.</div>';
      } finally {
        setBuildingUI(false);
        done();
      }
    }
  };

  // Export a manifest capturing sources and stats
  if (btnExportManifest) btnExportManifest.onclick = async () => {
    try {
      const manifest = {
        timestamp: new Date().toISOString(),
        profilesSource: (window.KNN && window.KNN.getSource && window.KNN.getSource()) || '',
        vectorsSource: (window.VEC && window.VEC.getSource && window.VEC.getSource()) || '',
        profilesCount: (window.KNN && window.KNN.size && window.KNN.size()) || 0,
        vectorsCount: (window.VEC && window.VEC.size && window.VEC.size()) || 0,
        vectorDim: (window.VEC && window.VEC.getDim && window.VEC.getDim()) || 0,
        hnsw: {
          ready: (window.VEC && window.VEC.isHnswReady && window.VEC.isHnswReady()) || false,
          cacheState: (window.VEC && window.VEC.getCacheState && window.VEC.getCacheState()) || 'none',
          ef: (window.VEC && window.VEC.getEfSearch && window.VEC.getEfSearch()) || 64,
          ctor: window.__HNSW_CTOR || ''
        },
        pca: (window.VEC && window.VEC.getPcaInfo && window.VEC.getPcaInfo()) || { dim: 0, count: 0, eigenvalues: [], totalVariance: 0, explained: [] }
      };
      const blob = new Blob([JSON.stringify(manifest, null, 2)], { type: 'application/json' });
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'dataset_manifest.json'; a.click(); URL.revokeObjectURL(a.href);
      toast('Manifest exported');
    } catch (e) { toast(e && e.message ? e.message : 'Export failed', false); }
  };

  // Export an ID map of alternate identifiers -> canonical cid
  if (btnExportIdMap) btnExportIdMap.onclick = async (ev) => {
    try {
      if (!window.KNN || !window.KNN.getAll) throw new Error('Profiles not loaded');
      const rows = window.KNN.getAll();
      const pairSet = new Set();
      const out = [];
      const looksLikeIdKey = (k) => /(^|_)(cid|uuid|uid|id)$/i.test(k || '');
      for (const r of rows) {
        if (!r) continue;
        const cid = (r.cid != null) ? String(r.cid) : '';
        const candidates = new Set();
        if (cid) candidates.add(cid);
        for (const [k, v] of Object.entries(r)) {
          if (!looksLikeIdKey(k)) continue;
          if (Array.isArray(v) || (v && typeof v === 'object')) continue;
          const s = v == null ? '' : String(v);
          if (!s) continue;
          candidates.add(s);
        }
        for (const id of candidates) {
          const key = `${id}\u0000${cid}`;
          if (!pairSet.has(key)) { pairSet.add(key); out.push({ id, cid }); }
        }
      }
      if (!out.length) throw new Error('No ids');
      const wantCsv = !!(ev && ev.altKey);
      const wantParquet = !!(ev && ev.ctrlKey);
      let blob, name;
      if (wantParquet) {
        if (!window.DuckVec || !window.DuckVec.exportProfilesParquet) throw new Error('Parquet export unavailable');
        const bytes = await window.DuckVec.exportProfilesParquet(out);
        blob = new Blob([bytes], { type: 'application/octet-stream' });
        name = 'idmap.parquet';
      } else if (wantCsv) {
        const lines = ['id,cid'];
        for (const p of out) lines.push(`${JSON.stringify(p.id)},${JSON.stringify(p.cid)}`);
        blob = new Blob([lines.join('\n') + '\n'], { type: 'text/csv' });
        name = 'idmap.csv';
      } else {
        blob = new Blob([JSON.stringify(out)], { type: 'application/json' });
        name = 'idmap.json';
      }
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = name; a.click(); URL.revokeObjectURL(a.href);
      toast('IdMap exported');
    } catch (e) { toast(e && e.message ? e.message : 'Export failed', false); }
  };

  // Export a combined dataset: profiles + normalized vectors (+ projections)
  if (btnExportCombined) btnExportCombined.onclick = async (ev) => {
    try {
      if (!window.KNN || !window.KNN.getAll) throw new Error('Profiles not loaded');
      if (!window.VEC || !window.VEC.getRows) throw new Error('Vectors not loaded');
      const profiles = window.KNN.getAll();
      const vectors = window.VEC.getRows();
      const proj = (window.VEC && window.VEC.getProjections) ? window.VEC.getProjections() : [];
      const pmap = new Map(proj.map(r => [r.cid, r]));
      const profMap = new Map();
      for (const r of profiles) if (r && r.cid) profMap.set(String(r.cid), r);
      const out = [];
      // Use intersection by default
      for (const r of vectors) {
        const cid = r && r.cid ? String(r.cid) : '';
        if (!cid) continue;
        const prof = profMap.get(cid);
        if (!prof) continue;
        const pr = pmap.get(cid);
        out.push({
          cid,
          displayName: prof.displayName || prof.name || '',
          mbti: prof.mbti || '', socionics: prof.socionics || '', big5: prof.big5 || '',
          description: prof.description || '',
          vector: r.vector,
          x: pr ? pr.x : undefined,
          y: pr ? pr.y : undefined,
          z: pr ? pr.z : undefined
        });
      }
      if (!out.length) throw new Error('No overlap between profiles and vectors');
      const wantCsv = !!(ev && ev.altKey);
      const wantParquet = !!(ev && ev.ctrlKey);
      let blob, name;
      if (wantParquet) {
        if (!window.DuckVec || !window.DuckVec.exportProfilesParquet) throw new Error('Parquet export unavailable');
        const bytes = await window.DuckVec.exportProfilesParquet(out);
        blob = new Blob([bytes], { type: 'application/octet-stream' });
        name = 'dataset_combined.parquet';
      } else if (wantCsv) {
        const cols = ['cid','displayName','mbti','socionics','big5','x','y','z','vector'];
        const lines = [cols.join(',')];
        for (const r of out) {
          const row = [r.cid, r.displayName||'', r.mbti||'', r.socionics||'', r.big5||'', r.x, r.y, r.z, JSON.stringify(r.vector)];
          lines.push(row.map(v => JSON.stringify(String(v == null ? '' : v))).join(','));
        }
        blob = new Blob([lines.join('\n') + '\n'], { type: 'text/csv' });
        name = 'dataset_combined.csv';
      } else {
        blob = new Blob([JSON.stringify(out)], { type: 'application/json' });
        name = 'dataset_combined.json';
      }
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = name; a.click(); URL.revokeObjectURL(a.href);
      toast('Combined exported');
    } catch (e) { toast(e && e.message ? e.message : 'Export failed', false); }
  };

  // Export a simple data health report
  if (btnHealthReport) btnHealthReport.onclick = async () => {
    try {
      const profilesSrc = (window.KNN && window.KNN.getSource && window.KNN.getSource()) || '';
      const vectorsSrc = (window.VEC && window.VEC.getSource && window.VEC.getSource()) || '';
      const pAll = (window.KNN && window.KNN.getAll && window.KNN.getAll()) || [];
      const vAll = (window.VEC && window.VEC.getRows && window.VEC.getRows()) || [];
      const pca = (window.VEC && window.VEC.getPcaInfo && window.VEC.getPcaInfo()) || {};
      const hnsw = {
        ready: (window.VEC && window.VEC.isHnswReady && window.VEC.isHnswReady()) || false,
        cacheState: (window.VEC && window.VEC.getCacheState && window.VEC.getCacheState()) || 'none',
        ef: (window.VEC && window.VEC.getEfSearch && window.VEC.getEfSearch()) || 64,
        ctor: window.__HNSW_CTOR || ''
      };
      const profByCid = new Map();
      const dupProfiles = [];
      let missingName = 0;
      for (const r of pAll) {
        if (!r) continue;
        const cid = r.cid ? String(r.cid) : '';
        if (!cid) continue;
        if (profByCid.has(cid)) dupProfiles.push(cid); else profByCid.set(cid, r);
        const name = (r.displayName || r.name || '').trim();
        if (!name || /^(Qm|bafy)[A-Za-z0-9]{20,}$/i.test(name)) missingName++;
      }
      const vecCids = new Set(vAll.map(r => String(r.cid)));
      let vecMissingProfile = 0;
      for (const cid of vecCids) if (!profByCid.has(cid)) vecMissingProfile++;
      let profMissingVector = 0;
      for (const cid of profByCid.keys()) if (!vecCids.has(cid)) profMissingVector++;
      const intersection = Math.min(profByCid.size, (Array.isArray(vAll) ? vAll.length : 0)) - vecMissingProfile;
      const report = {
        timestamp: new Date().toISOString(),
        sources: { profiles: profilesSrc, vectors: vectorsSrc },
        counts: {
          profiles: profByCid.size,
          profilesMissingName: missingName,
          vectors: Array.isArray(vAll) ? vAll.length : 0,
          vectorDim: (window.VEC && window.VEC.getDim && window.VEC.getDim()) || 0,
          intersection,
          vectorsWithoutProfile: vecMissingProfile,
          profilesWithoutVector: profMissingVector,
          duplicateProfileCids: dupProfiles.length
        },
        pca,
        hnsw
      };
      const blob = new Blob([JSON.stringify(report, null, 2)], { type: 'application/json' });
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'data_health_report.json'; a.click(); URL.revokeObjectURL(a.href);
      toast('Health report exported');
    } catch (e) { toast(e && e.message ? e.message : 'Export failed', false); }
  };

  if (btnCancel) btnCancel.onclick = () => {
    if (window.VEC && window.VEC.cancelBuild) {
      const ok = window.VEC.cancelBuild();
      if (ok) {
        setProgress('Cancelled.');
        setProgressPct(0);
        resultsDiv.innerHTML = '<div style="color:#888;padding:0.5em;">Build cancelled.</div>';
        setBuildingUI(false);
        updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0);
      }
    }
  };

  if (btnRebuildForce) btnRebuildForce.onclick = async () => {
    if (!window.VEC || !window.VEC.rebuildIndex) return;
    resultsDiv.innerHTML = '<div style="color:#888;padding:0.5em;">Force rebuilding index (no cache)…</div>';
    setProgress('HNSW: rebuilding (force)…');
    setBuildingUI(true);
    const done = setBusy(btnRebuildForce, 'Rebuilding…');
    const prev = (window.VEC.getUseCache && window.VEC.getUseCache());
    try {
      if (window.VEC.setUseCache) window.VEC.setUseCache(false);
      await window.VEC.rebuildIndex();
      updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0);
      resultsDiv.innerHTML = '<div style="color:#2ca02c;padding:0.5em;">Index rebuilt (force, no cache).</div>';
    } catch (e) {
      resultsDiv.innerHTML = `<div style='color:#c00;padding:0.5em;'>Force rebuild failed: ${e && e.message || e}</div>`;
    } finally {
      if (window.VEC.setUseCache) window.VEC.setUseCache(!!prev);
      setBuildingUI(false);
      done();
    }
  };

  if (btnClear) btnClear.onclick = async () => {
    if (window.VEC && window.VEC.clearHnswCache) {
      const done = setBusy(btnClear, 'Clearing…');
      try {
        const ok = await window.VEC.clearHnswCache();
        updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0);
        resultsDiv.innerHTML = ok ? '<div style="color:#2ca02c;padding:0.5em;">Cache cleared.</div>' : '<div style="color:#c00;padding:0.5em;">Failed to clear cache.</div>';
        toast(ok ? 'Cache cleared' : 'Failed to clear cache', !!ok);
      } finally { done(); }
    }
  };

  if (btnExportIdx) btnExportIdx.onclick = async () => {
    if (!window.VEC || !window.VEC.exportIndex) return;
    const done = setBusy(btnExportIdx, 'Exporting…');
    try {
      const bytes = await window.VEC.exportIndex();
      if (!bytes) { toast('Export failed', false); return; }
      const blob = new Blob([bytes], { type: 'application/octet-stream' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'hnsw_index.bin';
      a.click();
      URL.revokeObjectURL(a.href);
      toast('Index exported');
    } finally { done(); }
  };

  // Export normalized vectors (JSON by default; hold Alt for CSV)
  if (btnExportVec) btnExportVec.onclick = async (ev) => {
    try {
      if (!window.VEC || !window.VEC.getRows) throw new Error('Vectors not loaded');
      const rows = window.VEC.getRows();
      if (!rows || !rows.length) throw new Error('No vectors');
  const wantCsv = !!(ev && ev.altKey);
      const wantParquet = !!(ev && ev.ctrlKey);
      let blob, name;
      if (wantParquet) {
        if (!window.DuckVec || !window.DuckVec.exportVectorsParquet) throw new Error('Parquet export unavailable');
        const bytes = await window.DuckVec.exportVectorsParquet(rows);
        blob = new Blob([bytes], { type: 'application/octet-stream' });
        name = 'vectors_normalized.parquet';
      } else if (wantCsv) {
        // CSV: cid,vector_json
        const lines = ['cid,vector'];
        for (const r of rows) lines.push(`${JSON.stringify(r.cid)},${JSON.stringify(r.vector)}`);
        blob = new Blob([lines.join('\n') + '\n'], { type: 'text/csv' });
        name = 'vectors_normalized.csv';
      } else {
        blob = new Blob([JSON.stringify(rows)], { type: 'application/json' });
        name = 'vectors_normalized.json';
      }
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = name;
      a.click();
      URL.revokeObjectURL(a.href);
      toast('Vectors exported');
    } catch (e) { toast(e && e.message ? e.message : 'Export failed', false); }
  };

  // Export enriched profiles from KNN (JSON by default; hold Alt for CSV)
  if (btnExportProf) btnExportProf.onclick = async (ev) => {
    try {
  if (!window.KNN || !window.KNN.getAll) throw new Error('Profiles not loaded');
  const out = window.KNN.getAll();
  if (!out || !out.length) throw new Error('No profiles');
  const wantCsv = !!(ev && ev.altKey);
      const wantParquet = !!(ev && ev.ctrlKey);
      let blob, name;
      if (wantParquet) {
        if (!window.DuckVec || !window.DuckVec.exportProfilesParquet) throw new Error('Parquet export unavailable');
        const bytes = await window.DuckVec.exportProfilesParquet(out);
        blob = new Blob([bytes], { type: 'application/octet-stream' });
        name = 'profiles_enriched.parquet';
      } else if (wantCsv) {
        // CSV minimal: cid,displayName,mbti,socionics,big5
        const lines = ['cid,displayName,mbti,socionics,big5'];
        for (const r of out) {
          const line = [r.cid || '', r.displayName || r.name || '', r.mbti || '', r.socionics || '', r.big5 || '']
            .map(v => JSON.stringify(String(v || ''))).join(',');
          lines.push(line);
        }
        blob = new Blob([lines.join('\n') + '\n'], { type: 'text/csv' });
        name = 'profiles_enriched.csv';
      } else {
        blob = new Blob([JSON.stringify(out)], { type: 'application/json' });
        name = 'profiles_enriched.json';
      }
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = name;
      a.click();
      URL.revokeObjectURL(a.href);
      toast('Profiles exported');
    } catch (e) { toast(e && e.message ? e.message : 'Export failed', false); }
  };

  // Export PCA projections for all loaded vectors
  if (btnExportProj) btnExportProj.onclick = async (ev) => {
    try {
      if (!window.VEC || !window.VEC.getProjections) throw new Error('Vectors not loaded');
      const rows = window.VEC.getProjections();
      if (!rows || !rows.length) throw new Error('No projections');
      const wantCsv = !!(ev && ev.altKey);
      const wantParquet = !!(ev && ev.ctrlKey);
      let blob, name;
      if (wantParquet) {
        if (!window.DuckVec || !window.DuckVec.exportProfilesParquet) throw new Error('Parquet export unavailable');
        const bytes = await window.DuckVec.exportProfilesParquet(rows);
        blob = new Blob([bytes], { type: 'application/octet-stream' });
        name = 'vectors_projections.parquet';
      } else if (wantCsv) {
        const lines = ['cid,x,y,z'];
        for (const r of rows) lines.push([r.cid, r.x, r.y, r.z].map(v => JSON.stringify(String(v))).join(','));
        blob = new Blob([lines.join('\n') + '\n'], { type: 'text/csv' });
        name = 'vectors_projections.csv';
      } else {
        blob = new Blob([JSON.stringify(rows)], { type: 'application/json' });
        name = 'vectors_projections.json';
      }
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = name; a.click(); URL.revokeObjectURL(a.href);
      toast('Projections exported');
    } catch (e) { toast(e && e.message ? e.message : 'Export failed', false); }
  };

  if (fileImportIdx) fileImportIdx.addEventListener('change', async (ev) => {
    const f = ev.target.files && ev.target.files[0];
    if (!f || !window.VEC || !window.VEC.importIndex) return;
    const arr = new Uint8Array(await f.arrayBuffer());
    const ok = await window.VEC.importIndex(arr);
    if (ok) { updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0); toast('Index imported'); }
    else toast('Import failed', false);
  });

  if (btnParquet) btnParquet.onclick = async () => {
    const url = selVectors && selVectors.value ? selVectors.value : '/dataset/pdb_profile_vectors.parquet';
    resultsDiv.innerHTML = `<div style=\"color:#888;padding:0.5em;\">Loading vectors from ${baseName(url)}…</div>`;
    try {
      const done = setBusy(btnParquet, 'Loading…');
      if (/\.parquet$/i.test(url)) {
        if (!window.VEC || !window.VEC.loadFromParquet) throw new Error('Parquet loader unavailable');
        await window.VEC.loadFromParquet(url);
      } else {
        await window.VEC.load(url);
      }
      savePrefs();
      updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0);
      resultsDiv.innerHTML = `<div style=\"color:#2ca02c;padding:0.5em;\">Vectors loaded: ${baseName(url)}.</div>`;
      toast('Vectors loaded');
    } catch (e) {
      const hint = (String(url).startsWith('/dataset') ? "<div style='color:#666;margin-top:4px;'>Tip: Ensure the /dataset endpoint is served (start the dataset server) so Parquet URLs return binary data, not HTML.</div>" : '');
      resultsDiv.innerHTML = `<div style='color:#c00;padding:0.5em;'>Vectors load failed: ${e.message}</div>${hint}`;
      toast('Vectors load failed', false);
    } finally { if (typeof done === 'function') done(); }
  };

  if (btnProfiles) btnProfiles.onclick = async () => {
    const url = selProfiles && selProfiles.value ? selProfiles.value : '/dataset/pdb_profiles_normalized.parquet';
    resultsDiv.innerHTML = `<div style=\"color:#888;padding:0.5em;\">Loading profiles from ${baseName(url)}…</div>`;
    try {
      const done = setBusy(btnProfiles, 'Loading…');
      let n;
      if (/\.parquet$/i.test(url)) {
        if (!window.KNN || !window.KNN.loadFromParquet) throw new Error('Profiles parquet loader unavailable');
        n = await window.KNN.loadFromParquet(url);
      } else {
        n = await window.KNN.load(url);
      }
      savePrefs();
      updateStatus(n);
      resultsDiv.innerHTML = `<div style=\"color:#2ca02c;padding:0.5em;\">Loaded ${n} profiles from ${baseName(url)}.</div>`;
      toast('Profiles loaded');
    } catch (e) {
      const hint = (String(url).startsWith('/dataset') ? "<div style='color:#666;margin-top:4px;'>Tip: Ensure the /dataset endpoint is served (start the dataset server) so Parquet URLs return binary data, not HTML.</div>" : '');
      resultsDiv.innerHTML = `<div style='color:#c00;padding:0.5em;'>Profiles load failed: ${e.message}</div>${hint}`;
      toast('Profiles load failed', false);
    } finally { if (typeof done === 'function') done(); }
  };
  if (selVectors) selVectors.addEventListener('change', savePrefs);
  if (selProfiles) selProfiles.addEventListener('change', savePrefs);
  if (chkNoCache) chkNoCache.addEventListener('change', () => {
    if (window.VEC && window.VEC.setUseCache) window.VEC.setUseCache(!(chkNoCache.checked));
    savePrefs();
    updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0);
  });
  if (btnClearDebug) btnClearDebug.addEventListener('click', () => { if (debugPre) { debugPre.textContent = ''; toast('Debug cleared'); } });
  if (btnCopyDebug) btnCopyDebug.addEventListener('click', async () => {
    try {
      const txt = debugPre ? debugPre.textContent : '';
      if (!txt) { toast('Debug log empty'); return; }
      await navigator.clipboard.writeText(txt);
      toast('Copied debug log');
    } catch { toast('Copy failed', false); }
  });
  if (chkDebug) chkDebug.addEventListener('change', () => {
    if (debugPre) debugPre.style.display = chkDebug.checked ? 'block' : 'none';
    savePrefs();
  });

  // Globe handlers
  function ensureGlobe(){
    if (!planetContainer) return false;
    if (!window.GLOBE || !window.GLOBE.init) { toast('Globe not available', false); return false; }
    if (!planetVisible) { try { window.GLOBE.init(planetContainer); } catch(e) { console.warn(e); return false; } }
    planetContainer.style.display = 'block';
    // Ensure globe canvas fills the page beneath the sidebar
    try {
      const canvas = planetContainer.querySelector('canvas');
      if (canvas) { canvas.style.position = 'absolute'; canvas.style.inset = '0 0 0 0'; canvas.style.width = '100%'; canvas.style.height = '100%'; }
    } catch {}
    // Sidebar was originally created inside the container; ensure it exists
    try {
      planetSidebar = planetContainer.querySelector('#planet-sidebar') || planetSidebar;
    } catch {}
    if (planetControls) planetControls.style.display = 'flex';
    planetVisible = true;
  // Apply stored globe options
  try {
    const prefs = loadPrefs();
  if (prefs && prefs.globe && window.GLOBE) {
      if (window.GLOBE.setOptions) window.GLOBE.setOptions({ links: !!prefs.globe.links, labels: !!prefs.globe.labels, tooltip: false, grid: !!prefs.globe.grid });
      // Apply contour/axis settings
      const preset = (prefs.globe.contourPreset || 'vivid');
      const colors = (function(){
        if (preset === 'soft') return [[0.95,0.80,0.55],[0.96,0.70,0.72],[0.70,0.90,0.95],[0.75,0.90,0.75]];
        if (preset === 'mono') return [[1,1,1],[0.9,0.9,0.9],[0.8,0.8,0.8],[0.7,0.7,0.7]];
        return [[1,0.756,0.027],[0.902,0.098,0.294],[0,0.737,0.831],[0.545,0.764,0.294]]; // vivid
      })();
      const normalsPreset = prefs.globe.normalsPreset || 'globe';
      const normals = (function(){
        if (normalsPreset === 'mbti') {
          const n = MBTI_DEFAULT_NORMALS; return [n.nE,n.nN,n.nF,n.nP];
        }
        if (normalsPreset === 'orthogonal') {
          const N = (v)=>{ const L=Math.hypot(v[0],v[1],v[2])||1; return [v[0]/L,v[1]/L,v[2]/L]; };
          return [N([1,0,0]), N([0,1,0]), N([0,0,1]), N([1,1,1])];
        }
        if (normalsPreset === 'tetra') {
          // Map as [E,N,F,P] -> use tetrahedral normals in fixed order
          return [TETRAHEDRAL_NORMALS[0], TETRAHEDRAL_NORMALS[1], TETRAHEDRAL_NORMALS[2], TETRAHEDRAL_NORMALS[3]];
        }
        if (normalsPreset === 'custom') {
          const cn = (prefs.globe && Array.isArray(prefs.globe.customNormals)) ? prefs.globe.customNormals : null;
          if (cn && cn.length === 4 && cn.every(a => Array.isArray(a) && a.length===3 && a.every(Number.isFinite))) return cn.map(NormalizeVec3);
          // fallback: MBTI defaults
          const n = MBTI_DEFAULT_NORMALS; return [n.nE,n.nN,n.nF,n.nP];
        }
        return undefined; // use globe defaults
      })();
  try { window.GLOBE.setGreatCircles({ enabled: (Object.prototype.hasOwnProperty.call(prefs.globe,'contours') ? !!prefs.globe.contours : true), width: (typeof prefs.globe.contourWidth==='number'?prefs.globe.contourWidth:0.09), intensity: (typeof prefs.globe.contourIntensity==='number'?prefs.globe.contourIntensity:0.55), colors, ...(normals?{ normals }: {}) }); } catch {}
      const ax = prefs.globe.polarAxis || 'y';
      const angleDeg = (typeof prefs.globe.polarAngle === 'number') ? prefs.globe.polarAngle : 0;
      const axisVec = ax === 'x' ? [1,0,0] : (ax === 'z' ? [0,0,1] : [0,1,0]);
      try { window.GLOBE.setPolarRotation({ axis: axisVec, angle: angleDeg * Math.PI / 180 }); } catch {}
      // Apply preferred surface coloring
      try {
        if (selSurface && typeof prefs.globe.surface === 'string') selSurface.value = prefs.globe.surface;
        const surf = (selSurface && selSurface.value) || 'prismatic';
        if (surf === 'mbti') { ensureMbtiSurface(); window.GLOBE.setSurfaceMode('mbti'); }
        else if (surf === 'reinin') {
          if (Array.isArray(prefs.globe.reininColors) && prefs.globe.reininColors.length === 24) reininColorsLocal = prefs.globe.reininColors;
          if (Array.isArray(prefs.globe.reininNames) && prefs.globe.reininNames.length === 24) reininNamesLocal = prefs.globe.reininNames;
          ensureReininSurface(); window.GLOBE.setSurfaceMode('reinin');
          try { renderReininLegendView(); } catch {}
        } else { window.GLOBE.setSurfaceMode('prismatic'); }
      } catch {}
    }
  } catch {}
    return true;
  }
  function hideGlobe(){ try { if (window.GLOBE && window.GLOBE.setReininHighlight) window.GLOBE.setReininHighlight(-1); } catch {} if (planetContainer) planetContainer.style.display = 'none'; if (planetControls) planetControls.style.display = 'none'; planetVisible = false; }
  if (btnHideGlobe) btnHideGlobe.onclick = hideGlobe;
  // --- 4D mapping helpers (Socionics/MBTI -> S^3 -> S^2 via Hopf map) ---
  function normalizeTypeToMBTI(t) {
    if (!t || typeof t !== 'string') return null;
    const raw = t.trim();
    const s = raw.toUpperCase();
    // Direct MBTI 4-letter
    if (/^[EI][SN][TF][JP]$/.test(s)) return s;
    // Socionics 3-letter canonical codes -> MBTI (common mapping)
    const SOC3 = {
      ILE: 'ENTP', LII: 'INTP', ESE: 'ESFJ', SEI: 'ISFJ',
      SLE: 'ESTP', LSI: 'ISTP', SEE: 'ESFP', ESI: 'ISFP',
      LIE: 'ENTJ', ILI: 'INTJ', EIE: 'ENFJ', IEI: 'INFJ',
      LSE: 'ESTJ', SLI: 'ISTJ', EII: 'INFP', IEE: 'ENFP'
    };
    if (SOC3[s]) return SOC3[s];
    // Socionics 4-letter like INTj, ENTp (lowercase j/p)
    const m = /^([EI])([SN])([TF])([jp])$/i.exec(raw);
    if (m) {
      const ei = m[1].toUpperCase();
      const sn = m[2].toUpperCase();
      const tf = m[3].toUpperCase();
      let jp = m[4].toUpperCase();
      // For introverts, J/P flips between Socionics and MBTI conventions
      if (ei === 'I') jp = jp === 'J' ? 'P' : 'J';
      return `${ei}${sn}${tf}${jp}`;
    }
    return null;
  }
  function parseType4D(t) {
    const mbti = normalizeTypeToMBTI(t);
    if (!mbti) return null;
    const E = mbti[0] === 'E' ? 1 : -1;
    const N = mbti[1] === 'N' ? 1 : -1; // N vs S
    const F = mbti[2] === 'F' ? 1 : -1; // F vs T
    const P = mbti[3] === 'P' ? 1 : -1; // P vs J
    const len = Math.hypot(E,N,F,P) || 1;
    return [E/len, N/len, F/len, P/len];
  }

  // Infer type from nearest typed neighbors via vector KNN
  function inferTypeFromNeighbors(cid, k = 12) {
    try {
      if (!cid || !window.VEC || typeof window.VEC.similarByCid !== 'function') return null;
      const sims = window.VEC.similarByCid(cid, k) || [];
      const votes = new Map();
      for (const s of sims) {
        const rec = (window.KNN && typeof window.KNN.getByCid === 'function') ? window.KNN.getByCid(s.cid) : null;
        if (!rec) continue;
        const typ = normalizeTypeToMBTI(rec.mbti || rec.socionics || rec.type);
        if (!typ) continue;
        const w = Number.isFinite(s._score) ? Math.max(0, s._score) : 1;
        votes.set(typ, (votes.get(typ) || 0) + w);
      }
      let best = null, bestW = -1;
      for (const [t, w] of votes.entries()) { if (w > bestW) { best = t; bestW = w; } }
      return best;
    } catch { return null; }
  }
  function hopfToVec3(abcd) {
    const [a,b,c,d] = abcd;
    const x = 2.0 * (a*c + b*d);
    const y = 2.0 * (b*c - a*d);
    const z = (a*a + b*b) - (c*c + d*d);
    // normalize to unit sphere just in case of numeric drift
    const L = Math.hypot(x,y,z) || 1;
    return { x: x/L, y: y/L, z: z/L };
  }
  function vec3ToLatLon(v) {
    const lat = Math.asin(Math.max(-1, Math.min(1, v.y))) * 180/Math.PI;
    const lon = Math.atan2(v.z, v.x) * 180/Math.PI;
    return { lat, lon };
  }
  async function placeOnGlobe(baseRows) {
    try {
      if (!ensureGlobe()) return;
      const N = Math.max(10, Math.min(parseInt(numN.value||'200',10)||200, 2000));
      let k = Math.max(3, Math.min(parseInt(numK.value||'6',10)||6, 24));
      const base = (Array.isArray(baseRows) && baseRows.length) ? baseRows : (currentResults && currentResults.length ? currentResults : (window.KNN && input.value ? window.KNN.search(input.value, N) : []));
      const rows = base.slice(0, N);
      // Skip if nothing to place
      if (!rows.length) { toast('No results to place', false); return; }
      // Placement mode:
      const mode = (selPlacement && selPlacement.value) || 'clusters';
      if (mode === '4d') {
    // Build points from MBTI strings when available
        const points = [];
        const usedCombos = new Set();
        let inferred = 0;
        for (const r of rows) {
          const rec = r && r.cid && window.KNN && window.KNN.getByCid ? window.KNN.getByCid(r.cid) : r;
          const cid = (rec && rec.cid) || r.cid;
          const nm = (rec && (rec.displayName || rec.name)) || (r.displayName || r.name) || cid;
          const typ = (rec && (rec.mbti || rec.socionics || rec.type)) ? String(rec.mbti || rec.socionics || rec.type).trim() : null;
          let usedType = normalizeTypeToMBTI(typ);
          let v4 = usedType ? parseType4D(usedType) : null;
      let inferredFlag = false;
      if (!v4) {
            const tInf = inferTypeFromNeighbors(cid, 16);
            if (tInf) {
              usedType = normalizeTypeToMBTI(tInf) || tInf;
              v4 = parseType4D(usedType);
              inferred++;
        inferredFlag = true;
            }
          }
          if (!v4) continue; // skip if no type even after inference
          let v3 = hopfToVec3(v4);
          // Optional function-aware offset on the sphere
          try {
            const np = (selNormalsPreset && selNormalsPreset.value) || 'globe';
            let normalsFA = null;
            if (np === 'mbti') { const n = MBTI_DEFAULT_NORMALS; normalsFA = [n.nE,n.nN,n.nF,n.nP]; }
            else if (np === 'orthogonal') { const N=(v)=>{ const L=Math.hypot(v[0],v[1],v[2])||1; return [v[0]/L,v[1]/L,v[2]/L]; }; normalsFA = [N([1,0,0]),N([0,1,0]),N([0,0,1]),N([1,1,1])]; }
            else if (np === 'tetra') { normalsFA = [TETRAHEDRAL_NORMALS[0], TETRAHEDRAL_NORMALS[1], TETRAHEDRAL_NORMALS[2], TETRAHEDRAL_NORMALS[3]]; }
            else if (np === 'custom') { const v = getCustomNormalsFromInputs(); if (v && v.length===4) normalsFA = v; }
            const prefsFA = loadPrefs();
            const doFA = (prefsFA && prefsFA.globe && typeof prefsFA.globe.funcAware === 'boolean') ? !!prefsFA.globe.funcAware : (chkFuncAware ? !!chkFuncAware.checked : true);
            const maxDeg = (prefsFA && prefsFA.globe && typeof prefsFA.globe.funcOffsetDeg === 'number') ? prefsFA.globe.funcOffsetDeg : (rangeFuncOffset ? (parseFloat(rangeFuncOffset.value)||10) : 10);
            if (doFA && normalsFA && usedType) {
              const p = [v3.x, v3.y, v3.z];
              const rw = (prefsFA && prefsFA.globe && prefsFA.globe.funcRoleWeights) ? prefsFA.globe.funcRoleWeights : {
                dominant: rangeWDom ? (parseFloat(rangeWDom.value)||0.5) : DEFAULT_ROLE_WEIGHTS.dominant,
                auxiliary: rangeWAux ? (parseFloat(rangeWAux.value)||0.3) : DEFAULT_ROLE_WEIGHTS.auxiliary,
                tertiary: rangeWTer ? (parseFloat(rangeWTer.value)||0.15) : DEFAULT_ROLE_WEIGHTS.tertiary,
                inferior: rangeWInf ? (parseFloat(rangeWInf.value)||0.05) : DEFAULT_ROLE_WEIGHTS.inferior
              };
              const prefsAtt = prefsFA && prefsFA.globe;
              const attDeg = (prefsAtt && typeof prefsAtt.funcAttitudeDeg === 'number') ? prefsAtt.funcAttitudeDeg : (rangeFuncRoll ? (parseFloat(rangeFuncRoll.value)||6) : 6);
              const q = computeFunctionOffset(p, usedType, normalsFA, rw, attDeg * Math.PI/180, 1.0, maxDeg);
              v3 = { x: q[0], y: q[1], z: q[2] };
            }
          } catch {}
          const { lat, lon } = vec3ToLatLon(v3);
      points.push({ cid, label: nm, mbti: usedType || null, inferred: inferredFlag, cluster: 0, lat, lon });
          if (usedType) usedCombos.add(usedType);
        }
        if (!points.length) { toast('No MBTI/Socionics types in results to project (4D mode)', false); return; }
        // Reference centroids for visibility: the 16 MBTI combos
        const combos = [
          'ESTJ','ESTP','ESFJ','ESFP','ENTJ','ENTP','ENFJ','ENFP',
          'ISTJ','ISTP','ISFJ','ISFP','INTJ','INTP','INFJ','INFP'
        ];
        const centroids = combos.map((t,i) => {
          const v4 = parseType4D(t); const v3 = hopfToVec3(v4); const { lat, lon } = vec3ToLatLon(v3);
          return { id: i, lat, lon, label: t };
        });
        window.GLOBE.setData({ centroids, points });
        try {
          const dirs = centroids.map(c => {
            // Recompute 3D vectors for shader from lat/lon
            const v = hopfToVec3(parseType4D(c.label));
            return [v.x, v.y, v.z];
          });
          const colorHex = [0x1f77b4,0xff7f0e,0x2ca02c,0xd62728,0x9467bd,0x8c564b,0xe377c2,0x7f7f7f,0xbcbd22,0x17becf,
                            0x8dd3c7,0xffffb3,0xbebada,0xfb8072,0x80b1d3,0xfdb462];
          const cols = centroids.map((c, i) => {
            const h = colorHex[i % colorHex.length];
            const r = ((h >> 16) & 255) / 255, g = ((h >> 8) & 255) / 255, b = (h & 255) / 255;
            return [r, g, b];
          });
          window.GLOBE.setSurfaceMBTI({ dirs, colors: cols });
          const surfSel = selSurface ? selSurface.value : 'mbti';
          if (surfSel === 'reinin') {
            ensureReininSurface();
            try { window.GLOBE.setSurfaceMode('reinin'); } catch {}
          } else if (surfSel === 'mbti') {
            try { window.GLOBE.setSurfaceMode('mbti'); } catch {}
          } else {
            try { window.GLOBE.setSurfaceMode('prismatic'); } catch {}
          }
        } catch {}
        // Save for reuse
        const cidToCluster = new Map();
        lastKMeans = { centroids: centroids.map(c => [c.lat,c.lon]), labels: [], placements: centroids, meta: rows, vecs: null, cidToCluster, points };
        if (inferred > 0) toast(`Projected ${points.length} (inferred ${inferred}) with 4D mapping`); else toast(`Projected ${points.length} with 4D mapping`);
        return;
      }
      // Compute signature to avoid redundant re-placements
      try {
        const ids = rows.map(r => (r && (r.cid || r.id || r.displayName || r.name)) || '').join('|');
        const sig = `k=${k};N=${rows.length};ids=${ids}`;
  if (sig === lastGlobeSig) { try { logDebug('Auto Globe: placement unchanged; skipping'); if (chkDebug && chkDebug.checked) toast('Auto Globe: unchanged; skipped'); } catch {} return; }
        lastGlobeSig = sig;
      } catch {}
      // Build vectors and names
      const vecs = []; const meta = [];
      for (const r of rows) {
        const rec = r && r.cid && window.KNN && window.KNN.getByCid ? window.KNN.getByCid(r.cid) : r;
        const cid = (rec && rec.cid) || r.cid;
        const nm = (rec && (rec.displayName || rec.name)) || (r.displayName || r.name) || cid;
        const v = cid && window.VEC && window.VEC.getVector ? window.VEC.getVector(cid) : null;
        if (!v) continue;
        vecs.push(v);
        meta.push({ cid, label: nm });
      }
      if (!vecs.length) { toast('No vectors in results', false); return; }
      if (!numK.value || isNaN(parseInt(numK.value,10))) k = (window.KMEANS && window.KMEANS.suggestK) ? window.KMEANS.suggestK(vecs.length) : Math.max(3, Math.min(12, Math.round(Math.sqrt(Math.max(1, vecs.length)))));
      // Use worker-based kmeans for large N to keep UI responsive, fallback to inline
      let labels, centroids;
      const D = (vecs[0] && vecs[0].length) || 0;
      const prefsKM = loadPrefs();
      const forceInlineKM = !!(prefsKM && prefsKM.inlineKMeans);
      const USE_WORKER = !forceInlineKM && (vecs.length > 400); // threshold; tune as needed
      if (USE_WORKER) {
        try {
          const worker = new Worker(new URL('./kmeans_worker.js', import.meta.url), { type: 'module' });
          kmeansWorkerRef = worker; showGlobeBusy('Clustering…');
          const buf = new Float32Array(vecs.length * D);
          for (let i = 0; i < vecs.length; i++) buf.set(vecs[i], i * D);
          let rejectFn = null;
          const done = new Promise((resolve, reject) => {
            rejectFn = reject;
            worker.onmessage = (ev) => {
              const m = ev.data || {};
              if (m.type === 'kmeans:start') { setProgress(`KMeans: starting (${m.n}×${m.d}, k=${m.k})…`); setProgressPct(3); try { logDebug(`kmeans:start n=${m.n} d=${m.d} k=${m.k} maxIters=${m.maxIters}`); } catch {} return; }
              if (m.type === 'kmeans:progress') { const pct = Math.min(99, Math.floor((m.iter / Math.max(1, m.maxIters)) * 100)); setProgress(`KMeans: ${pct}% (${m.iter}/${m.maxIters})`); updateGlobeBusy(`Clustering… ${pct}%`); setProgressPct(pct); return; }
              if (m.type === 'kmeans:done') {
                try { worker.terminate(); } catch {}
                kmeansWorkerRef = null; hideGlobeBusy();
                const lab = Array.from(new Int32Array(m.labels));
                const k2 = m.k|0, d2 = m.d|0;
                const cenFlat = new Float32Array(m.centroids);
                const cen = new Array(k2);
                for (let i = 0; i < k2; i++) cen[i] = cenFlat.slice(i * d2, (i+1) * d2);
                const itersTxt = (m.iters && Number.isFinite(m.iters)) ? ` in ${m.iters} iters` : '';
                setProgress(`KMeans: done${itersTxt}.`); resetProgressSoon();
                resolve({ labels: lab, centroids: cen });
              } else if (m.type === 'error') {
                try { worker.terminate(); } catch {}
                kmeansWorkerRef = null; hideGlobeBusy();
                setProgress('KMeans: failed.'); resetProgressSoon();
                reject(new Error(m.message || 'kmeans worker error'));
              }
            };
            worker.onerror = (e) => { try { worker.terminate(); } catch {}; kmeansWorkerRef = null; hideGlobeBusy(); reject(new Error(e.message || 'kmeans worker failed')); };
          });
          worker.postMessage({ type: 'kmeans', buf: buf.buffer, n: vecs.length, d: D, k, maxIters: 60, tol: 1e-4 }, [buf.buffer]);
          // Cancel button wiring
          try { const btnCancelKM = globeBusy && globeBusy.querySelector('#btn-cancel-kmeans'); if (btnCancelKM) btnCancelKM.onclick = () => { try { worker.terminate(); } catch {}; kmeansWorkerRef = null; hideGlobeBusy(); if (rejectFn) rejectFn(new Error('cancelled')); toast('KMeans cancelled'); }; } catch {}
          const res = await done;
          labels = res.labels; centroids = res.centroids;
        } catch (e) {
          // Fallback inline
          if (!window.KMEANS || !window.KMEANS.kmeans) { toast('KMeans unavailable', false); return; }
          hideGlobeBusy();
          const res = window.KMEANS.kmeans(vecs, k, { maxIters: 60, tol: 1e-4 });
          labels = res.labels; centroids = res.centroids;
        }
      } else {
        if (!window.KMEANS || !window.KMEANS.kmeans) { toast('KMeans unavailable', false); return; }
        const res = window.KMEANS.kmeans(vecs, k, { maxIters: 60, tol: 1e-4 });
        labels = res.labels; centroids = res.centroids;
      }
      // Place centroids on the sphere via fibonacci points
      const fib = (function(n){ const pts=[]; const offset=2/n; const inc=Math.PI*(3-Math.sqrt(5)); for(let i=0;i<n;i++){ const y=((i*offset)-1)+(offset/2); const r=Math.sqrt(1-y*y); const phi=(i%n)*inc; const x=Math.cos(phi)*r; const z=Math.sin(phi)*r; const lat=Math.asin(y)*180/Math.PI; const lon=Math.atan2(z,x)*180/Math.PI; pts.push({lat,lon}); } return pts; })(centroids.length);
      const centroidPlacements = fib.map((p, i) => ({ id: i, lat: p.lat, lon: p.lon }));
      // For members, jitter around centroid, scaled by similarity
      const points = new Array(meta.length);
      for (let i=0;i<meta.length;i++){
        const c = labels[i] || 0; const base = centroidPlacements[c];
        // cosine similarity to centroid determines radius
        const sim = 1 - Math.max(0, Math.min(1, (1 - (function(a,b){ let s=0; for(let j=0;j<a.length;j++) s+=a[j]*b[j]; return s; })(vecs[i], centroids[c])) ));
        const maxJitter = Math.max(2, 12 * (1 - sim)); // degrees
        const dLat = (Math.random()*2-1) * maxJitter;
        const dLon = (Math.random()*2-1) * maxJitter;
        points[i] = { cid: meta[i].cid, label: meta[i].label, cluster: c, lat: base.lat + dLat, lon: base.lon + dLon };
      }
  window.GLOBE.setData({ centroids: centroidPlacements, points });
  try {
    const surfSel2 = selSurface ? selSurface.value : 'prismatic';
    if (surfSel2 === 'mbti') {
      ensureMbtiSurface();
      window.GLOBE.setSurfaceMode('mbti');
    } else if (surfSel2 === 'reinin') {
      ensureReininSurface();
      window.GLOBE.setSurfaceMode('reinin');
    } else {
      window.GLOBE.setSurfaceMode('prismatic');
    }
  } catch {}
  // Save for reuse (include exact point lat/lon)
  const cidToCluster = new Map();
  for (let i = 0; i < meta.length; i++) { const id = meta[i].cid; const c = labels[i] || 0; if (id) cidToCluster.set(id, c); }
  lastKMeans = { centroids, labels, placements: centroidPlacements, meta, vecs, cidToCluster, points };
  toast(`Placed ${points.length} results into ${centroids.length} clusters`);
    } catch (e) {
      console.warn(e); toast('Globe render failed', false);
    }

      function ensureMbtiSurface() {
        try {
          const combos = [
            'ESTJ','ESTP','ESFJ','ESFP','ENTJ','ENTP','ENFJ','ENFP',
            'ISTJ','ISTP','ISFJ','ISFP','INTJ','INTP','INFJ','INFP'
          ];
          const dirs = combos.map(t => { const v = hopfToVec3(parseType4D(t)); return [v.x, v.y, v.z]; });
          const colorHex = [0x1f77b4,0xff7f0e,0x2ca02c,0xd62728,0x9467bd,0x8c564b,0xe377c2,0x7f7f7f,0xbcbd22,0x17becf,
                            0x8dd3c7,0xffffb3,0xbebada,0xfb8072,0x80b1d3,0xfdb462];
          const cols = combos.map((_, i) => { const h = colorHex[i % colorHex.length]; const r=((h>>16)&255)/255, g=((h>>8)&255)/255, b=(h&255)/255; return [r,g,b]; });
          window.GLOBE.setSurfaceMBTI({ dirs, colors: cols });
        } catch {}
      }

      function ensureReininSurface() {
        try {
          let colors = null;
          if (Array.isArray(reininColorsLocal) && reininColorsLocal.length === 24) {
            colors = reininColorsLocal;
          } else {
            // Default 24-color palette (distinct/lightfast). Order maps to reininFaceIndex 0..23.
            const hex = [
              0x1f77b4,0xff7f0e,0x2ca02c,0xd62728, // +X faces
              0x9467bd,0x8c564b,0xe377c2,0x7f7f7f, // -X faces
              0xbcbd22,0x17becf,0x8dd3c7,0xffffb3, // +Y faces
              0xbebada,0xfb8072,0x80b1d3,0xfdb462, // -Y faces
              0xa6cee3,0x1f78b4,0xb2df8a,0x33a02c, // +Z faces
              0xfb9a99,0xe31a1c,0xfdbf6f,0xff7f00  // -Z faces
            ];
            colors = hex.map(h => [((h>>16)&255)/255, ((h>>8)&255)/255, (h&255)/255]);
            reininColorsLocal = colors;
          }
          if (window.GLOBE && window.GLOBE.setSurfaceReinin) window.GLOBE.setSurfaceReinin({ colors });
          // Ensure legend view stays in sync
          renderReininLegendView();
        } catch {}
      }

      if (selSurface) {
        selSurface.addEventListener('change', () => {
          const m = selSurface.value;
          if (m === 'mbti') { ensureMbtiSurface(); try { window.GLOBE.setSurfaceMode('mbti'); } catch {} }
          else if (m === 'reinin') { ensureReininSurface(); try { if (Array.isArray(reininNamesLocal) && reininNamesLocal.length===24 && window.GLOBE && window.GLOBE.setReininFaceNames) window.GLOBE.setReininFaceNames(reininNamesLocal); } catch {} try { window.GLOBE.setSurfaceMode('reinin'); } catch {} }
          else { try { window.GLOBE.setSurfaceMode('prismatic'); } catch {} }
          // Clear Reinin highlight if leaving Reinin mode
          try { if (window.GLOBE && window.GLOBE.setReininHighlight && m !== 'reinin') window.GLOBE.setReininHighlight(-1); } catch {}
          savePrefs();
          try { updateReininButtonVisibility(); } catch {}
          try { renderReininLegendView(); } catch {}
        });
      }
  }

  if (btnShowGlobe) btnShowGlobe.onclick = async () => { await placeOnGlobe(); };

  // Toggle globe options
  function applyGlobeOptions(){
    try {
      if (window.GLOBE && window.GLOBE.setOptions) window.GLOBE.setOptions({
        links: !!(chkGlobeLinks && chkGlobeLinks.checked),
        labels: !!(chkGlobeLabels && chkGlobeLabels.checked),
        grid: !!(chkGlobeGrid && chkGlobeGrid.checked)
      });
    } catch {}
    savePrefs();
  }
  if (chkGlobeLinks) chkGlobeLinks.addEventListener('change', applyGlobeOptions);
  if (chkGlobeLabels) chkGlobeLabels.addEventListener('change', applyGlobeOptions);
  if (chkGlobeGrid) chkGlobeGrid.addEventListener('change', applyGlobeOptions);
  // Contour/axis wiring
  function applyContourSettings(){
    try {
      const preset = (selContourPreset && selContourPreset.value) || 'vivid';
      const colors = (function(){
        if (preset === 'soft') return [[0.95,0.80,0.55],[0.96,0.70,0.72],[0.70,0.90,0.95],[0.75,0.90,0.75]];
        if (preset === 'mono') return [[1,1,1],[0.9,0.9,0.9],[0.8,0.8,0.8],[0.7,0.7,0.7]];
        return [[1,0.756,0.027],[0.902,0.098,0.294],[0,0.737,0.831],[0.545,0.764,0.294]];
      })();
      const enabled = !!(chkContours && chkContours.checked);
      const width = rangeContourWidth ? (parseFloat(rangeContourWidth.value) || 0.09) : 0.09;
  const intensity = rangeContourIntensity ? (parseFloat(rangeContourIntensity.value) || 0.55) : 0.55;
    let normals;
    const np = (selNormalsPreset && selNormalsPreset.value) || 'globe';
  if (np === 'mbti') { const n = MBTI_DEFAULT_NORMALS; normals = [n.nE,n.nN,n.nF,n.nP]; }
  else if (np === 'orthogonal') { const N=(v)=>{ const L=Math.hypot(v[0],v[1],v[2])||1; return [v[0]/L,v[1]/L,v[2]/L]; }; normals = [N([1,0,0]),N([0,1,0]),N([0,0,1]),N([1,1,1])]; }
  else if (np === 'tetra') { normals = [TETRAHEDRAL_NORMALS[0], TETRAHEDRAL_NORMALS[1], TETRAHEDRAL_NORMALS[2], TETRAHEDRAL_NORMALS[3]]; }
    else if (np === 'custom') {
      const v = getCustomNormalsFromInputs();
      if (v && v.length === 4) normals = v;
    }
  if (window.GLOBE && window.GLOBE.setGreatCircles) window.GLOBE.setGreatCircles({ enabled, width, intensity, colors, ...(normals?{ normals }: {}) });
      const ax = (selPolarAxis && selPolarAxis.value) || 'y';
      const axisVec = ax === 'x' ? [1,0,0] : (ax === 'z' ? [0,0,1] : [0,1,0]);
      const angleDeg = rangePolarAngle ? (parseInt(rangePolarAngle.value,10) || 0) : 0;
      if (window.GLOBE && window.GLOBE.setPolarRotation) window.GLOBE.setPolarRotation({ axis: axisVec, angle: angleDeg * Math.PI / 180 });
    } catch {}
  }
  if (chkContours) chkContours.addEventListener('change', () => { applyContourSettings(); savePrefs(); });
  if (rangeContourWidth) rangeContourWidth.addEventListener('input', () => { applyContourSettings(); });
  if (rangeContourWidth) rangeContourWidth.addEventListener('input', () => { if (valContourWidth) valContourWidth.textContent = Number(rangeContourWidth.value).toFixed(3); });
  if (rangeContourIntensity) rangeContourIntensity.addEventListener('input', () => { applyContourSettings(); });
  if (rangeContourIntensity) rangeContourIntensity.addEventListener('input', () => { if (valContourIntensity) valContourIntensity.textContent = Number(rangeContourIntensity.value).toFixed(2); });
  if (selContourPreset) selContourPreset.addEventListener('change', () => { applyContourSettings(); savePrefs(); });
  function maybeReproject4D(){ try { if (planetVisible && selPlacement && selPlacement.value==='4d') placeOnGlobe(); } catch {} }
  if (chkFuncAware) chkFuncAware.addEventListener('change', () => { savePrefs(); maybeReproject4D(); });
  if (rangeFuncOffset) rangeFuncOffset.addEventListener('input', () => { if (valFuncOffset) valFuncOffset.textContent = `${Number(rangeFuncOffset.value).toFixed(1)}°`; });
  if (rangeFuncOffset) rangeFuncOffset.addEventListener('change', () => { savePrefs(); maybeReproject4D(); });
  if (rangeFuncRoll) rangeFuncRoll.addEventListener('input', () => { if (valFuncRoll) valFuncRoll.textContent = `${Number(rangeFuncRoll.value).toFixed(1)}°`; });
  if (rangeFuncRoll) rangeFuncRoll.addEventListener('change', () => { savePrefs(); maybeReproject4D(); });
  function bindWeight(rangeEl, valEl){ if (!rangeEl) return; rangeEl.addEventListener('input', () => { if (valEl) valEl.textContent = Number(rangeEl.value).toFixed(2); }); rangeEl.addEventListener('change', () => { savePrefs(); maybeReproject4D(); }); }
  bindWeight(rangeWDom, valWDom);
  bindWeight(rangeWAux, valWAux);
  bindWeight(rangeWTer, valWTer);
  bindWeight(rangeWInf, valWInf);
  function toggleCustomNormalsUI(){
    try {
      const useCustom = selNormalsPreset && selNormalsPreset.value === 'custom';
      if (customNormalsWrap) customNormalsWrap.style.display = useCustom ? 'block' : 'none';
      if (useCustom && !inpNormE.value && !inpNormN.value && !inpNormF.value && !inpNormP.value) {
        // seed with MBTI defaults for convenience
        const n = MBTI_DEFAULT_NORMALS;
        setCustomNormalsInputs([n.nE, n.nN, n.nF, n.nP]);
      }
    } catch {}
  }
  if (selNormalsPreset) selNormalsPreset.addEventListener('change', () => { toggleCustomNormalsUI(); applyContourSettings(); savePrefs(); maybeReproject4D(); });
  if (inpNormE) inpNormE.addEventListener('change', () => { if (selNormalsPreset.value==='custom') { applyContourSettings(); savePrefs(); maybeReproject4D(); } });
  if (inpNormN) inpNormN.addEventListener('change', () => { if (selNormalsPreset.value==='custom') { applyContourSettings(); savePrefs(); maybeReproject4D(); } });
  if (inpNormF) inpNormF.addEventListener('change', () => { if (selNormalsPreset.value==='custom') { applyContourSettings(); savePrefs(); maybeReproject4D(); } });
  if (inpNormP) inpNormP.addEventListener('change', () => { if (selNormalsPreset.value==='custom') { applyContourSettings(); savePrefs(); maybeReproject4D(); } });
  if (selPolarAxis) selPolarAxis.addEventListener('change', () => { applyContourSettings(); savePrefs(); });
  if (rangePolarAngle) rangePolarAngle.addEventListener('input', () => { applyContourSettings(); });
  if (btnResetAxis) btnResetAxis.addEventListener('click', () => { if (rangePolarAngle) rangePolarAngle.value = '0'; applyContourSettings(); savePrefs(); });
  if (btnResetGuides) btnResetGuides.addEventListener('click', () => {
    try {
      if (chkContours) chkContours.checked = true;
      if (rangeContourWidth) { rangeContourWidth.value = '0.09'; if (valContourWidth) valContourWidth.textContent = '0.090'; }
      if (rangeContourIntensity) { rangeContourIntensity.value = '0.55'; if (valContourIntensity) valContourIntensity.textContent = '0.55'; }
      if (selContourPreset) selContourPreset.value = 'vivid';
      if (selNormalsPreset) selNormalsPreset.value = 'globe';
      if (customNormalsWrap) customNormalsWrap.style.display = 'none';
      if (inpNormE) inpNormE.value = ''; if (inpNormN) inpNormN.value = ''; if (inpNormF) inpNormF.value = ''; if (inpNormP) inpNormP.value = '';
      if (selPolarAxis) selPolarAxis.value = 'y';
      if (rangePolarAngle) rangePolarAngle.value = '0';
  if (chkFuncAware) chkFuncAware.checked = true;
  if (rangeFuncOffset) { rangeFuncOffset.value = '10'; if (valFuncOffset) valFuncOffset.textContent = '10.0°'; }
  if (rangeWDom) { rangeWDom.value = '0.5'; if (valWDom) valWDom.textContent = '0.50'; }
  if (rangeWAux) { rangeWAux.value = '0.3'; if (valWAux) valWAux.textContent = '0.30'; }
  if (rangeWTer) { rangeWTer.value = '0.15'; if (valWTer) valWTer.textContent = '0.15'; }
  if (rangeWInf) { rangeWInf.value = '0.05'; if (valWInf) valWInf.textContent = '0.05'; }
  if (rangeFuncRoll) { rangeFuncRoll.value = '6'; if (valFuncRoll) valFuncRoll.textContent = '6.0°'; }
  applyContourSettings(); maybeReproject4D();
      savePrefs();
    } catch {}
  });
  function updateReininButtonVisibility(){ try { const on = (selSurface && selSurface.value === 'reinin'); if (btnReininPalette) btnReininPalette.style.display = on ? 'inline-block' : 'none'; if (btnReininLegend) btnReininLegend.style.display = 'inline-block'; } catch {} }
  updateReininButtonVisibility();
  if (btnReininPalette) btnReininPalette.addEventListener('click', () => {
    try {
      // Build editor on demand
      let editor = document.getElementById('reinin-editor');
      if (!editor) {
        editor = document.createElement('div'); editor.id = 'reinin-editor';
        editor.style.position = 'absolute'; editor.style.right = '16px'; editor.style.top = '16px'; editor.style.left = '16px';
        editor.style.background = 'rgba(15,23,42,0.96)'; editor.style.border = '1px solid rgba(255,255,255,0.12)'; editor.style.borderRadius = '12px'; editor.style.padding = '12px'; editor.style.zIndex = '10';
        const hdr = document.createElement('div'); hdr.style.display='flex'; hdr.style.alignItems='center'; hdr.style.justifyContent='space-between'; hdr.style.marginBottom='8px';
        const title = document.createElement('div'); title.textContent = 'Reinin Palette (24 faces)'; title.style.color='#fff'; title.style.fontWeight='600';
        const btnClose = document.createElement('button'); btnClose.textContent='✕'; btnClose.style.border='0'; btnClose.style.background='transparent'; btnClose.style.color='#fff'; btnClose.style.cursor='pointer'; btnClose.onclick = () => { try { editor.remove(); } catch {} };
        hdr.appendChild(title); hdr.appendChild(btnClose); editor.appendChild(hdr);
        const grid = document.createElement('div'); grid.style.display='grid'; grid.style.gridTemplateColumns='auto auto auto auto'; grid.style.gap='8px 12px'; grid.id='reinin-grid'; editor.appendChild(grid);
        const btnRow = document.createElement('div'); btnRow.style.display='flex'; btnRow.style.gap='8px'; btnRow.style.marginTop='10px';
        const btnSave = document.createElement('button'); btnSave.textContent='Save'; btnSave.style.border='1px solid #bbb'; btnSave.style.background='#fff'; btnSave.style.borderRadius='6px'; btnSave.style.padding='0.25em 0.5em'; btnSave.style.cursor='pointer';
        const btnReset = document.createElement('button'); btnReset.textContent='Reset'; btnReset.style.border='1px solid #bbb'; btnReset.style.background='#fff'; btnReset.style.borderRadius='6px'; btnReset.style.padding='0.25em 0.5em'; btnReset.style.cursor='pointer';
        const btnExport = document.createElement('button'); btnExport.textContent='Export'; btnExport.style.border='1px solid #bbb'; btnExport.style.background='#fff'; btnExport.style.borderRadius='6px'; btnExport.style.padding='0.25em 0.5em'; btnExport.style.cursor='pointer';
        const lblImport = document.createElement('label'); lblImport.textContent='Import'; lblImport.style.display='inline-flex'; lblImport.style.alignItems='center'; lblImport.style.gap='0.3em'; lblImport.style.border='1px solid #bbb'; lblImport.style.background='#fff'; lblImport.style.borderRadius='6px'; lblImport.style.padding='0.25em 0.5em'; lblImport.style.cursor='pointer';
        const inpImport = document.createElement('input'); inpImport.type='file'; inpImport.accept='application/json,.json'; inpImport.style.display='none'; lblImport.appendChild(inpImport);
        btnRow.appendChild(btnSave); btnRow.appendChild(btnReset); btnRow.appendChild(btnExport); btnRow.appendChild(lblImport);
        editor.appendChild(btnRow);
        if (planetSidebar) planetSidebar.appendChild(editor); else document.body.appendChild(editor);
        // Helpers
        const toHex = (rgb)=>{ try { const r=Math.round((rgb[0]||0)*255), g=Math.round((rgb[1]||0)*255), b=Math.round((rgb[2]||0)*255); return '#'+[r,g,b].map(x=>x.toString(16).padStart(2,'0')).join(''); } catch { return '#888888'; } };
        const fromHex = (h)=>{ try { const m=/^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(h||''); if(!m) return [0.5,0.5,0.5]; return [parseInt(m[1],16)/255, parseInt(m[2],16)/255, parseInt(m[3],16)/255]; } catch { return [0.5,0.5,0.5]; } };
        const faceLabel = (idx)=>{
          const base = Math.floor(idx/4); const q = idx%4;
          const axis = base===0?'+X':base===1?'-X':base===2?'+Y':base===3?'-Y':base===4?'+Z':'-Z';
          const quad = q===0?'Q1':q===1?'Q2':q===2?'Q3':'Q4';
          return `${axis} · ${quad}`;
        };
        const buildGrid = ()=>{
          grid.innerHTML='';
          // Ensure local colors exist
          if (!Array.isArray(reininColorsLocal) || reininColorsLocal.length!==24) { ensureReininSurface(); }
          for (let i=0;i<24;i++){
            const row = document.createElement('div'); row.style.display='contents';
            const lbl = document.createElement('div'); lbl.textContent=faceLabel(i); lbl.style.color='#fff'; lbl.style.fontSize='12px';
            const inp = document.createElement('input'); inp.type='color'; inp.value = toHex(reininColorsLocal && reininColorsLocal[i] ? reininColorsLocal[i] : [0.5,0.5,0.5]); inp.dataset.index=String(i);
            const sw = document.createElement('div'); sw.style.width='24px'; sw.style.height='24px'; sw.style.borderRadius='4px'; sw.style.border='1px solid rgba(255,255,255,0.15)'; sw.style.background=inp.value;
            inp.addEventListener('input', ()=>{ sw.style.background = inp.value; });
            grid.appendChild(lbl); grid.appendChild(inp); grid.appendChild(sw);
            const spacer = document.createElement('div'); spacer.textContent=''; grid.appendChild(spacer);
          }
        };
        buildGrid();
        btnSave.onclick = ()=>{
          try {
            const inputs = grid.querySelectorAll('input[type="color"][data-index]');
            const colors = new Array(24);
            inputs.forEach(inp => { const i = parseInt(inp.dataset.index||'0',10); colors[i] = fromHex(inp.value); });
            reininColorsLocal = colors;
            if (window.GLOBE && window.GLOBE.setSurfaceReinin) window.GLOBE.setSurfaceReinin({ colors });
            savePrefs(); toast('Reinin palette saved');
      try { renderReininLegendView(); } catch {}
          } catch { toast('Save failed', false); }
        };
    btnReset.onclick = ()=>{ try { reininColorsLocal = null; ensureReininSurface(); buildGrid(); savePrefs(); toast('Reinin palette reset'); try { renderReininLegendView(); } catch {} } catch { toast('Reset failed', false); } };
        btnExport.onclick = ()=>{
          try {
            const payload = { kind: 'planet.reinin.colors', version: 1, timestamp: new Date().toISOString(), colors: (Array.isArray(reininColorsLocal)&&reininColorsLocal.length===24)?reininColorsLocal:null };
            const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
            const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'reinin_colors.json'; a.click(); URL.revokeObjectURL(a.href);
            toast('Palette exported');
          } catch { toast('Export failed', false); }
        };
    inpImport.addEventListener('change', async (ev)=>{
          try {
            const f = ev.target.files && ev.target.files[0]; if (!f) return;
            const txt = await f.text(); const js = JSON.parse(txt);
            const arr = (js && js.colors) ? js.colors : js;
            if (!Array.isArray(arr) || arr.length!==24) throw new Error('Bad file');
            reininColorsLocal = arr;
            if (window.GLOBE && window.GLOBE.setSurfaceReinin) window.GLOBE.setSurfaceReinin({ colors: reininColorsLocal });
      buildGrid(); savePrefs(); toast('Palette imported'); ev.target.value=''; try { renderReininLegendView(); } catch {}
          } catch { toast('Import failed', false); }
        });
      }
    } catch {}
  });
  if (btnReininLegend) btnReininLegend.addEventListener('click', () => {
    try {
      let editor = document.getElementById('reinin-legend-editor');
      if (!editor) {
        editor = document.createElement('div'); editor.id = 'reinin-legend-editor';
        editor.style.position = 'absolute'; editor.style.right = '16px'; editor.style.top = '16px'; editor.style.left = '16px';
        editor.style.background = 'rgba(15,23,42,0.96)'; editor.style.border = '1px solid rgba(255,255,255,0.12)'; editor.style.borderRadius = '12px'; editor.style.padding = '12px'; editor.style.zIndex = '10';
        const hdr = document.createElement('div'); hdr.style.display='flex'; hdr.style.alignItems='center'; hdr.style.justifyContent='space-between'; hdr.style.marginBottom='8px';
        const title = document.createElement('div'); title.textContent = 'Reinin Legend (24 names)'; title.style.color='#fff'; title.style.fontWeight='600';
        const btnClose = document.createElement('button'); btnClose.textContent='✕'; btnClose.style.border='0'; btnClose.style.background='transparent'; btnClose.style.color='#fff'; btnClose.style.cursor='pointer'; btnClose.onclick = () => { try { editor.remove(); } catch {} };
        hdr.appendChild(title); hdr.appendChild(btnClose); editor.appendChild(hdr);
        const grid = document.createElement('div'); grid.style.display='grid'; grid.style.gridTemplateColumns='auto 1fr'; grid.style.gap='8px 12px'; grid.id='reinin-legend-grid'; editor.appendChild(grid);
        const btnRow = document.createElement('div'); btnRow.style.display='flex'; btnRow.style.gap='8px'; btnRow.style.marginTop='10px';
        const btnSave = document.createElement('button'); btnSave.textContent='Save'; btnSave.style.border='1px solid #bbb'; btnSave.style.background='#fff'; btnSave.style.borderRadius='6px'; btnSave.style.padding='0.25em 0.5em'; btnSave.style.cursor='pointer';
        const btnReset = document.createElement('button'); btnReset.textContent='Reset'; btnReset.style.border='1px solid #bbb'; btnReset.style.background='#fff'; btnReset.style.borderRadius='6px'; btnReset.style.padding='0.25em 0.5em'; btnReset.style.cursor='pointer';
        const btnExport = document.createElement('button'); btnExport.textContent='Export'; btnExport.style.border='1px solid #bbb'; btnExport.style.background='#fff'; btnExport.style.borderRadius='6px'; btnExport.style.padding='0.25em 0.5em'; btnExport.style.cursor='pointer';
        const lblImport = document.createElement('label'); lblImport.textContent='Import'; lblImport.style.display='inline-flex'; lblImport.style.alignItems='center'; lblImport.style.gap='0.3em'; lblImport.style.border='1px solid #bbb'; lblImport.style.background='#fff'; lblImport.style.borderRadius='6px'; lblImport.style.padding='0.25em 0.5em'; lblImport.style.cursor='pointer';
        const inpImport = document.createElement('input'); inpImport.type='file'; inpImport.accept='application/json,.json'; inpImport.style.display='none'; lblImport.appendChild(inpImport);
        btnRow.appendChild(btnSave); btnRow.appendChild(btnReset); btnRow.appendChild(btnExport); btnRow.appendChild(lblImport);
        editor.appendChild(btnRow);
        if (planetSidebar) planetSidebar.appendChild(editor); else document.body.appendChild(editor);
        const faceLabel = (idx)=>{ const base = Math.floor(idx/4); const q = idx%4; const axis = base===0?'+X':base===1?'-X':base===2?'+Y':base===3?'-Y':base===4?'+Z':'-Z'; const quad = q===0?'Q1':q===1?'Q2':q===2?'Q3':'Q4'; return `${axis} · ${quad}`; };
        const buildGrid = ()=>{
          grid.innerHTML='';
          if (!Array.isArray(reininNamesLocal) || reininNamesLocal.length!==24) {
            // seed defaults to face labels
            reininNamesLocal = new Array(24).fill(0).map((_,i)=>faceLabel(i));
          }
          for (let i=0;i<24;i++){
            const lbl = document.createElement('div'); lbl.textContent = faceLabel(i); lbl.style.color='#fff'; lbl.style.fontSize='12px';
            const inp = document.createElement('input'); inp.type='text'; inp.placeholder = faceLabel(i); inp.value = reininNamesLocal[i] || ''; inp.dataset.index=String(i);
            inp.style.padding='0.25em 0.4em'; inp.style.border='1px solid #bbb'; inp.style.borderRadius='6px';
            grid.appendChild(lbl); grid.appendChild(inp);
          }
        };
        buildGrid();
  btnSave.onclick = ()=>{ try { const inputs = grid.querySelectorAll('input[type="text"][data-index]'); const names = new Array(24); inputs.forEach(inp=>{ const i=parseInt(inp.dataset.index||'0',10); names[i]=String(inp.value||'').trim()||faceLabel(i); }); reininNamesLocal = names; try { if (window.GLOBE && window.GLOBE.setReininFaceNames) window.GLOBE.setReininFaceNames(reininNamesLocal); } catch {} savePrefs(); toast('Legend saved'); try { renderReininLegendView(); } catch {} } catch { toast('Save failed', false); } };
  btnReset.onclick = ()=>{ try { reininNamesLocal = null; try { if (window.GLOBE && window.GLOBE.setReininFaceNames) window.GLOBE.setReininFaceNames(new Array(24).fill(0).map((_,i)=>faceLabel(i))); } catch {} buildGrid(); savePrefs(); toast('Legend reset'); try { renderReininLegendView(); } catch {} } catch { toast('Reset failed', false); } };
        btnExport.onclick = ()=>{ try { const payload={ kind:'planet.reinin.legend', version:1, timestamp:new Date().toISOString(), names:(Array.isArray(reininNamesLocal)&&reininNamesLocal.length===24)?reininNamesLocal:null }; const blob=new Blob([JSON.stringify(payload,null,2)],{type:'application/json'}); const a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download='reinin_legend.json'; a.click(); URL.revokeObjectURL(a.href); toast('Legend exported'); } catch { toast('Export failed', false); } };
  inpImport.addEventListener('change', async (ev)=>{ try { const f=ev.target.files&&ev.target.files[0]; if(!f) return; const txt=await f.text(); const js=JSON.parse(txt); const arr=(js&&js.names)?js.names:js; if(!Array.isArray(arr)||arr.length!==24) throw new Error('Bad file'); reininNamesLocal = arr.map(x=>String(x||'')); try { if (window.GLOBE && window.GLOBE.setReininFaceNames) window.GLOBE.setReininFaceNames(reininNamesLocal); } catch {} buildGrid(); savePrefs(); toast('Legend imported'); ev.target.value=''; try { renderReininLegendView(); } catch {} } catch { toast('Import failed', false); } });
      }
    } catch {}
  });
  // Export/Import globe settings (globe prefs only)
  if (btnExportGlobeSettings) btnExportGlobeSettings.addEventListener('click', () => {
    try {
      const prefs = loadPrefs();
      const g = (prefs && prefs.globe) ? prefs.globe : {};
      const payload = {
        kind: 'planet.globe.settings',
        version: 1,
        timestamp: new Date().toISOString(),
        globe: g
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'globe_settings.json'; a.click(); URL.revokeObjectURL(a.href);
      toast('Settings exported');
    } catch { toast('Export failed', false); }
  });
  function applyImportedGlobeSettings(g){
    try {
      if (!g || typeof g !== 'object') return;
      // Basic toggles
      if (typeof g.links === 'boolean' && chkGlobeLinks) chkGlobeLinks.checked = g.links;
      if (typeof g.labels === 'boolean' && chkGlobeLabels) chkGlobeLabels.checked = g.labels;
      if (typeof g.grid === 'boolean' && chkGlobeGrid) chkGlobeGrid.checked = g.grid;
      // Contours and colors
      if (typeof g.contours === 'boolean' && chkContours) chkContours.checked = g.contours;
      if (typeof g.contourWidth === 'number' && rangeContourWidth) { rangeContourWidth.value = String(g.contourWidth); if (valContourWidth) valContourWidth.textContent = Number(g.contourWidth).toFixed(3); }
      if (typeof g.contourIntensity === 'number' && rangeContourIntensity) { rangeContourIntensity.value = String(g.contourIntensity); if (valContourIntensity) valContourIntensity.textContent = Number(g.contourIntensity).toFixed(2); }
      if (g.contourPreset && selContourPreset) selContourPreset.value = g.contourPreset;
      // Normals
      if (g.normalsPreset && selNormalsPreset) selNormalsPreset.value = g.normalsPreset;
      toggleCustomNormalsUI();
      if (Array.isArray(g.customNormals) && g.customNormals.length === 4) setCustomNormalsInputs(g.customNormals);
  // Function-aware
  if (typeof g.funcAware === 'boolean' && chkFuncAware) chkFuncAware.checked = g.funcAware;
  if (typeof g.funcOffsetDeg === 'number' && rangeFuncOffset) { rangeFuncOffset.value = String(g.funcOffsetDeg); if (valFuncOffset) valFuncOffset.textContent = `${Number(g.funcOffsetDeg).toFixed(1)}°`; }
      if (g.funcRoleWeights) {
        if (rangeWDom && typeof g.funcRoleWeights.dominant==='number') { rangeWDom.value = String(g.funcRoleWeights.dominant); if (valWDom) valWDom.textContent = Number(rangeWDom.value).toFixed(2); }
        if (rangeWAux && typeof g.funcRoleWeights.auxiliary==='number') { rangeWAux.value = String(g.funcRoleWeights.auxiliary); if (valWAux) valWAux.textContent = Number(rangeWAux.value).toFixed(2); }
        if (rangeWTer && typeof g.funcRoleWeights.tertiary==='number') { rangeWTer.value = String(g.funcRoleWeights.tertiary); if (valWTer) valWTer.textContent = Number(rangeWTer.value).toFixed(2); }
        if (rangeWInf && typeof g.funcRoleWeights.inferior==='number') { rangeWInf.value = String(g.funcRoleWeights.inferior); if (valWInf) valWInf.textContent = Number(rangeWInf.value).toFixed(2); }
      }
  if (typeof g.funcAttitudeDeg === 'number' && rangeFuncRoll) { rangeFuncRoll.value = String(g.funcAttitudeDeg); if (valFuncRoll) valFuncRoll.textContent = `${Number(g.funcAttitudeDeg).toFixed(1)}°`; }
      // Axis
      if (g.polarAxis && selPolarAxis) selPolarAxis.value = g.polarAxis;
      if (typeof g.polarAngle === 'number' && rangePolarAngle) rangePolarAngle.value = String(g.polarAngle);
  // Surface + Reinin palette + names
      if (Array.isArray(g.reininColors) && g.reininColors.length === 24) {
        reininColorsLocal = g.reininColors;
        try { if (window.GLOBE && window.GLOBE.setSurfaceReinin) window.GLOBE.setSurfaceReinin({ colors: reininColorsLocal }); } catch {}
      }
  if (Array.isArray(g.reininNames) && g.reininNames.length === 24) { reininNamesLocal = g.reininNames; try { if (window.GLOBE && window.GLOBE.setReininFaceNames) window.GLOBE.setReininFaceNames(reininNamesLocal); } catch {} }
      if (g.surface && selSurface) selSurface.value = g.surface;
      // Legend collapse/density preferences
      try {
        if (typeof g.reininLegendCollapsed === 'boolean') {
          localStorage.setItem('compass_reinin_legend_collapsed', g.reininLegendCollapsed ? '1' : '0');
        }
        if (typeof g.reininLegendDensity === 'string') {
          localStorage.setItem('compass_reinin_legend_density', g.reininLegendDensity);
          reininLegendDensity = g.reininLegendDensity;
        }
      } catch {}
  // Apply and persist
      applyGlobeOptions();
      applyContourSettings();
  try { renderReininLegendView(); } catch {}
      // Apply selected surface now
      try {
        const surf = (selSurface && selSurface.value) || 'prismatic';
        if (surf === 'mbti') { ensureMbtiSurface(); window.GLOBE.setSurfaceMode('mbti'); }
  else if (surf === 'reinin') { ensureReininSurface(); window.GLOBE.setSurfaceMode('reinin'); try { renderReininLegendView(); } catch {} }
        else { window.GLOBE.setSurfaceMode('prismatic'); }
      } catch {}
      savePrefs();
    } catch {}
  }
  if (fileImportGlobeSettings) fileImportGlobeSettings.addEventListener('change', async (ev) => {
    try {
      const f = ev.target.files && ev.target.files[0];
      if (!f) return;
      const txt = await f.text();
      const js = JSON.parse(txt);
      const g = (js && js.globe) ? js.globe : js; // accept raw globe object too
      applyImportedGlobeSettings(g);
      toast('Settings imported');
      ev.target.value = '';
    } catch (e) { toast('Import failed', false); }
  });
  if (chkAutoGlobe) chkAutoGlobe.addEventListener('change', savePrefs);
  if (numAutoThreshold) numAutoThreshold.addEventListener('change', savePrefs);
  if (chkInlineKMeans) chkInlineKMeans.addEventListener('change', savePrefs);

  if (btnExportPlanet) btnExportPlanet.onclick = async () => {
    try {
      if (!lastKMeans || !lastKMeans.placements) throw new Error('No globe data');
      const prefs = loadPrefs();
      const payload = {
        timestamp: new Date().toISOString(),
        options: { links: !!(prefs && prefs.globe && prefs.globe.links), labels: !!(prefs && prefs.globe && prefs.globe.labels) },
        k: (lastKMeans && lastKMeans.centroids && lastKMeans.centroids.length) || 0,
        n: (lastKMeans && lastKMeans.meta && lastKMeans.meta.length) || 0,
        query: (input && input.value) || '',
        sources: {
          profiles: (window.KNN && window.KNN.getSource && window.KNN.getSource()) || '',
          vectors: (window.VEC && window.VEC.getSource && window.VEC.getSource()) || ''
        },
        hnsw: {
          ready: (window.VEC && window.VEC.isHnswReady && window.VEC.isHnswReady()) || false,
          ef: (window.VEC && window.VEC.getEfSearch && window.VEC.getEfSearch()) || 64,
          ctor: window.__HNSW_CTOR || '',
          cacheState: (window.VEC && window.VEC.getCacheState && window.VEC.getCacheState()) || 'none'
        },
        pca: (window.VEC && window.VEC.getPcaInfo && window.VEC.getPcaInfo()) || {},
        centroids: lastKMeans.placements,
        // Use exact jittered point positions if available
        points: Array.isArray(lastKMeans.points) && lastKMeans.points.length
          ? lastKMeans.points.map(p => ({ cid: p.cid, label: p.label, cluster: p.cluster, lat: p.lat, lon: p.lon }))
          : (function(){ const out=[]; const meta=lastKMeans.meta||[]; const labels=lastKMeans.labels||[]; for(let i=0;i<meta.length;i++){ const cid=meta[i].cid; const label=meta[i].label; const cluster=labels[i]||0; const c=lastKMeans.placements[cluster]; out.push({ cid, label, cluster, lat: c.lat, lon: c.lon }); } return out; })()
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'personality_planet.json'; a.click(); URL.revokeObjectURL(a.href);
      toast('Planet exported');
    } catch (e) { toast(e && e.message ? e.message : 'Export failed', false); }
  };

  // Auto-show globe on load when explicitly in planet view
  try {
    const qs = new URLSearchParams(location.search);
    const v = qs.get('view') || (localStorage.getItem('ui_view') || '');
    const isPlanet = (v === 'planet');
    if (isPlanet) {
      // Just open the globe UI panel; actual placement happens on search
      ensureGlobe();
    }
  } catch {}

  // React to globe selection events (point or centroid)
  window.addEventListener('globe:select', (e) => {
    const d = e && e.detail || {};
    try {
      if (typeof d.cluster === 'number') {
        if (window.GLOBE && window.GLOBE.highlightCluster) window.GLOBE.highlightCluster(d.cluster);
      }
      if (d.cid) {
        // Show info card
        try {
          const rec = (window.KNN && window.KNN.getByCid) ? window.KNN.getByCid(d.cid) : null;
          if (planetInfo && rec) {
            const name = (rec.displayName || rec.name || rec.cid || '');
            const tags = [rec.mbti, rec.socionics, rec.big5].filter(Boolean).join(' · ');
            const desc = (rec.description || '').slice(0, 240);
            planetInfo.innerHTML = `
              <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:4px;">
                <strong style="font-size:13px;">${name}</strong>
                <button id="btn-infocard-close" style="border:0;background:transparent;color:#fff;cursor:pointer;font-size:14px;">✕</button>
              </div>
              <div style="color:#ddd;margin-bottom:6px;">${tags}</div>
              ${desc ? `<div style="color:#eee;margin-bottom:8px;">${desc}${(rec.description||'').length>240?'…':''}</div>` : ''}
              <div style="display:flex;gap:8px;">
                <button id="btn-infocard-add" style="border:1px solid #bbb;background:#fff;color:#000;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Add to chart</button>
                <button id="btn-infocard-select" style="border:1px solid #999;background:#222;color:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Highlight in list</button>
              </div>
            `;
            planetInfo.style.display = 'block';
            const btnClose = planetInfo.querySelector('#btn-infocard-close');
            const btnAdd = planetInfo.querySelector('#btn-infocard-add');
            const btnSel = planetInfo.querySelector('#btn-infocard-select');
            if (btnClose) btnClose.onclick = () => { planetInfo.style.display = 'none'; };
            if (btnAdd) btnAdd.onclick = () => {
              try {
                const row = { cid: rec.cid, displayName: rec.displayName || rec.name || rec.cid };
                const person = computePersonVisual(row);
                if (!window.personalityData) window.personalityData = [];
                const exists = window.personalityData.some(p => (p.cid && person.cid) ? p.cid === person.cid : p.label === person.label);
                if (!exists) {
                  window.personalityData.push(person);
                  if (window.refreshCompass) window.refreshCompass();
                  toast('Added to chart');
                } else {
                  toast('Already added');
                }
              } catch {}
            };
            if (btnSel) btnSel.onclick = () => {
              try {
                const rows = resultsDiv ? resultsDiv.querySelectorAll('div[data-index]') : [];
                let found = -1;
                currentResults.forEach((r, i) => { if (r && r.cid === rec.cid) found = i; });
                if (found >= 0) {
                  selectedIndex = found;
                  highlightSelected();
                  const el = resultsDiv.querySelector(`div[data-index="${found}"]`);
                  if (el && el.scrollIntoView) el.scrollIntoView({ block: 'nearest' });
                }
              } catch {}
            };
          }
        } catch {}
        // Try to scroll/select in results list
        const rows = resultsDiv ? resultsDiv.querySelectorAll('div[data-index]') : [];
        let found = -1;
        currentResults.forEach((r, i) => { if (r && r.cid === d.cid) found = i; });
        if (found >= 0) {
          selectedIndex = found;
          highlightSelected();
          const el = resultsDiv.querySelector(`div[data-index="${found}"]`);
          if (el && el.scrollIntoView) el.scrollIntoView({ block: 'nearest' });
        }
      }
    } catch {}
  });

  function vecDot(a, b) { let s = 0; for (let i = 0; i < a.length; i++) s += a[i]*b[i]; return s; }
  function toLatLonFromVec3(x, y, z) {
    const r = Math.sqrt(x*x + y*y + z*z) || 1;
    const lat = Math.asin(y / r) * 180 / Math.PI;
    const lon = Math.atan2(z, x) * 180 / Math.PI;
    return { lat, lon };
  }
  if (btnMyPos) btnMyPos.onclick = async () => {
    try {
      if (!planetVisible) { if (!(await (async () => { if (!ensureGlobe()) return false; return true; })())) return; }
      if (!lastKMeans || !lastKMeans.centroids) { toast('Run Show on Globe first', false); return; }
      const text = (myText && myText.value || '').trim();
      if (!text) { toast('Enter some text', false); return; }
      if (!window.KNN || !window.KNN.embed) { toast('Embed unavailable', false); return; }
      // Smooth placement: average the vectors of top text matches, then compute nearest cluster
      let v = window.KNN.embed(text);
      try {
        if (window.KNN && window.KNN.search) {
          const top = window.KNN.search(text, 10) || [];
          const acc = new Float32Array(v.length);
          let cnt = 0;
          for (const r of top) {
            const cid = r && r.cid;
            const vv = cid && window.VEC && window.VEC.getVector ? window.VEC.getVector(cid) : null;
            if (!vv) continue;
            for (let i = 0; i < acc.length; i++) acc[i] += vv[i];
            cnt++;
          }
          if (cnt >= 3) { for (let i = 0; i < acc.length; i++) acc[i] /= cnt; v = acc; }
        }
      } catch {}
      // Cosine similarity to centroids
      let best = -1, bi = -1;
      for (let i = 0; i < lastKMeans.centroids.length; i++) {
        const s = vecDot(v, lastKMeans.centroids[i]);
        if (s > best) { best = s; bi = i; }
      }
      if (bi < 0) { toast('Could not determine cluster', false); return; }
      // Place marker using PCA projection if available
      let lat, lon;
      if (window.VEC && window.VEC.projectVector) {
        const p = window.VEC.projectVector(v);
        if (p && p.length >= 3) {
          const x = p[0], y = p[1], z = p[2];
          ({ lat, lon } = toLatLonFromVec3(x, y, z));
        }
      }
      if (lat == null || lon == null) {
        const c = lastKMeans.centroids[bi];
        const len = Math.sqrt(vecDot(c, c)) || 1;
        const x = c[0]/len, y = c[1]/len, z = (c[2]||0)/len;
        ({ lat, lon } = toLatLonFromVec3(x, y, z));
      }
      window.GLOBE.setMyMarker({ lat, lon, label: 'Me' });
      window.GLOBE.highlightCluster(bi);
      toast(`Nearest cluster: ${bi} (sim ${(best*100).toFixed(0)}%)`);
    } catch (e) { console.warn(e); toast('My position failed', false); }
  };
  if (chkInlinePca) chkInlinePca.addEventListener('change', () => {
    try {
      if (chkInlinePca.checked) localStorage.setItem('vec_disable_pca_worker', '1');
      else localStorage.removeItem('vec_disable_pca_worker');
      toast(`Inline PCA ${chkInlinePca.checked ? 'enabled' : 'disabled'}. Reload or reload vectors to apply.`, true);
    } catch {}
  });
  if (chkDisableWorker) chkDisableWorker.addEventListener('change', () => {
    try {
      if (chkDisableWorker.checked) localStorage.setItem('vec_disable_worker', '1');
      else localStorage.removeItem('vec_disable_worker');
      toast(`Inline HNSW ${chkDisableWorker.checked ? 'enabled' : 'disabled'}. Reload or rebuild index to apply.`, true);
    } catch {}
  });
  if (numEfc) numEfc.addEventListener('change', () => {
    const v = parseInt(numEfc.value, 10) || 200;
    if (window.VEC && window.VEC.setEfConstruction) window.VEC.setEfConstruction(v);
    savePrefs();
    toast(`efC set to ${v}`);
  });
  if (numEf) numEf.addEventListener('change', () => {
    const v = parseInt(numEf.value, 10) || 64;
    if (window.VEC && window.VEC.setEfSearch) window.VEC.setEfSearch(v);
    savePrefs();
    updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0);
    toast(`ef set to ${v}`);
  });

  // --- Helpers for Custom normals ---
  function NormalizeVec3(v){ const L = Math.hypot(v[0]||0, v[1]||0, v[2]||0) || 1; return [ (v[0]||0)/L, (v[1]||0)/L, (v[2]||0)/L ]; }
  function parseVec3(str){
    try {
      if (!str || typeof str !== 'string') return null;
      const parts = str.split(/[,\s]+/).map(s => s.trim()).filter(Boolean).slice(0,3);
      if (parts.length < 3) return null;
      const v = parts.map(Number);
      if (v.some(x => !isFinite(x))) return null;
      const L = Math.hypot(v[0], v[1], v[2]);
      if (!(L > 0)) return null;
      return [ v[0]/L, v[1]/L, v[2]/L ];
    } catch { return null; }
  }
  function setValidityBorder(inputEl, ok){ if (!inputEl) return; inputEl.style.borderColor = ok ? '#bbb' : '#d9534f'; inputEl.style.background = ok ? '#fff' : 'rgba(217,83,79,0.08)'; }
  function getCustomNormalsFromInputs(){
    try {
      const e = parseVec3(inpNormE && inpNormE.value); setValidityBorder(inpNormE, !!e);
      const n = parseVec3(inpNormN && inpNormN.value); setValidityBorder(inpNormN, !!n);
      const f = parseVec3(inpNormF && inpNormF.value); setValidityBorder(inpNormF, !!f);
      const p = parseVec3(inpNormP && inpNormP.value); setValidityBorder(inpNormP, !!p);
      if (e && n && f && p) return [e,n,f,p];
      return null;
    } catch { return null; }
  }
  function setCustomNormalsInputs(arr){
    try {
      const toStr = (v)=> Array.isArray(v) && v.length===3 ? `${(+v[0]).toFixed(3)},${(+v[1]).toFixed(3)},${(+v[2]).toFixed(3)}` : '';
      if (inpNormE) inpNormE.value = toStr(arr[0]||[]);
      if (inpNormN) inpNormN.value = toStr(arr[1]||[]);
      if (inpNormF) inpNormF.value = toStr(arr[2]||[]);
      if (inpNormP) inpNormP.value = toStr(arr[3]||[]);
    } catch {}
  }
  // --- Presets manager for Custom normals ---
  // UI elements
  (function initCustomNormalsPresetsUI(){
    try {
      if (!customNormalsWrap) return;
      const row = document.createElement('div');
      row.style.marginTop = '0.5em';
      row.style.display = 'grid';
  row.style.gridTemplateColumns = 'auto 1fr auto auto auto';
      row.style.gap = '0.4em 0.6em';
      const lbl = document.createElement('label');
      lbl.textContent = 'Presets'; lbl.style.color = '#444';
      const sel = document.createElement('select'); sel.id = 'sel-custom-normals-preset'; sel.style.padding = '0.25em 0.4em'; sel.style.border = '1px solid #bbb'; sel.style.borderRadius = '6px';
      const name = document.createElement('input'); name.type = 'text'; name.placeholder = 'Name'; name.id = 'name-custom-normals-preset'; name.style.padding = '0.25em 0.4em'; name.style.border = '1px solid #bbb'; name.style.borderRadius = '6px';
      const btnSave = document.createElement('button'); btnSave.id = 'btn-save-custom-normals'; btnSave.textContent = 'Save'; btnSave.style.border = '1px solid #bbb'; btnSave.style.background = '#fff'; btnSave.style.borderRadius = '6px'; btnSave.style.padding = '0.25em 0.5em'; btnSave.style.cursor = 'pointer';
      const btnLoad = document.createElement('button'); btnLoad.id = 'btn-load-custom-normals'; btnLoad.textContent = 'Load'; btnLoad.style.border = '1px solid #bbb'; btnLoad.style.background = '#fff'; btnLoad.style.borderRadius = '6px'; btnLoad.style.padding = '0.25em 0.5em'; btnLoad.style.cursor = 'pointer';
      const btnDel = document.createElement('button'); btnDel.id = 'btn-del-custom-normals'; btnDel.textContent = 'Delete'; btnDel.style.border = '1px solid #bbb'; btnDel.style.background = '#fff'; btnDel.style.borderRadius = '6px'; btnDel.style.padding = '0.25em 0.5em'; btnDel.style.cursor = 'pointer';
      row.appendChild(lbl); row.appendChild(sel); row.appendChild(name); row.appendChild(btnSave); row.appendChild(btnLoad); row.appendChild(btnDel);
      customNormalsWrap.appendChild(row);
  // Secondary row for export/import of presets
  const row2 = document.createElement('div');
  row2.style.marginTop = '0.4em';
  row2.style.display = 'flex';
  row2.style.gap = '0.5em';
  const btnExportPresets = document.createElement('button'); btnExportPresets.id = 'btn-export-custom-normals-presets'; btnExportPresets.textContent = 'Export Presets'; btnExportPresets.style.border = '1px solid #bbb'; btnExportPresets.style.background = '#fff'; btnExportPresets.style.borderRadius = '6px'; btnExportPresets.style.padding = '0.25em 0.5em'; btnExportPresets.style.cursor = 'pointer';
  const lblImport = document.createElement('label'); lblImport.textContent = 'Import Presets'; lblImport.style.display = 'inline-flex'; lblImport.style.alignItems = 'center'; lblImport.style.gap = '0.3em'; lblImport.style.border = '1px solid #bbb'; lblImport.style.background = '#fff'; lblImport.style.borderRadius = '6px'; lblImport.style.padding = '0.25em 0.5em'; lblImport.style.cursor = 'pointer';
  const inpImportPresets = document.createElement('input'); inpImportPresets.type = 'file'; inpImportPresets.accept = 'application/json,.json'; inpImportPresets.style.display = 'none'; inpImportPresets.id = 'file-import-custom-normals-presets';
  lblImport.htmlFor = 'file-import-custom-normals-presets';
  lblImport.appendChild(inpImportPresets);
  row2.appendChild(btnExportPresets); row2.appendChild(lblImport);
  customNormalsWrap.appendChild(row2);
      // Functions
      function getPresets(){
        try { const raw = localStorage.getItem('planet_custom_normals_presets'); return raw ? JSON.parse(raw) : {}; } catch { return {}; }
      }
      function setPresets(obj){ try { localStorage.setItem('planet_custom_normals_presets', JSON.stringify(obj||{})); } catch {} }
      function refresh(){
        try {
          const ps = getPresets();
          sel.innerHTML = '';
          const keys = Object.keys(ps).sort();
          for (const k of keys) { const o = document.createElement('option'); o.value = k; o.textContent = k; sel.appendChild(o); }
          if (!keys.length) { const o = document.createElement('option'); o.value = ''; o.textContent = '(none)'; sel.appendChild(o); }
        } catch {}
      }
      function saveCurrent(nameStr){
        try {
          const v = getCustomNormalsFromInputs();
          if (!v) { toast('Enter valid normals first', false); return; }
          const n = String(nameStr || '').trim(); if (!n) { toast('Enter a name', false); return; }
          const ps = getPresets(); ps[n] = v; setPresets(ps); refresh(); toast('Preset saved');
        } catch { toast('Save failed', false); }
      }
      function loadPreset(k){
        try {
          const ps = getPresets(); const v = ps && ps[k]; if (!v) { toast('Preset not found', false); return; }
          setCustomNormalsInputs(v);
          if (selNormalsPreset) selNormalsPreset.value = 'custom';
          toggleCustomNormalsUI();
          applyContourSettings();
          savePrefs();
          toast('Preset loaded');
        } catch { toast('Load failed', false); }
      }
      function deletePreset(k){
        try { const ps = getPresets(); if (!ps || !(k in ps)) { toast('Preset not found', false); return; } delete ps[k]; setPresets(ps); refresh(); toast('Preset deleted'); } catch { toast('Delete failed', false); }
      }
      // Wire events
      btnSave.addEventListener('click', () => saveCurrent(name.value));
      btnLoad.addEventListener('click', () => { const k = sel && sel.value; if (k) loadPreset(k); });
      btnDel.addEventListener('click', () => { const k = sel && sel.value; if (k) deletePreset(k); });
      btnExportPresets.addEventListener('click', () => {
        try {
          const ps = getPresets();
          const payload = { kind: 'planet.customNormals.presets', version: 1, timestamp: new Date().toISOString(), presets: ps };
          const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
          const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'custom_normals_presets.json'; a.click(); URL.revokeObjectURL(a.href);
          toast('Presets exported');
        } catch { toast('Export failed', false); }
      });
      inpImportPresets.addEventListener('change', async (ev) => {
        try {
          const f = ev.target.files && ev.target.files[0]; if (!f) return;
          const txt = await f.text(); const js = JSON.parse(txt);
          const incoming = (js && js.presets) ? js.presets : js; // accept raw map
          if (!incoming || typeof incoming !== 'object') throw new Error('Bad file');
          const ps = getPresets();
          // merge (overwrite by name)
          for (const [k, v] of Object.entries(incoming)) { if (Array.isArray(v) && v.length === 4) ps[k] = v; }
          setPresets(ps); refresh(); toast('Presets imported');
          ev.target.value = '';
        } catch { toast('Import failed', false); }
      });
      refresh();
    } catch {}
  })();
  // Initialize visibility based on loaded prefs
  toggleCustomNormalsUI();
});
