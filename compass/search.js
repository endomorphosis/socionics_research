// Search UI and logic for highlighting personalities

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
        <button id="btn-clear-debug" title="Clear debug log" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Clear</button>
        <button id="btn-copy-debug" title="Copy debug log to clipboard" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Copy</button>
      </span>
    </div>
  <div id="progress-bar" style="height:6px;background:#eee;border-radius:4px;overflow:hidden;margin:-0.25em 0 0.5em 0;">
    <div id="progress-bar-fill" style="height:100%;width:0;background:#4363d8;transition:width 0.12s ease;"></div>
  </div>
  <pre id="debug-log" style="display:none;white-space:pre-wrap;background:#fafafa;border:1px solid #eee;border-radius:6px;padding:0.5em;color:#444;max-height:160px;overflow:auto;margin:0 0 0.5em 0;"></pre>
  <div id="toast" style="position:fixed;bottom:16px;left:16px;max-width:60vw;background:#333;color:#fff;padding:0.6em 0.8em;border-radius:6px;opacity:0;transform:translateY(10px);transition:opacity 0.2s, transform 0.2s;z-index:1006;"></div>
  <div id="search-results" style="max-height:260px;overflow-y:auto;"></div>
  `;
  document.body.appendChild(searchDiv);

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
  const debugPre = searchDiv.querySelector('#debug-log');
  const btnClearDebug = searchDiv.querySelector('#btn-clear-debug');
  const btnCopyDebug = searchDiv.querySelector('#btn-copy-debug');
  const progressFill = searchDiv.querySelector('#progress-bar-fill');
  const toastEl = searchDiv.querySelector('#toast');
  let currentResults = [];
  let selectedIndex = -1;

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
  efConstruction: numEfc ? (parseInt(numEfc.value, 10) || 200) : 200
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
      resultsDiv.innerHTML = `<div style='color:#c00;padding:0.5em;'>Vectors load failed: ${e.message}</div>`;
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
      resultsDiv.innerHTML = `<div style='color:#c00;padding:0.5em;'>Profiles load failed: ${e.message}</div>`;
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
});
