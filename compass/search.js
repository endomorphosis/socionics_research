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
  <small style="color:#666">Tip: prefix with cid:Qm... to use vector KNN by CID</small>
    <div id="search-status" style="color:#666;margin:0.25em 0 0.25em 0;font-size:0.85em;"></div>
    <div id="search-progress" style="color:#888;margin:0 0 0.5em 0;font-size:0.8em;height:1.2em;"></div>
    <div id="search-actions" style="display:flex;gap:0.5em;margin-bottom:0.5em;flex-wrap:wrap;align-items:center;">
      <button id="btn-rebuild-index" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Rebuild index</button>
      <button id="btn-cancel-build" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;" disabled>Cancel</button>
  <button id="btn-cancel-build" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;" disabled>Cancel</button>
      <button id="btn-clear-cache" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Clear cache</button>
      <select id="sel-vectors" title="Choose vectors parquet" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;min-width:200px;"></select>
      <button id="btn-load-parquet" title="Load vectors via DuckDB-Wasm" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Load Parquet</button>
      <select id="sel-profiles" title="Choose profiles parquet" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;min-width:200px;"></select>
      <button id="btn-load-profiles" title="Load profiles for KNN" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Load Profiles</button>
      <button id="btn-export-index" title="Export HNSW index" style="border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">Export Index</button>
      <label for="file-import-index" style="display:inline-flex;align-items:center;gap:0.3em;border:1px solid #bbb;background:#fff;border-radius:6px;padding:0.25em 0.5em;cursor:pointer;">
        Import Index <input type="file" id="file-import-index" accept="application/octet-stream,.bin" style="display:none" />
      </label>
      <label style="display:flex;align-items:center;gap:0.3em;color:#444;font-size:0.9em;">
        <input type="checkbox" id="chk-auto-parquet" /> Auto Parquet
      </label>
    </div>
  <div id="progress-bar" style="height:6px;background:#eee;border-radius:4px;overflow:hidden;margin:-0.25em 0 0.5em 0;">
    <div id="progress-bar-fill" style="height:100%;width:0;background:#4363d8;transition:width 0.12s ease;"></div>
  </div>
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
  const btnParquet = searchDiv.querySelector('#btn-load-parquet');
  const btnProfiles = searchDiv.querySelector('#btn-load-profiles');
  const btnExportIdx = searchDiv.querySelector('#btn-export-index');
  const fileImportIdx = searchDiv.querySelector('#file-import-index');
  const selVectors = searchDiv.querySelector('#sel-vectors');
  const selProfiles = searchDiv.querySelector('#sel-profiles');
  const chkAuto = searchDiv.querySelector('#chk-auto-parquet');
  const progressFill = searchDiv.querySelector('#progress-bar-fill');
  const toastEl = searchDiv.querySelector('#toast');
  let currentResults = [];
  let selectedIndex = -1;

  function savePrefs() {
    try {
      const prefs = {
        vectors: selVectors && selVectors.value || '',
        profiles: selProfiles && selProfiles.value || '',
        autoParquet: !!(chkAuto && chkAuto.checked)
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
      // If first run (no prefs) and we have valid options, default-enable auto parquet
      if (!prefs && selVectors.options.length && selProfiles.options.length) {
        if (chkAuto) chkAuto.checked = true;
      }
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
      const m = q.match(/^cid:([A-Za-z0-9]+)/);
      if (m && window.VEC && window.VEC.similarByCid) {
        isVector = true;
        const cid = m[1];
        const hits = window.VEC.similarByCid(cid, 20);
        filtered = hits.map(h => {
          const rec = window.KNN && window.KNN.getByCid ? window.KNN.getByCid(h.cid) : null;
          return { cid: h.cid, name: rec?.name || h.cid, mbti: rec?.mbti, socionics: rec?.socionics, big5: rec?.big5, _score: h._score };
        });
      } else {
        filtered = window.KNN.search(q, 20);
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
            const label = row.name || row.cid || 'Unknown';
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
    const label = row.name || row.cid || 'Unknown';
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
  if (statusDiv) statusDiv.textContent = `Profiles: ${nProfiles} (${pShort}) · Vectors: ${vecN} (${srcShort}) · HNSW: ${hnsw ? 'ready' : 'off'} · Cache: ${cache}${building ? ' · Building…' : ''}`;
  }

  function setProgress(msg) { if (progressDiv) progressDiv.textContent = msg || ''; }
  function setProgressPct(pct){ if (progressFill) progressFill.style.width = `${Math.max(0,Math.min(100, pct|0))}%`; }
  function resetProgressSoon(){ setTimeout(()=> setProgressPct(0), 800); }
  function toast(msg, ok=true){ if(!toastEl) return; toastEl.textContent = msg; toastEl.style.background = ok ? '#2ca02c' : '#c0392b'; toastEl.style.opacity = '1'; toastEl.style.transform = 'translateY(0)'; setTimeout(()=>{ toastEl.style.opacity='0'; toastEl.style.transform='translateY(10px)'; }, 2200); }

  function setBuildingUI(on){
    try{
      if (btnRebuild) btnRebuild.disabled = !!on;
      if (btnCancel) btnCancel.disabled = !on;
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
  window.addEventListener('vec:pca:error', () => { setProgress('PCA: failed.'); resetProgressSoon(); setBuildingUI(false); });
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
  window.addEventListener('vec:hnsw:cache:loaded', () => { setProgress('HNSW: loaded from cache.'); resetProgressSoon(); });
  window.addEventListener('vec:hnsw:cache:saved', () => { setProgress('HNSW: cached.'); resetProgressSoon(); });
  window.addEventListener('vec:hnsw:error', () => { setProgress('HNSW: error (fallback).'); resetProgressSoon(); setBuildingUI(false); updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0); });

  if (btnRebuild) btnRebuild.onclick = async () => {
    if (window.VEC && window.VEC.rebuildIndex) {
      resultsDiv.innerHTML = '<div style="color:#888;padding:0.5em;">Rebuilding index…</div>';
      setProgress('HNSW: rebuilding…');
      setBuildingUI(true);
      await window.VEC.rebuildIndex();
      updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0);
      resultsDiv.innerHTML = '<div style="color:#2ca02c;padding:0.5em;">Index rebuilt.</div>';
      setBuildingUI(false);
    }
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

  if (btnClear) btnClear.onclick = async () => {
    if (window.VEC && window.VEC.clearHnswCache) {
      const ok = await window.VEC.clearHnswCache();
      updateStatus(window.KNN && window.KNN.size ? window.KNN.size() : 0);
      resultsDiv.innerHTML = ok ? '<div style="color:#2ca02c;padding:0.5em;">Cache cleared.</div>' : '<div style="color:#c00;padding:0.5em;">Failed to clear cache.</div>';
    }
  };

  if (btnExportIdx) btnExportIdx.onclick = async () => {
    if (!window.VEC || !window.VEC.exportIndex) return;
    const bytes = await window.VEC.exportIndex();
    if (!bytes) { toast('Export failed', false); return; }
    const blob = new Blob([bytes], { type: 'application/octet-stream' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'hnsw_index.bin';
    a.click();
    URL.revokeObjectURL(a.href);
    toast('Index exported');
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
      btnParquet.disabled = true;
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
    } finally { btnParquet.disabled = false; }
  };

  if (btnProfiles) btnProfiles.onclick = async () => {
    const url = selProfiles && selProfiles.value ? selProfiles.value : '/dataset/pdb_profiles_normalized.parquet';
    resultsDiv.innerHTML = `<div style=\"color:#888;padding:0.5em;\">Loading profiles from ${baseName(url)}…</div>`;
    try {
      btnProfiles.disabled = true;
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
    } finally { btnProfiles.disabled = false; }
  };
  if (selVectors) selVectors.addEventListener('change', savePrefs);
  if (selProfiles) selProfiles.addEventListener('change', savePrefs);
});
