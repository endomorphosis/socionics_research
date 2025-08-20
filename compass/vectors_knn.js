// Client-side vector KNN using real embeddings loaded from JSON

const VEC = (() => {
  let V = []; // Array of Float32Array vectors
  let IDS = []; // Parallel array of cid strings
  let INDEX = new Map(); // cid -> index
  let MEAN = null; // Float64Array mean vector
  let PC = null;   // principal components [pc1, pc2, pc3] as Float64Array
  let SCALE = null; // {min:[3], max:[3]}
  let PROJ = new Map(); // cid -> [x,y,z] in [-1,1]
  let HNSW = null; // wasm lib instance
  let INDEX_HNSW = null; // built index
  let WORKER = null; // web worker for heavy tasks
  let BUILDING = false; // whether PCA/HNSW is building
  let EF_SEARCH = 64; // tunable efSearch for HNSW queries
  let EF_CONSTRUCTION = 200; // tunable efConstruction for HNSW build
  const PENDING = new Set(); // pending worker promise rejectors
  let VEC_URL = null; // dataset url for signature
  let CACHE_STATE = 'none'; // 'none' | 'loaded' | 'saved' | 'error'
  let USE_CACHE = true; // whether to attempt to load/save from IndexedDB
  let EIG = null; // Float64Array of top eigenvalues
  let TOTVAR = 0; // total variance (trace of covariance)
  // Persistently disable worker build path if wasm FS is unavailable (avoids repeated errors)
  function getWorkerDisabled(){ try { return localStorage.getItem('vec_disable_worker') === '1'; } catch { return false; } }
  function setWorkerDisabled(v){ try { if (v) localStorage.setItem('vec_disable_worker','1'); else localStorage.removeItem('vec_disable_worker'); } catch {} }
  // Separate flag for PCA worker failures (RangeError, etc.)
  function getPcaWorkerDisabled(){ try { return localStorage.getItem('vec_disable_pca_worker') === '1'; } catch { return false; } }
  function setPcaWorkerDisabled(v){ try { if (v) localStorage.setItem('vec_disable_pca_worker','1'); else localStorage.removeItem('vec_disable_pca_worker'); } catch {} }

  function emit(name, detail){ try { window.dispatchEvent(new CustomEvent(name, { detail })); } catch {} }

  function cosine(a, b) {
    let dot = 0, na = 0, nb = 0;
    for (let i = 0; i < a.length; i++) {
      const x = a[i], y = b[i];
      dot += x * y; na += x * x; nb += y * y;
    }
    const denom = Math.sqrt(na) * Math.sqrt(nb) || 1;
    return dot / denom;
  }

  async function load(url = '/pdb_profile_vectors.json') {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`Failed to load vectors: ${resp.status}`);
    const rows = await resp.json();
    VEC_URL = url;
    return loadFromRows(rows);
  }

  async function loadFromRows(rows) {
    // 1) Collect raw arrays and determine the target dimension by majority vote (mode)
    const raw = rows.map(r => ({ cid: r.cid, vec: Array.isArray(r.vector) ? r.vector.map(Number) : [] }));
    const lenCounts = new Map();
    for (const { vec } of raw) {
      const L = vec.length | 0;
      if (L > 0) lenCounts.set(L, (lenCounts.get(L) || 0) + 1);
    }
    // Choose the dimension with the highest frequency; tie-breaker: larger dimension
    let targetDim = 0, bestCount = -1;
    for (const [L, c] of lenCounts.entries()) {
      if (c > bestCount || (c === bestCount && L > targetDim)) { targetDim = L; bestCount = c; }
    }
    if (!targetDim) {
      throw new Error('No non-empty vectors found');
    }
    // 2) Normalize every row to targetDim: pad with 0s or truncate; coerce non-finite to 0
    const kept = [];
    let padded = 0, truncated = 0, fixedNonFinite = 0, dropped = 0;
    for (const { cid, vec } of raw) {
      if (!cid || !vec || vec.length === 0) { dropped++; continue; }
      let out = new Array(targetDim);
      if (vec.length >= targetDim) {
        for (let j = 0; j < targetDim; j++) {
          const x = vec[j]; const v = Number.isFinite(x) ? x : 0; if (!Number.isFinite(x)) fixedNonFinite++;
          out[j] = v;
        }
        if (vec.length > targetDim) truncated++;
      } else {
        for (let j = 0; j < vec.length; j++) {
          const x = vec[j]; const v = Number.isFinite(x) ? x : 0; if (!Number.isFinite(x)) fixedNonFinite++;
          out[j] = v;
        }
        for (let j = vec.length; j < targetDim; j++) out[j] = 0;
        padded++;
      }
      kept.push({ cid, vec: out });
    }
    try { 
      const msg = `sanitized vectors: kept=${kept.length} dropped=${dropped} dim=${targetDim}`
        + (padded||truncated||fixedNonFinite ? ` (padded=${padded}, truncated=${truncated}, fixedNonFinite=${fixedNonFinite})` : '');
      if (dropped || padded || truncated || fixedNonFinite) console.warn(`Sanitized vectors: ${msg}`);
      emit('vec:hnsw:debug', { message: msg });
    } catch {}

    // 3) Build typed arrays and indices
    V = new Array(kept.length);
    IDS = new Array(kept.length);
    INDEX = new Map();
    for (let i = 0; i < kept.length; i++) {
      const { cid, vec } = kept[i];
      const f32 = new Float32Array(targetDim);
      f32.set(vec);
      V[i] = f32;
      IDS[i] = cid;
      INDEX.set(cid, i);
    }
    console.log(`Vectors loaded: ${kept.length}`);
    if (rows.length && V[0]) {
      // Launch PCA in worker to keep UI responsive
      try {
        BUILDING = true;
        if (getPcaWorkerDisabled()) {
          try { emit('vec:hnsw:debug', { message: 'pca worker disabled; computing inline' }); } catch {}
          computePCAAndProjections();
        } else {
          emit('vec:pca:start', { count: rows.length, dim: V[0].length });
          await runPCAInWorker();
        }
        emit('vec:pca:end', {});
      } catch (e) {
        console.warn('PCA (worker) failed, fallback inline:', e);
        try { setPcaWorkerDisabled(true); emit('vec:hnsw:debug', { message: 'pca worker failed; disabling worker path' }); } catch {}
        try {
          computePCAAndProjections();
          emit('vec:pca:end', {});
        } catch (e2) {
          console.warn('PCA projection failed:', e2);
          emit('vec:pca:error', { message: String(e2) });
        }
      } finally {
        BUILDING = false;
      }
      try {
        // Try loading index from cache first; fall back to building
        let ok = false;
        if (USE_CACHE) ok = await loadHnswFromCache();
        if (!ok) await buildHnswIndexWorkerFirst();
      } catch (e) {
        console.warn('HNSW init failed (fallback to brute force):', e);
        emit('vec:hnsw:error', { message: String(e) });
      }
    }
  return kept.length;
  }

  async function loadFromParquet(parquetUrl = '/dataset/pdb_profile_vectors.parquet') {
    if (!window.DuckVec || !window.DuckVec.loadVectors) {
      throw new Error('DuckDB-Wasm loader not available');
    }
    const rows = await window.DuckVec.loadVectors(parquetUrl);
    VEC_URL = parquetUrl;
    return loadFromRows(rows);
  }

  function similarByVector(vec, k = 10, filterCids = null) {
    if (INDEX_HNSW) {
      // Use HNSW index (cosine metric). Distances are (1 - cosine).
      let res;
      try {
        if (typeof INDEX_HNSW.searchKnn === 'function') {
          res = INDEX_HNSW.searchKnn(vec, k);
        } else if (typeof INDEX_HNSW.knnQuery === 'function') {
          // Some builds expose knnQuery returning arrays
          const out = INDEX_HNSW.knnQuery([vec], k);
          // Normalize to { neighbors, distances }
          if (out && Array.isArray(out) && out.length > 0) {
            const row = out[0];
            if (row && row.neighbors && row.distances) {
              res = row;
            } else if (Array.isArray(row)) {
              res = { neighbors: row.map((r) => r[0] | 0), distances: row.map((r) => r[1] ?? 0) };
            }
          }
        }
      } catch (e) {
        console.warn('HNSW search failed, falling back:', e);
        res = null;
      }
      if (res && res.neighbors) {
        const out = [];
        const neigh = res.neighbors;
        const dists = res.distances || new Float32Array(neigh.length);
        for (let i = 0; i < neigh.length; i++) {
          const idx = neigh[i] | 0;
          // Optional client-side filter
          if (filterCids && filterCids.has(IDS[idx])) continue;
          const sim = 1 - (dists[i] ?? 0);
          out.push({ cid: IDS[idx], _score: sim });
          if (out.length >= k) break;
        }
        return out;
      }
    }
    const scores = [];
    for (let i = 0; i < V.length; i++) {
      if (filterCids && filterCids.has(IDS[i])) continue;
      scores.push([cosine(vec, V[i]), i]);
    }
    scores.sort((a, b) => b[0] - a[0]);
    const out = [];
    for (let j = 0; j < Math.min(k, scores.length); j++) {
      const [score, idx] = scores[j];
      out.push({ cid: IDS[idx], _score: score });
    }
    return out;
  }

  function similarByCid(cid, k = 10) {
    const idx = INDEX.get(cid);
    if (idx == null) return [];
    const filter = new Set([cid]);
    return similarByVector(V[idx], k, filter);
  }

  function getVector(cid) {
    const idx = INDEX.get(cid);
    return idx == null ? null : V[idx];
  }

  async function buildHnswIndex() {
    try { emit('vec:hnsw:build:start', { count: V.length, dim: V[0].length }); } catch {}
    // lazy load wasm lib and build index
  const { loadHnswlib } = await import('./hnswlib_loader.js');
    HNSW = await loadHnswlib();
    const dim = V[0].length;
  // Create index using 3-arg ctor when required, fallback to 2-arg
  let index;
  try {
    index = createHnswIndex(HNSW, dim, V.length);
  } catch (e) {
    throw new Error('HNSW ctor failed on main thread: ' + (e && e.message || e));
  }
  try { emit('vec:hnsw:ctor', { which: (index && index._ctorWhich) || ((index && index.maxElements) ? '3-arg' : '2-arg') }); } catch {}
    // Initialize if available; try multiple signatures
  try { if (typeof index.initIndex === 'function') initIndexCompat(index, dim, V.length, EF_CONSTRUCTION); } catch {}
  try { setEf(index, EF_SEARCH); } catch {}
  // add items using array-of-rows to align with documented API (addPoints)
  const n = V.length, d = dim;
  const rows = V.map((row) => row);
  const labels = new Array(n);
  for (let i = 0; i < n; i++) labels[i] = i;
  if (typeof index.addPoints === 'function') {
    index.addPoints(rows, labels, false);
  } else if (typeof index.addItems === 'function') {
    try { index.addItems(rows, false, labels); }
    catch (e1) { try { index.addItems(rows, false); } catch (e2) { index.addItems(rows); } }
  } else {
    throw new Error('HNSW index missing addPoints/addItems');
  }
    INDEX_HNSW = index;
    console.log('HNSW index built');
    try { emit('vec:hnsw:build:end', {}); } catch {}
    // Attempt to persist to cache
    try {
      await saveHnswToCache();
      try { emit('vec:hnsw:cache:saved', {}); } catch {}
    } catch (e) {
      console.warn('HNSW cache save failed:', e);
    }
  }

  async function buildHnswIndexWorkerFirst() {
    if (!V.length) return;
    if (getWorkerDisabled()) {
      try { emit('vec:hnsw:debug', { message: 'worker disabled; using main thread' }); } catch {}
      await buildHnswIndex();
      return;
    }
    try { emit('vec:hnsw:build:start', { count: V.length, dim: V[0].length }); } catch {}
    // Flatten V into a contiguous Float32Array buffer [n*d]
    const n = V.length; const d = V[0].length;
    const flat = new Float32Array(n * d);
    for (let i = 0; i < n; i++) flat.set(V[i], i * d);
    try {
  BUILDING = true;
  const w = getWorker();
  const bytes = await postWorker('hnsw:build', { buf: flat.buffer, n, d, efC: EF_CONSTRUCTION }, [flat.buffer]);
  // Create an empty index and read bytes
  const { loadHnswlib } = await import('./hnswlib_loader.js');
      HNSW = await loadHnswlib();
  const index = createHnswIndex(HNSW, d, n);
      try { emit('vec:hnsw:ctor', { which: (index && index._ctorWhich) || ((index && index.maxElements) ? '3-arg' : '2-arg') }); } catch {}
  readIndexCompat(index, new Uint8Array(bytes), HNSW);
  try { setEf(index, EF_SEARCH); } catch {}
      INDEX_HNSW = index;
      try { emit('vec:hnsw:build:end', {}); } catch {}
      try { await saveHnswToCache(); emit('vec:hnsw:cache:saved', {}); } catch {}
  BUILDING = false;
      return;
    } catch (e) {
      console.warn('HNSW worker build failed, building on main thread:', e);
      try { emit('vec:hnsw:debug', { message: 'worker failed; disabling worker path' }); } catch {}
      setWorkerDisabled(true);
      try { await buildHnswIndex(); } finally { BUILDING = false; }
    }
  }

  function computePCAAndProjections() {
    const n = V.length;
    const d = V[0].length;
    // mean
    MEAN = new Float64Array(d);
    for (let i = 0; i < n; i++) {
      const v = V[i];
      for (let j = 0; j < d; j++) MEAN[j] += v[j];
    }
    for (let j = 0; j < d; j++) MEAN[j] /= n;

    // covariance (symmetric dxd)
    const C = new Float64Array(d * d);
    for (let i = 0; i < n; i++) {
      const v = V[i];
      for (let a = 0; a < d; a++) {
        const va = v[a] - MEAN[a];
        let row = a * d;
        for (let b = a; b < d; b++) {
          const vb = v[b] - MEAN[b];
          C[row + b] += va * vb;
        }
      }
    }
    const inv = 1 / Math.max(1, n - 1);
    // mirror upper to lower and scale
    for (let a = 0; a < d; a++) {
      for (let b = a; b < d; b++) {
        // scale covariance values
        const val = C[a * d + b] * inv;
        C[a * d + b] = val;
        if (a !== b) C[b * d + a] = val;
      }
    }

  // power iteration to get top 3 eigenvectors
    function powerIter(Cm, d, iters = 60) {
      let v = new Float64Array(d);
      for (let i = 0; i < d; i++) v[i] = Math.random() - 0.5;
      normalize(v);
      const y = new Float64Array(d);
      for (let t = 0; t < iters; t++) {
        // y = C v
        y.fill(0);
        for (let r = 0; r < d; r++) {
          const ro = r * d;
          let s = 0;
          for (let c = 0; c < d; c++) s += Cm[ro + c] * v[c];
          y[r] = s;
        }
        // orthonormalize against previous PCs if present
        if (PC) {
          for (const pc of PC) if (pc) projSubtract(y, pc);
        }
        normalize(y);
        v.set(y);
      }
      // Rayleigh quotient for eigenvalue
      let num = 0, den = 0;
      for (let r = 0; r < d; r++) {
        let s = 0; const ro = r * d;
        for (let c = 0; c < d; c++) s += Cm[ro + c] * v[c];
        num += v[r] * s; den += v[r] * v[r];
      }
  const lambda = num / (den || 1);
      return { vec: v, lambda };
    }
    function normalize(x) {
      let nrm = 0; for (let i = 0; i < x.length; i++) nrm += x[i] * x[i];
      nrm = Math.sqrt(nrm) || 1; for (let i = 0; i < x.length; i++) x[i] /= nrm;
    }
    function projSubtract(x, pc) {
      // x -= (x·pc) pc
      let dot = 0; for (let i = 0; i < x.length; i++) dot += x[i] * pc[i];
      for (let i = 0; i < x.length; i++) x[i] -= dot * pc[i];
    }

    // Total variance is the trace of covariance matrix
    let trace = 0; for (let i = 0; i < d; i++) trace += C[i * d + i];
    TOTVAR = trace;
    PC = [];
    EIG = new Float64Array(3);
    for (let k = 0; k < 3; k++) {
      const { vec, lambda } = powerIter(C, d, 60);
      PC.push(vec);
      EIG[k] = lambda || 0;
    }

    // compute projections and min/max
    let minv = [Infinity, Infinity, Infinity];
    let maxv = [-Infinity, -Infinity, -Infinity];
    const tmp = new Float64Array(3);
    for (let i = 0; i < V.length; i++) {
      const v = V[i];
      tmp[0] = dotCentered(v, PC[0]);
      tmp[1] = dotCentered(v, PC[1]);
      tmp[2] = dotCentered(v, PC[2]);
      for (let j = 0; j < 3; j++) {
        if (tmp[j] < minv[j]) minv[j] = tmp[j];
        if (tmp[j] > maxv[j]) maxv[j] = tmp[j];
      }
    }
    SCALE = { min: minv, max: maxv };
    // precompute normalized [-1,1] projections per cid
    PROJ.clear();
    for (let i = 0; i < V.length; i++) {
      const cid = IDS[i];
      const p = [0, 0, 0];
      for (let j = 0; j < 3; j++) {
        const val = dotCentered(V[i], PC[j]);
        const a = minv[j], b = maxv[j];
        const t = (val - a) / (b - a || 1);
        p[j] = Math.min(1, Math.max(-1, t * 2 - 1));
      }
      PROJ.set(cid, p);
    }
    console.log('PCA projection ready');
  }

  function getWorker() {
    if (WORKER) return WORKER;
    // Vite will handle bundling worker when referenced via new URL(..., import.meta.url)
  // Add a tiny cache-buster to avoid stale worker code after updates
  const url = new URL('./vec_worker.js?v=10', import.meta.url);
  WORKER = new Worker(url, { type: 'module' });
    try {
      WORKER.addEventListener('message', (ev) => {
        const m = ev.data || {};
        if (m && m.type === 'hnsw:ctor') {
          try { emit('vec:hnsw:ctor', { which: m.which || '?' }); } catch {}
        } else if (m && m.type === 'debug') {
          try { emit('vec:hnsw:debug', { message: m.which || m.message || '' }); } catch {}
        }
      });
    } catch {}
    return WORKER;
  }

  function postWorker(type, payload, transfer = []) {
    return new Promise((resolve, reject) => {
      const w = getWorker();
      const ticket = { reject };
      PENDING.add(ticket);
      const onMsg = (ev) => {
        const m = ev.data || {};
        // Relay progress events for UI
        if (m.type === 'pca:phase') {
          try { emit('vec:pca:start', { phase: m.phase, i: m.i, n: m.n }); } catch {}
        } else if (m.type === 'hnsw:progress') {
          try { emit('vec:hnsw:build:start', { count: m.total, added: m.added }); } catch {}
        } else if (m.type === 'debug') {
          // Avoid console spam; UI listens to vec:hnsw:debug
          try { emit('vec:hnsw:debug', { message: m.which || m.message || '' }); } catch {}
        }
        if (m.type === 'error') {
          w.removeEventListener('message', onMsg);
          w.removeEventListener('error', onErr);
          w.removeEventListener('messageerror', onMsgErr);
          PENDING.delete(ticket);
          reject(new Error(m.message || 'worker error'));
        } else if (type === 'pca' && m.type === 'pca:done') {
          w.removeEventListener('message', onMsg);
          w.removeEventListener('error', onErr);
          w.removeEventListener('messageerror', onMsgErr);
          PENDING.delete(ticket);
          resolve(m);
        } else if (type === 'hnsw:build' && m.type === 'hnsw:built') {
          w.removeEventListener('message', onMsg);
          w.removeEventListener('error', onErr);
          w.removeEventListener('messageerror', onMsgErr);
          PENDING.delete(ticket);
          resolve(m.bytes);
        }
      };
      const onErr = (e) => {
        w.removeEventListener('message', onMsg);
        w.removeEventListener('error', onErr);
        w.removeEventListener('messageerror', onMsgErr);
        PENDING.delete(ticket);
        // Try to extract useful info from WorkerErrorEvent
        if (e instanceof Error) return reject(e);
        let msg = '';
        try {
          msg = e && e.message ? e.message : '';
          if (!msg && e && typeof e === 'object') {
            const fn = e.filename || e.fileName;
            const ln = e.lineno || e.lineNo;
            const cn = e.colno || e.columnNo;
            if (fn) msg = `Worker error at ${fn}${ln ? `:${ln}` : ''}${cn ? `:${cn}` : ''}`;
          }
        } catch {}
        reject(new Error(msg || 'worker error'));
      };
      const onMsgErr = (e) => {
        w.removeEventListener('message', onMsg);
        w.removeEventListener('error', onErr);
        w.removeEventListener('messageerror', onMsgErr);
        PENDING.delete(ticket);
        reject(new Error('worker messageerror (structured clone failed)'));
      };
      w.addEventListener('message', onMsg);
      w.addEventListener('error', onErr);
      w.addEventListener('messageerror', onMsgErr);
      w.postMessage({ type, ...payload }, transfer);
    });
  }

  async function runPCAInWorker() {
    const n = V.length; const d = V[0].length;
    const flat = new Float32Array(n * d);
    for (let i = 0; i < n; i++) flat.set(V[i], i * d);
    const ids = IDS.slice();
    const res = await postWorker('pca', { buf: flat.buffer, n, d, ids }, [flat.buffer]);
    // Fill PROJ map from res.proj order (same as IDS order)
    PROJ.clear();
    for (let i = 0; i < ids.length; i++) PROJ.set(ids[i], res.proj[i]);
  }

  function cancelBuild() {
    try {
      if (WORKER) {
        WORKER.terminate();
        WORKER = null;
        BUILDING = false;
        // Reject all pending worker calls to unblock awaits
        for (const t of PENDING) {
          try { t.reject(new Error('cancelled')); } catch {}
        }
        PENDING.clear();
        try {
          emit('vec:pca:error', { message: 'cancelled' });
          emit('vec:hnsw:error', { message: 'cancelled' });
        } catch {}
        return true;
      }
      return false;
    } catch (e) {
      console.warn('Cancel build failed:', e);
      return false;
    }
  }

  function dotCentered(v, pc) {
    let s = 0;
    for (let i = 0; i < v.length; i++) s += (v[i] - MEAN[i]) * pc[i];
    return s;
  }

  function projectCid(cid) {
    return PROJ.get(cid) || null;
  }

  function getProjections() {
    // Ensure projections exist; compute inline if needed
    try {
      if (!PROJ || PROJ.size === 0) {
        if (V && V.length && V[0]) computePCAAndProjections();
      }
    } catch {}
    const out = [];
    for (const [cid, p] of PROJ.entries()) {
      if (Array.isArray(p) && p.length >= 3) out.push({ cid, x: p[0], y: p[1], z: p[2] });
    }
    return out;
  }

  function getPcaInfo() {
    try {
      const dim = (V && V[0]) ? V[0].length : 0;
      const count = IDS ? IDS.length : 0;
      const eigen = (EIG && EIG.length) ? Array.from(EIG) : [];
      const totalVar = TOTVAR || 0;
      const explained = (totalVar > 0 && eigen.length) ? eigen.map(v => v / totalVar) : [];
      return { dim, count, eigenvalues: eigen, totalVariance: totalVar, explained };
    } catch { return { dim: 0, count: 0, eigenvalues: [], totalVariance: 0, explained: [] }; }
  }

  function setEf(index, v){
    try {
      if (typeof index.setEfSearch === 'function') return index.setEfSearch(v);
      if (typeof index.setEf === 'function') return index.setEf(v);
      if ('ef' in index) { try { index.ef = v; } catch {} }
    } catch {}
  }

  function initIndexCompat(index, dim, maxE, efC){
    const tries = [
      () => index.initIndex(maxE, 36, efC, 100),
      () => index.initIndex(dim, maxE, 36, efC),
      () => index.initIndex(dim, 36, efC),
      () => index.initIndex(maxE, 36, efC)
    ];
    for (const t of tries) { try { t(); return true; } catch {} }
    return false;
  }

  // ---------- IndexedDB cache helpers ----------
  function datasetSignature() {
    const d = V && V[0] ? V[0].length : 0;
    const n = IDS ? IDS.length : 0;
    const first = IDS && IDS.length ? IDS[0] : '';
    return `${VEC_URL || ''}|${n}|${d}|${first}`;
  }

  function idbOpen() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open('socionics_compass', 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains('hnswIndex')) {
          db.createObjectStore('hnswIndex');
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async function idbGet(key) {
    const db = await idbOpen();
    return new Promise((resolve, reject) => {
      const tx = db.transaction('hnswIndex', 'readonly');
      const store = tx.objectStore('hnswIndex');
      const req = store.get(key);
      req.onsuccess = () => resolve(req.result || null);
      req.onerror = () => reject(req.error);
    });
  }

  async function idbSet(key, value) {
    const db = await idbOpen();
    return new Promise((resolve, reject) => {
      const tx = db.transaction('hnswIndex', 'readwrite');
      const store = tx.objectStore('hnswIndex');
      const req = store.put(value, key);
      req.onsuccess = () => resolve(true);
      req.onerror = () => reject(req.error);
    });
  }

  async function loadHnswFromCache() {
    try {
      const sig = datasetSignature();
      const key = `hnsw:${sig}`;
      const entry = await idbGet(key);
      if (!entry) return false;
  const { loadHnswlib } = await import('./hnswlib_loader.js');
      HNSW = await loadHnswlib();
      const dim = V[0].length;
  const index = createHnswIndex(HNSW, dim, IDS.length || 0);
      if (!entry || !entry.data) return false;
  const bytes = entry.data instanceof Uint8Array ? entry.data : new Uint8Array(entry.data);
  if (!bytes || !bytes.length) return false;
  readIndexCompat(index, bytes, HNSW);
    try { emit('vec:hnsw:ctor', { which: (index && index._ctorWhich) || ((index && index.maxElements) ? '3-arg' : '2-arg') }); emit('vec:hnsw:debug', { message: 'loaded from cache' }); } catch {}
    try { setEf(index, EF_SEARCH); } catch {}
      INDEX_HNSW = index;
      CACHE_STATE = 'loaded';
      console.log('HNSW index loaded from cache');
      try { emit('vec:hnsw:cache:loaded', {}); } catch {}
      return true;
    } catch (e) {
      console.warn('HNSW cache load failed:', e);
      CACHE_STATE = 'error';
      return false;
    }
  }

  async function saveHnswToCache() {
  if (!INDEX_HNSW || !USE_CACHE) return false;
    try {
      const sig = datasetSignature();
      const key = `hnsw:${sig}`;
      // writeIndex may return Uint8Array
  const bytes = writeIndexCompat(INDEX_HNSW, HNSW);
      const payload = { sig, data: bytes, createdAt: Date.now() };
      await idbSet(key, payload);
      CACHE_STATE = 'saved';
      console.log('HNSW index saved to cache');
      return true;
    } catch (e) {
      console.warn('HNSW cache save error:', e);
      CACHE_STATE = 'error';
      return false;
    }
  }

  async function exportIndex() {
    if (!INDEX_HNSW) return null;
    try {
  const bytes = writeIndexCompat(INDEX_HNSW, HNSW);
      return bytes; // Uint8Array
    } catch (e) {
      console.warn('Export HNSW failed:', e);
      return null;
    }
  }

  async function importIndex(bytes) {
    if (!bytes || !V.length) throw new Error('No vectors or bytes');
    try {
      const arr = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const { loadHnswlib } = await import('./hnswlib_loader.js');
      HNSW = await loadHnswlib();
    const dim = V[0].length;
  const index = createHnswIndex(HNSW, dim, IDS.length || 0);
    readIndexCompat(index, arr, HNSW);
  try { emit('vec:hnsw:ctor', { which: (index && index._ctorWhich) || ((index && index.maxElements) ? '3-arg' : '2-arg') }); emit('vec:hnsw:debug', { message: 'imported index' }); } catch {}
  try { setEf(index, EF_SEARCH); } catch {}
      INDEX_HNSW = index;
      await saveHnswToCache();
      try { emit('vec:hnsw:build:end', {}); emit('vec:hnsw:cache:saved', {}); } catch {}
      return true;
    } catch (e) {
      console.warn('Import HNSW failed:', e);
      return false;
    }
  }

  function getCacheState() { return CACHE_STATE; }
  function setUseCache(v){ USE_CACHE = !!v; }
  function getUseCache(){ return USE_CACHE; }

  async function clearHnswCache() {
    try {
      const sig = datasetSignature();
      const key = `hnsw:${sig}`;
      const db = await idbOpen();
      await new Promise((resolve, reject) => {
        const tx = db.transaction('hnswIndex', 'readwrite');
        const store = tx.objectStore('hnswIndex');
        const req = store.delete(key);
        req.onsuccess = () => resolve(true);
        req.onerror = () => reject(req.error);
      });
      CACHE_STATE = 'none';
      console.log('HNSW cache cleared for current dataset');
      return true;
    } catch (e) {
      console.warn('Clear HNSW cache failed:', e);
      CACHE_STATE = 'error';
      return false;
    }
  }

  async function rebuildIndex() {
    try {
      INDEX_HNSW = null;
  await buildHnswIndexWorkerFirst();
      return true;
    } catch (e) {
      console.warn('Rebuild HNSW index failed:', e);
      return false;
    }
  }

  function size() { return IDS.length; }
  function isHnswReady() { return !!INDEX_HNSW; }
  function isLoaded() { return V.length > 0; }
  function getSource() { return VEC_URL || ''; }
  function isBuilding() { return !!BUILDING; }
  function setEfSearch(v){
    try {
      EF_SEARCH = Math.max(4, Math.min(1024, v|0));
      if (INDEX_HNSW) setEf(INDEX_HNSW, EF_SEARCH);
    } catch {}
  }
  function getEfSearch(){ return EF_SEARCH; }
  function setEfConstruction(v){ try{ EF_CONSTRUCTION = Math.max(8, Math.min(2048, v|0)); }catch{} }
  function getEfConstruction(){ return EF_CONSTRUCTION; }

  // ---- Internal helpers ----
  function createHnswIndex(HNSWLib, dim, maxElements){
  const maxE = Math.max(1, maxElements|0);
  const space = 'cosine';
  const d = Math.max(1, dim|0);
  const ctors = [];
  const add = (f) => { if (typeof f === 'function') ctors.push(f); };
  add(HNSWLib && HNSWLib.HierarchicalNSW);
  add(HNSWLib && HNSWLib.default && HNSWLib.default.HierarchicalNSW);
  add(HNSWLib && HNSWLib.hnswlib && HNSWLib.hnswlib.HierarchicalNSW);
  // Probe other function exports with matching prototype methods
  try {
    for (const v of Object.values(HNSWLib || {})) {
      if (typeof v === 'function') {
        const proto = v && v.prototype ? Object.getOwnPropertyNames(v.prototype) : [];
        if (proto.includes('addPoints') || proto.includes('addItems')) add(v);
      }
    }
  } catch {}
  // De-duplicate
  const uniq = [];
  const seen = new Set();
  for (const f of ctors) { const id = String(f && f.name || 'ctor'); if (!seen.has(id)) { uniq.push(f); seen.add(id); } }
  // Debug: log available keys once
  try {
    const keys = Object.keys(HNSWLib || {}).slice(0, 12).join(',');
    console.debug('HNSW keys:', keys);
    try { emit('vec:hnsw:debug', { message: `keys: ${keys}` }); } catch {}
  } catch {}
  const errs = [];
  const perms3 = [
    ['space','dim','maxE','space,dim,maxE'],
    ['dim','space','maxE','dim,space,maxE'],
    ['dim','maxE','space','dim,maxE,space'],
    ['maxE','dim','space','maxE,dim,space'],
    ['space','maxE','dim','space,maxE,dim'],
    ['maxE','space','dim','maxE,space,dim']
  ];
  for (const Ctor of uniq) {
    try { const idx = new Ctor(space, d, ''); idx._ctorWhich = '3-arg space,dim,\'\' (new)'; return idx; } catch (e) { errs.push(e && e.message ? e.message : String(e)); }
    try { const idx = new Ctor(space, d, '/hnsw_autosave.bin'); idx._ctorWhich = '3-arg space,dim,/hnsw_autosave.bin (new)'; return idx; } catch (e) { errs.push(e && e.message ? e.message : String(e)); }
    for (const [a,b,c,tag] of perms3) {
      try {
        const args = { space, dim: d, maxE };
        const idx = new Ctor(args[a], args[b], String(args[c]));
        idx._ctorWhich = `3-arg ${tag} (new)`;
        return idx;
      } catch (e) { errs.push(e && e.message ? e.message : String(e)); }
    }
  }
  // Try 2-arg permutations, rely on initIndex later
  for (const Ctor of uniq) {
    try { const idx = new Ctor(space, d); idx._ctorWhich = '2-arg space,dim (new)'; return idx; } catch (e) { errs.push(e && e.message ? e.message : String(e)); }
    try { const idx = new Ctor(d, space); idx._ctorWhich = '2-arg dim,space (new)'; return idx; } catch (e) { errs.push(e && e.message ? e.message : String(e)); }
  }
  // Last resort: enum metric
  try { const METRIC = (HNSWLib && HNSWLib.MetricSpace && HNSWLib.MetricSpace.COSINE) || space; const Ctor = uniq[0]; if (Ctor) { const idx = new Ctor(METRIC, d, maxE); idx._ctorWhich = '3-arg METRIC const (new)'; return idx; } } catch (e) { errs.push(e && e.message ? e.message : String(e)); }
  try { emit('vec:hnsw:debug', { message: `ctor failed: ${errs.join(' | ')}` }); } catch {}
  throw new Error('HierarchicalNSW constructor variants exhausted: ' + errs.join(' | '));
  }

  // --- Compatibility helpers for path-based read/write ---
  function ensureFS(api) {
    const FS = api && (api.FS || (api.Module && api.Module.FS));
    if (!FS || typeof FS.readFile !== 'function' || typeof FS.writeFile !== 'function') {
  throw new Error('FS was not exported. add it to EXPORTED_RUNTIME_METHODS (see the FAQ)');
    }
    return FS;
  }

  function readIndexCompat(index, bytes, api) {
    // Try byte-array API first
    try {
      index.readIndex(bytes);
      return true;
    } catch (e) {
      const msg = e && (e.message || String(e));
      if (!(msg && /std::string|string|argument/i.test(msg))) throw e;
    }
    // Path-based: write bytes to FS then read by path
    const FS = ensureFS(api);
    const path = `/hnsw_${Date.now()}_${Math.random().toString(36).slice(2)}.bin`;
    FS.writeFile(path, bytes);
    // Try common variants
    let ok = false; const errs = [];
    const tries = [
      () => index.readIndex(path),
      () => index.readIndex(path, false),
      () => index.readIndex(path, true),
      () => index.readIndex(path, (IDS && IDS.length) || 0),
      () => index.readIndex(path, (IDS && IDS.length) || 0, false),
      () => index.readIndex(path, (IDS && IDS.length) || 0, true)
    ];
    for (const t of tries) {
      try { t(); ok = true; break; } catch (e) { errs.push(e && e.message ? e.message : String(e)); }
    }
    if (!ok) {
      try { FS.unlink && FS.unlink(path); } catch {}
      throw new Error('HNSW readIndex failed: ' + errs.join(' | '));
    }
    try { FS.unlink && FS.unlink(path); } catch {}
    return true;
  }

  function writeIndexCompat(index, api) {
    try {
      const out = index.writeIndex();
      if (out && (out instanceof Uint8Array || ArrayBuffer.isView(out))) return out;
    } catch (e) {
      const msg = e && (e.message || String(e));
      if (!(msg && /std::string|string|argument/i.test(msg))) throw e;
    }
    const FS = ensureFS(api);
    const path = `/hnsw_${Date.now()}_${Math.random().toString(36).slice(2)}.bin`;
    // Some builds require additional params
    let ok = false; const errs = [];
    const tries = [
      () => index.writeIndex(path),
      () => index.writeIndex(path, false),
      () => index.writeIndex(path, true)
    ];
    for (const t of tries) {
      try { t(); ok = true; break; } catch (e) { errs.push(e && e.message ? e.message : String(e)); }
    }
    if (!ok) throw new Error('HNSW writeIndex failed: ' + errs.join(' | '));
    const bytes = FS.readFile(path);
    try { FS.unlink && FS.unlink(path); } catch {}
    return bytes;
  }

  function getRows() {
    // Return a plain array of { cid, vector } with normalized vectors
    const out = new Array(IDS.length);
    for (let i = 0; i < IDS.length; i++) out[i] = { cid: IDS[i], vector: Array.from(V[i]) };
    return out;
  }
  function getDim() { return (V && V[0]) ? V[0].length : 0; }

  return { load, loadFromParquet, loadFromRows, similarByCid, getVector, projectCid, size, isHnswReady, isLoaded, getCacheState, clearHnswCache, rebuildIndex, getSource, exportIndex, importIndex, cancelBuild, isBuilding, setEfSearch, getEfSearch, setEfConstruction, getEfConstruction, setUseCache, getUseCache, getRows, getDim, getProjections, getPcaInfo };
})();

window.VEC = VEC;
