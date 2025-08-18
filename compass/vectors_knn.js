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
    V = new Array(rows.length);
    IDS = new Array(rows.length);
    INDEX = new Map();
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const vec = new Float32Array(r.vector);
      V[i] = vec;
      IDS[i] = r.cid;
      INDEX.set(r.cid, i);
    }
    console.log(`Vectors loaded: ${rows.length}`);
    if (rows.length && V[0]) {
      // Launch PCA in worker to keep UI responsive
      try {
        emit('vec:pca:start', { count: rows.length, dim: V[0].length });
        BUILDING = true;
        await runPCAInWorker();
        emit('vec:pca:end', {});
        BUILDING = false;
      } catch (e) {
        console.warn('PCA (worker) failed, fallback inline:', e);
        try {
          BUILDING = true;
          computePCAAndProjections();
          emit('vec:pca:end', {});
        } catch (e2) {
          console.warn('PCA projection failed:', e2);
          emit('vec:pca:error', { message: String(e2) });
        }
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
    return rows.length;
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
        res = INDEX_HNSW.searchKnn(vec, k);
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
  // cosine metric; pass max elements as 3rd arg for ctor
  const index = new HNSW.HierarchicalNSW('cosine', dim);
    // parameters: max elements, M, efConstruction, random seed
  index.initIndex(V.length, 36, EF_CONSTRUCTION, 100);
  index.setEfSearch(EF_SEARCH);
  // add items using array-of-rows to align with documented API (addPoints)
  const n = V.length, d = dim;
  const rows = V.map((row) => row);
  const labels = new Array(n);
  for (let i = 0; i < n; i++) labels[i] = i;
  index.addPoints(rows, labels, false);
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
  const index = new HNSW.HierarchicalNSW('cosine', d);
      index.readIndex(new Uint8Array(bytes));
  index.setEfSearch(EF_SEARCH);
      INDEX_HNSW = index;
      try { emit('vec:hnsw:build:end', {}); } catch {}
      try { await saveHnswToCache(); emit('vec:hnsw:cache:saved', {}); } catch {}
  BUILDING = false;
      return;
    } catch (e) {
      console.warn('HNSW worker build failed, building on main thread:', e);
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

    PC = [];
    for (let k = 0; k < 3; k++) {
      const { vec } = powerIter(C, d, 60);
      PC.push(vec);
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
  const url = new URL('./vec_worker.js?v=2', import.meta.url);
  WORKER = new Worker(url, { type: 'module' });
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
  const index = new HNSW.HierarchicalNSW('cosine', dim);
      if (!entry || !entry.data) return false;
      const bytes = entry.data instanceof Uint8Array ? entry.data : new Uint8Array(entry.data);
      if (!bytes || !bytes.length) return false;
      index.readIndex(bytes);
    index.setEfSearch(EF_SEARCH);
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
      const bytes = INDEX_HNSW.writeIndex();
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
      const bytes = INDEX_HNSW.writeIndex();
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
  const index = new HNSW.HierarchicalNSW('cosine', dim);
      index.readIndex(arr);
  index.setEfSearch(EF_SEARCH);
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
  function setEfSearch(v){ try{ EF_SEARCH = Math.max(4, Math.min(1024, v|0)); if (INDEX_HNSW) INDEX_HNSW.setEfSearch(EF_SEARCH); }catch{} }
  function getEfSearch(){ return EF_SEARCH; }
  function setEfConstruction(v){ try{ EF_CONSTRUCTION = Math.max(8, Math.min(2048, v|0)); }catch{} }
  function getEfConstruction(){ return EF_CONSTRUCTION; }

  return { load, loadFromParquet, loadFromRows, similarByCid, getVector, projectCid, size, isHnswReady, isLoaded, getCacheState, clearHnswCache, rebuildIndex, getSource, exportIndex, importIndex, cancelBuild, isBuilding, setEfSearch, getEfSearch, setEfConstruction, getEfConstruction, setUseCache, getUseCache };
})();

window.VEC = VEC;
