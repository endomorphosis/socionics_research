// Web Worker: performs heavy PCA and HNSW index building off the main thread
import { loadHnswlib } from './hnswlib_loader.js';
// Receives messages with { type: 'pca' | 'hnsw:build', payload }
// PCA input: { buf: ArrayBuffer (Float32), n: number, d: number, ids: string[] }
// PCA output: { type: 'pca:done', proj: Array<[number,number,number]>, ids: string[] }
// HNSW input: { buf, n, d }
// HNSW output: { type: 'hnsw:built', bytes: Uint8Array }

// Note: We keep PCA implementation self-contained here.

self.onmessage = async (ev) => {
  const msg = ev.data || {};
  try {
    if (msg.type === 'pca') {
      const { buf, n, d, ids } = msg;
      const V = new Float32Array(buf);
      const { proj } = computePCAProjections(V, n, d);
      // Send back projections aligned with ids index order
      self.postMessage({ type: 'pca:done', proj, ids }, { transfer: [] });
    } else if (msg.type === 'hnsw:build') {
      const { buf, n, d, efC } = msg;
      const V = new Float32Array(buf);
      const bytes = await buildHnswBytes(V, n, d, efC);
      self.postMessage({ type: 'hnsw:built', bytes }, { transfer: [bytes.buffer] });
    }
  } catch (e) {
    self.postMessage({ type: 'error', message: String(e && e.message || e) });
  }
};

function computePCAProjections(V, n, d) {
  // mean
  self.postMessage({ type: 'pca:phase', phase: 'mean' });
  const MEAN = new Float64Array(d);
  for (let i = 0; i < n; i++) {
    let off = i * d;
    for (let j = 0; j < d; j++) MEAN[j] += V[off + j];
  }
  for (let j = 0; j < d; j++) MEAN[j] /= Math.max(1, n);

  // covariance matrix C (d x d)
  self.postMessage({ type: 'pca:phase', phase: 'cov' });
  const C = new Float64Array(d * d);
  for (let i = 0; i < n; i++) {
    const off = i * d;
    for (let a = 0; a < d; a++) {
      const va = V[off + a] - MEAN[a];
      const row = a * d;
      for (let b = a; b < d; b++) {
        const vb = V[off + b] - MEAN[b];
        C[row + b] += va * vb;
      }
    }
    if ((i & 1023) === 0) self.postMessage({ type: 'pca:phase', phase: 'cov', i, n });
  }
  const inv = 1 / Math.max(1, n - 1);
  for (let a = 0; a < d; a++) {
    for (let b = a; b < d; b++) {
      const val = C[a * d + b] * inv;
      C[a * d + b] = val;
      if (a !== b) C[b * d + a] = val;
    }
  }

  // power iteration for top-3 PCs
  self.postMessage({ type: 'pca:phase', phase: 'eigen' });
  let PC = [];
  for (let k = 0; k < 3; k++) {
    const { vec } = powerIter(C, d, 60, PC);
    PC.push(vec);
  }

  // projections and scaling to [-1,1]
  self.postMessage({ type: 'pca:phase', phase: 'project' });
  const mins = [Infinity, Infinity, Infinity];
  const maxs = [-Infinity, -Infinity, -Infinity];
  const dots = new Float64Array(3);
  const dotCentered = (off, pc) => {
    let s = 0;
    for (let j = 0; j < d; j++) s += (V[off + j] - MEAN[j]) * pc[j];
    return s;
  };
  for (let i = 0; i < n; i++) {
    const off = i * d;
    dots[0] = dotCentered(off, PC[0]);
    dots[1] = dotCentered(off, PC[1]);
    dots[2] = dotCentered(off, PC[2]);
    for (let j = 0; j < 3; j++) {
      if (dots[j] < mins[j]) mins[j] = dots[j];
      if (dots[j] > maxs[j]) maxs[j] = dots[j];
    }
    if ((i & 2047) === 0) self.postMessage({ type: 'pca:phase', phase: 'project', i, n });
  }
  const proj = new Array(n);
  for (let i = 0; i < n; i++) {
    const off = i * d;
    const p = [0, 0, 0];
    for (let j = 0; j < 3; j++) {
      const val = dotCentered(off, PC[j]);
      const a = mins[j], b = maxs[j];
      const t = (val - a) / (b - a || 1);
      p[j] = Math.min(1, Math.max(-1, t * 2 - 1));
    }
    proj[i] = p;
    if ((i & 2047) === 0) self.postMessage({ type: 'pca:phase', phase: 'scale', i, n });
  }
  return { proj };
}

function powerIter(C, d, iters, prevPCs) {
  const v = new Float64Array(d);
  for (let i = 0; i < d; i++) v[i] = Math.random() - 0.5;
  normalize(v);
  const y = new Float64Array(d);
  for (let t = 0; t < iters; t++) {
    y.fill(0);
    for (let r = 0; r < d; r++) {
      let s = 0; const ro = r * d;
      for (let c = 0; c < d; c++) s += C[ro + c] * v[c];
      y[r] = s;
    }
    if (prevPCs && prevPCs.length) for (const pc of prevPCs) projSubtract(y, pc);
    normalize(y);
    v.set(y);
  }
  return { vec: v };
}

function normalize(x) {
  let nrm = 0; for (let i = 0; i < x.length; i++) nrm += x[i] * x[i];
  nrm = Math.sqrt(nrm) || 1; for (let i = 0; i < x.length; i++) x[i] /= nrm;
}

function projSubtract(x, pc) {
  let dot = 0; for (let i = 0; i < x.length; i++) dot += x[i] * pc[i];
  for (let i = 0; i < x.length; i++) x[i] -= dot * pc[i];
}

async function buildHnswBytes(V, n, d, efC = 200) {
  // Use statically imported loader so Vite rewrites the path for worker builds
  const HNSW = await loadHnswlib();
  // Try multiple ctor permutations to handle binding variations
  let index; let which = '';
  const maxE = Math.max(1, n|0);
  try { const keys = Object.keys(HNSW || {}).slice(0, 12).join(','); self.postMessage({ type: 'debug', which: `HNSW keys: ${keys}` }); } catch {}
  const ctors = [HNSW.HierarchicalNSW, HNSW.default && HNSW.default.HierarchicalNSW, HNSW.hnswlib && HNSW.hnswlib.HierarchicalNSW].filter(Boolean);
  const errs = [];
  const perms3 = [
    ['cosine', d, maxE, 'space,dim,maxE (new)'],
    [d, 'cosine', maxE, 'dim,space,maxE (new)'],
    [d, maxE, 'cosine', 'dim,maxE,space (new)'],
    [maxE, d, 'cosine', 'maxE,dim,space (new)'],
    ['cosine', maxE, d, 'space,maxE,dim (new)'],
    [maxE, 'cosine', d, 'maxE,space,dim (new)']
  ];
  for (const Ctor of ctors) {
    try { index = new Ctor('cosine', d, ''); which = `3-arg space,dim,'' (new)`; break; } catch (e) { errs.push(e && e.message ? e.message : String(e)); }
    try { index = new Ctor('cosine', d, '/hnsw_autosave.bin'); which = `3-arg space,dim,/hnsw_autosave.bin (new)`; break; } catch (e) { errs.push(e && e.message ? e.message : String(e)); }
    for (const [a,b,c,tag] of perms3) {
      try { const idx = new Ctor(a,b, String(c)); index = idx; which = `3-arg ${tag}`; break; } catch (e) { errs.push(e && e.message ? e.message : String(e)); }
    }
    if (index) break;
  }
  if (!index) {
    for (const Ctor of ctors) { try { const idx = new Ctor('cosine', d); index = idx; which = '2-arg space,dim (new)'; break; } catch (e) { errs.push(e && e.message ? e.message : String(e)); } }
  }
  if (!index) throw new Error('HNSW ctor failed: ' + errs.join(' | '));
  try { self.postMessage({ type: 'hnsw:ctor', which }); } catch {}
  try { if (typeof index.initIndex === 'function') index.initIndex(n, 36, efC, 100); } catch {}
  try { setEf(index, 64); } catch {}
  // Add in chunks using array-of-rows views to match API addPoints(items[], labels[], replaceDeleted)
  const CHUNK = 1024;
  for (let start = 0; start < n; start += CHUNK) {
    const end = Math.min(n, start + CHUNK);
    const count = end - start;
    const rows = new Array(count);
    const l = new Int32Array(count);
    for (let i = 0; i < count; i++) {
      const srcOff = (start + i) * d;
      rows[i] = new Float32Array(V.buffer, V.byteOffset + srcOff * 4, d);
      l[i] = start + i;
    }
    if (typeof index.addPoints === 'function') {
      index.addPoints(rows, Array.from(l), false);
    } else if (typeof index.addItems === 'function') {
      try { index.addItems(rows, false, Array.from(l)); }
      catch (e1) { try { index.addItems(rows, false); } catch (e2) { index.addItems(rows); } }
    } else {
      throw new Error('HNSW index missing addPoints/addItems');
    }
    self.postMessage({ type: 'hnsw:progress', added: end, total: n });
  }
  // Compat: some builds expect a filename (std::string) instead of returning bytes
  let bytes;
  try {
    bytes = writeIndexCompat(index, HNSW);
  } catch (e) {
    // Surface a concise error so main thread fallback path triggers cleanly
    throw new Error((e && e.message) ? e.message : 'writeIndex failed');
  }
  return bytes;
}

// --- Compatibility helpers for path-based read/write ---
function ensureFS(api) {
  const FS = api && (api.FS || (api.Module && api.Module.FS));
  if (!FS || typeof FS.readFile !== 'function' || typeof FS.writeFile !== 'function') {
  throw new Error('FS was not exported. add it to EXPORTED_RUNTIME_METHODS (see the FAQ)');
  }
  return FS;
}

function writeIndexCompat(index, api) {
  // Try no-arg (returns Uint8Array)
  try {
    const out = index.writeIndex();
    if (out && (out instanceof Uint8Array || ArrayBuffer.isView(out))) return out;
  } catch (e) {
    const msg = e && (e.message || String(e));
    // fall through to path-based if std::string required
    if (!(msg && /std::string|string|argument/i.test(msg))) throw e;
  }
  // Path-based: write to a temp file, then read via FS
  const FS = ensureFS(api);
  const path = `/hnsw_${Date.now()}_${Math.random().toString(36).slice(2)}.bin`;
  index.writeIndex(path);
  const bytes = FS.readFile(path);
  try { FS.unlink && FS.unlink(path); } catch {}
  return bytes;
}

// (Currently we only need writeIndex in worker; keep a read compat helper in case of future use)
function readIndexCompat(index, bytes, api) {
  try { index.readIndex(bytes); return true; } catch (e) {
    const msg = e && (e.message || String(e));
    if (!(msg && /std::string|string|argument/i.test(msg))) throw e;
  }
  const FS = ensureFS(api);
  const path = `/hnsw_${Date.now()}_${Math.random().toString(36).slice(2)}.bin`;
  FS.writeFile(path, bytes);
  const tries = [
    () => index.readIndex(path),
    () => index.readIndex(path, false),
    () => index.readIndex(path, true)
  ];
  let ok = false; const errs = [];
  for (const t of tries) { try { t(); ok = true; break; } catch (e) { errs.push(e && e.message ? e.message : String(e)); } }
  try { FS.unlink && FS.unlink(path); } catch {}
  if (!ok) throw new Error('HNSW readIndex failed: ' + errs.join(' | '));
  return true;
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
