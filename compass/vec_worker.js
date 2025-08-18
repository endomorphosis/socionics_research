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
  // Use 2-arg ctor: (space, dim). Max elements is set via initIndex.
  const index = new HNSW.HierarchicalNSW('cosine', d);
  index.initIndex(n, 36, efC, 100);
  index.setEfSearch(64);
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
    index.addPoints(rows, Array.from(l), false);
    self.postMessage({ type: 'hnsw:progress', added: end, total: n });
  }
  const bytes = index.writeIndex(); // Uint8Array
  return bytes;
}
