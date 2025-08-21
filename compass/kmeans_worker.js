// Web Worker: cosine K-Means with KMeans++ seeding for off-main-thread clustering

self.onmessage = (ev) => {
  const msg = ev.data || {};
  if (msg.type !== 'kmeans') return;
  try {
    const { buf, n, d, k, maxIters = 60, tol = 1e-4 } = msg;
    const X = new Float32Array(buf);
    // Validate buffer size
    if (!Number.isFinite(n) || !Number.isFinite(d) || X.length !== (n|0)*(d|0)) {
      throw new Error(`Bad kmeans input: n=${n}, d=${d}, bufLen=${X.length}`);
    }
  try { self.postMessage({ type: 'kmeans:start', n, d, k, maxIters }); } catch {}
  const { labels, centroids, iters } = kmeansFromBuffer(X, n|0, d|0, k|0, maxIters|0, +tol, (iter) => { try { self.postMessage({ type: 'kmeans:progress', iter, maxIters }); } catch {} });
  const lab = new Int32Array(labels);
  const cen = new Float32Array(centroids.length * d);
  for (let i = 0; i < centroids.length; i++) cen.set(centroids[i], i * d);
  // Use transfer list (second arg) to transfer buffers for performance and compatibility
  self.postMessage({ type: 'kmeans:done', labels: lab, centroids: cen, k: centroids.length, d, iters: iters|0 }, [lab.buffer, cen.buffer]);
  } catch (e) {
    self.postMessage({ type: 'error', message: String(e && e.message || e) });
  }
};

function l2normalizeInto(src, dst, off, d) {
  let s = 0; for (let j = 0; j < d; j++) s += src[off + j] * src[off + j];
  s = Math.sqrt(s) || 1;
  for (let j = 0; j < d; j++) dst[j] = src[off + j] / s;
}
function dot(a,b){ let s=0; for(let i=0;i<a.length;i++) s+=a[i]*b[i]; return s; }
function cosineDist(a,b){ return 1 - dot(a,b); }
function randInt(n){ return (Math.random()*n)|0; }

function kmeansFromBuffer(X, n, d, k, maxIters, tol, onIter) {
  if (!n || !d) return { labels: [], centroids: [] };
  // Normalize inputs
  const N = new Float32Array(n * d);
  for (let i = 0; i < n; i++) l2normalizeInto(X, N, i * d, d);
  // Guard k
  k = Math.max(1, Math.min(k|0, n));
  // KMeans++ seeding
  const centroids = [];
  const first = randInt(n);
  centroids.push(N.slice(first * d, first * d + d));
  // Track nearest squared Euclidean distance on unit sphere: ||x - c||^2 = 2*(1 - dot(x,c))
  const curD2 = new Float64Array(n);
  for (let i = 0; i < n; i++) {
    const off = i * d; const c0 = centroids[0];
    let s = 0; for (let j = 0; j < d; j++) s += N[off + j] * c0[j];
    curD2[i] = Math.max(0, 2 * (1 - s));
  }
  for (let c = 1; c < k; c++) {
    let sum = 0;
    for (let i = 0; i < n; i++) sum += curD2[i];
    if (sum <= 0 || !isFinite(sum)) { const r = randInt(n); centroids.push(N.slice(r * d, r * d + d)); continue; }
    // Sample next centroid proportionally to curD2
    let r = Math.random() * sum; let idx = 0;
    for (; idx < n; idx++) { r -= curD2[idx]; if (r <= 0) break; }
    const pick = Math.min(idx, n - 1);
    const next = N.slice(pick * d, pick * d + d);
    centroids.push(next);
    // Update curD2 with new centroid (take min distance squared to any centroid)
    for (let i = 0; i < n; i++) {
      const off = i * d; let s = 0; for (let j = 0; j < d; j++) s += N[off + j] * next[j];
      const d2 = Math.max(0, 2 * (1 - s));
      if (d2 < curD2[i]) curD2[i] = d2;
    }
  }
  // Lloyd iterations
  const labels = new Int32Array(n);
  const sums = new Array(k); for (let c = 0; c < k; c++) sums[c] = new Float64Array(d);
  const counts = new Int32Array(k);
  let iters = 0;
  for (let iter = 0; iter < maxIters; iter++) {
    // Assign
    for (let i = 0; i < n; i++) {
      let best = Infinity, bestK = 0; const off = i * d;
      for (let c = 0; c < k; c++) {
        const cd = 1 - dotRow(centroids[c], N, off, d);
        if (cd < best) { best = cd; bestK = c; }
      }
      labels[i] = bestK;
    }
    // Update
    for (let c = 0; c < k; c++) { sums[c].fill(0); counts[c] = 0; }
    for (let i = 0; i < n; i++) {
      const off = i * d; const c = labels[i]; counts[c]++;
      const s = sums[c]; for (let j = 0; j < d; j++) s[j] += N[off + j];
    }
    let maxShift = 0;
    for (let c = 0; c < k; c++) {
      if (counts[c] === 0) { const r = randInt(n); centroids[c] = N.slice(r * d, r * d + d); continue; }
      const avg = new Float32Array(d);
      for (let j = 0; j < d; j++) avg[j] = sums[c][j] / counts[c];
      const newC = l2normalize(avg);
      const shift = cosineDist(centroids[c], newC);
      if (shift > maxShift) maxShift = shift;
      centroids[c] = newC;
    }
    iters = iter + 1;
    if (typeof onIter === 'function') { try { onIter(iters); } catch {} }
    if (maxShift < tol) break;
  }
  return { labels, centroids, iters };
}

function dotRow(a, N, off, d){ let s=0; for(let j=0;j<d;j++) s+=a[j]*N[off+j]; return s; }
function l2normalize(v){
  let s=0; for(let i=0;i<v.length;i++) s+=v[i]*v[i]; s=Math.sqrt(s)||1;
  const out=new Float32Array(v.length); for(let i=0;i<v.length;i++) out[i]=v[i]/s; return out;
}
