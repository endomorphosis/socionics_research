// Lightweight cosine K-Means with KMeans++ seeding

const KMEANS = (() => {
  function l2normalize(v) {
    let s = 0; for (let i = 0; i < v.length; i++) s += v[i] * v[i];
    s = Math.sqrt(s) || 1;
    const out = new Float32Array(v.length);
    for (let i = 0; i < v.length; i++) out[i] = v[i] / s;
    return out;
  }
  function dot(a,b){ let s=0; for(let i=0;i<a.length;i++) s+=a[i]*b[i]; return s; }
  function cosineDist(a,b){ return 1 - dot(a,b); }
  function randInt(n){ return (Math.random()*n)|0; }

  function kmeans(vectors, k, opts={}){
    const maxIters = opts.maxIters || 50;
    const tol = opts.tol || 1e-4;
    const n = vectors.length;
    if (!n) return { labels: [], centroids: [], inertia: 0 };
    const d = vectors[0].length;
    // Normalize inputs
    const X = new Array(n);
    for (let i=0;i<n;i++) X[i] = l2normalize(vectors[i]);
    // Guard k
    k = Math.max(1, Math.min(k|0, n));
    if (k === 1) {
      // Mean centroid
      const c = new Float32Array(d);
      for (let i=0;i<n;i++){ const v=X[i]; for(let j=0;j<d;j++) c[j]+=v[j]; }
      for (let j=0;j<d;j++) c[j] /= n;
      const cN = l2normalize(c);
      const labels = new Array(n).fill(0);
      let inertia=0; for(let i=0;i<n;i++) inertia+=cosineDist(X[i],cN);
      return { labels, centroids:[cN], inertia };
    }
    // KMeans++ seeding
    const centroids = [];
    const first = randInt(n); centroids.push(X[first]);
    const distSq = new Float64Array(n);
    for (let c=1;c<k;c++) {
      let sum=0;
      for (let i=0;i<n;i++) { const d0 = cosineDist(X[i], centroids[centroids.length-1]); distSq[i] = Math.max(0, d0*d0); sum += distSq[i]; }
      if (sum === 0) { centroids.push(X[randInt(n)]); continue; }
      let r = Math.random() * sum; let idx=0;
      for (; idx<n; idx++){ r -= distSq[idx]; if (r<=0) break; }
      centroids.push(X[Math.min(idx,n-1)]);
    }
    // Lloyd iterations
    const labels = new Int32Array(n);
    let inertia = 0;
    for (let iter=0; iter<maxIters; iter++) {
      // Assign
      inertia = 0;
      for (let i=0;i<n;i++) {
        let best=Infinity, bestK=0;
        for (let c=0;c<k;c++) { const dist = cosineDist(X[i], centroids[c]); if (dist<best) { best=dist; bestK=c; } }
        labels[i]=bestK; inertia += best;
      }
      // Update
      const sums = new Array(k); const counts = new Int32Array(k);
      for (let c=0;c<k;c++) sums[c] = new Float64Array(d);
      for (let i=0;i<n;i++){
        const v = X[i]; const c = labels[i]; counts[c]++;
        const s = sums[c]; for (let j=0;j<d;j++) s[j]+=v[j];
      }
      let maxShift = 0;
      for (let c=0;c<k;c++){
        if (counts[c]===0) { centroids[c] = X[randInt(n)]; continue; }
        const avg = new Float32Array(d);
        for (let j=0;j<d;j++) avg[j] = sums[c][j]/counts[c];
        const newC = l2normalize(avg);
        // shift = cosineDist(old,new)
        const shift = cosineDist(centroids[c], newC);
        if (shift>maxShift) maxShift = shift;
        centroids[c] = newC;
      }
      if (maxShift < tol) break;
    }
    return { labels: Array.from(labels), centroids, inertia };
  }

  // Utility: suggested k
  function suggestK(n){ return Math.max(3, Math.min(12, Math.round(Math.sqrt(Math.max(1,n))))); }

  return { kmeans, suggestK };
})();

window.KMEANS = KMEANS;
