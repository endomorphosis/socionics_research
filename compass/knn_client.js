// Lightweight client-side KNN over exported JSON dataset

const KNN = (() => {
  // simple hash-based embedding for dev (64-dim) to avoid heavy models in-browser
  function embed(text) {
    const buckets = new Float32Array(64);
    if (!text) return buckets;
    const toks = String(text).toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
    for (const t of toks) {
      let h = 2166136261 >>> 0; // FNV-1a
      for (let i = 0; i < t.length; i++) {
        h ^= t.charCodeAt(i);
        h = Math.imul(h, 16777619) >>> 0;
      }
      buckets[h % buckets.length] += 1;
    }
    // L2 normalize
    let norm = 0;
    for (let i = 0; i < buckets.length; i++) norm += buckets[i] * buckets[i];
    norm = Math.sqrt(norm) || 1;
    for (let i = 0; i < buckets.length; i++) buckets[i] /= norm;
    return buckets;
  }

  function dot(a, b) {
    let s = 0;
    for (let i = 0; i < a.length; i++) s += a[i] * b[i];
    return s;
  }

  let DATA = [];
  let VECTORS = [];
  let CIDX = new Map(); // cid -> record
  let SRC = '';

  async function load(url = '/pdb_profiles.json') {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`Failed to load dataset: ${resp.status}`);
    const rows = await resp.json();
  SRC = url;
    return loadFromRows(rows);
  }

  function loadFromRows(rows) {
    DATA = rows;
    VECTORS = DATA.map(r => embed(r.text));
    CIDX = new Map();
    for (const r of DATA) if (r.cid) CIDX.set(r.cid, r);
    console.log(`KNN loaded ${DATA.length} records`);
    return DATA.length;
  }

  async function loadFromParquet(url = '/dataset/pdb_profiles_normalized.parquet') {
    if (!window.DuckVec || !window.DuckVec.loadProfiles) throw new Error('DuckDB-Wasm loader unavailable');
    const rows = await window.DuckVec.loadProfiles(url);
  SRC = url;
    return loadFromRows(rows);
  }

  function search(query, k = 10) {
    const qv = embed(query);
    const scores = new Array(DATA.length);
    for (let i = 0; i < DATA.length; i++) scores[i] = [dot(qv, VECTORS[i]), i];
    scores.sort((a, b) => b[0] - a[0]);
    const out = [];
    for (let j = 0; j < Math.min(k, scores.length); j++) {
      const [score, idx] = scores[j];
      const r = DATA[idx];
      out.push({ ...r, _score: score });
    }
    return out;
  }

  function getByCid(cid) {
    return CIDX.get(cid) || null;
  }
  function size() { return DATA.length; }
  function getSource() { return SRC || ''; }

  return { load, loadFromRows, loadFromParquet, search, getByCid, size, getSource };
})();

window.KNN = KNN;
