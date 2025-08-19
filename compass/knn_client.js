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
    // Normalize and enrich rows with a canonical displayName
    DATA = rows.map(normalizeRecord);
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
  out.push({ ...r, name: r.displayName || r.name || r.cid || '', _score: score });
    }
    return out;
  }

  function getByCid(cid) {
    return CIDX.get(cid) || null;
  }
  function size() { return DATA.length; }
  function getSource() { return SRC || ''; }

  // --------- Helpers ---------
  function normalizeRecord(r0) {
    const r = { ...r0 };
    const cid = r.cid ? String(r.cid) : undefined;
    const maybeNameFields = [
      r.display_name, r.displayName, r.name, r.full_name, r.profile_name, r.title, r.profile_title,
      r.given_name, r.first_name, r.family_name, r.last_name, r.handle, r.username, r.alias, r.primary_alias
    ].map(x => (typeof x === 'string' ? x : '')).filter(Boolean);
    const ipfsLike = (s) => /^(Qm|bafy)[a-zA-Z0-9]{20,}$/i.test(s || '');
    let best = '';
    for (const cand of maybeNameFields) {
      if (!cand) continue;
      if (ipfsLike(cand)) continue;
      best = cand;
      break;
    }
    // If still empty, try to build from parts (first + last)
    if (!best) {
      const first = [r.given_name, r.first_name].find(v => typeof v === 'string' && v.trim());
      const last = [r.family_name, r.last_name].find(v => typeof v === 'string' && v.trim());
      if (first || last) best = [first || '', last || ''].join(' ').trim();
    }
    // If name like "Last, First" convert to "First Last"
    if (/^[^,]+,\s*[^,]+$/.test(best)) {
      const [a, b] = best.split(',');
      best = `${b.trim()} ${a.trim()}`.trim();
    }
    best = cleanName(best);
    // Title-case only if it looks all upper or all lower
    if (best && (best === best.toUpperCase() || best === best.toLowerCase())) {
      best = smartTitleCase(best);
    }
    r.displayName = best || (cid || '');
    // Ensure text field has at least the chosen name
    const desc = typeof r.description === 'string' ? r.description : '';
    const tags = [r.mbti, r.socionics, r.big5].filter(Boolean).join(' ');
    const baseText = typeof r.text === 'string' && r.text.trim() ? r.text : '';
    r.text = [r.displayName, tags, desc, baseText].filter(Boolean).join(' ').trim();
    r.cid = cid; // normalize to string
    return r;
  }

  function cleanName(s) {
    if (!s) return '';
    let t = String(s).normalize('NFC');
    t = t.replace(/^['"\s]+|['"\s]+$/g, ''); // trim quotes/spaces
    t = t.replace(/\s+/g, ' '); // collapse whitespace
    return t;
  }

  function smartTitleCase(s) {
    return s.split(' ').map(part => {
      if (!part) return '';
      // Preserve all-uppercase acronyms <= 4 chars
      if (part.length <= 4 && part === part.toUpperCase()) return part;
      const lower = part.toLowerCase();
      return lower.replace(/^[\p{L}]/u, c => c.toUpperCase());
    }).join(' ');
  }

  return { load, loadFromRows, loadFromParquet, search, getByCid, size, getSource };
})();

window.KNN = KNN;
