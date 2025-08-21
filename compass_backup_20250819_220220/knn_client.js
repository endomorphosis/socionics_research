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
   let IDMAP = new Map(); // any known id (cid/pid/profile_cid/uuid/uid/...) -> record
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
     DATA = new Array(rows.length);
     VECTORS = new Array(rows.length);
     CIDX = new Map();
     IDMAP = new Map();
     for (let i = 0; i < rows.length; i++) {
       const r0 = rows[i] || {};
       const r = normalizeRecord(r0);
       DATA[i] = r;
       VECTORS[i] = embed(r.text);
       // Primary cid map
       if (r.cid) CIDX.set(r.cid, r);
       // Index by alternate id fields to improve name resolution for vector neighbors
       const ids = idCandidates(r0, r);
       for (const id of ids) {
         if (!id) continue;
         const key = String(id);
         if (!IDMAP.has(key)) IDMAP.set(key, r);
       }
     }
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
      const nm = r.displayName || r.name || r.cid || '';
      out.push({ ...r, name: nm, displayName: nm, _score: score });
    }
    return out;
  }

  function getByCid(cid) {
    if (!cid) return null;
    return CIDX.get(cid) || IDMAP.get(String(cid)) || null;
  }
  function resolveId(id) {
    if (id == null) return null;
    const rec = CIDX.get(String(id)) || IDMAP.get(String(id)) || null;
    return rec || null;
  }
  function size() { return DATA.length; }
  function getIdMapSize() { return IDMAP.size; }
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

   function idCandidates(raw, norm) {
     const ids = [];
     const add = (v) => {
       try {
         if (v == null) return;
         const s = typeof v === 'string' ? v : String(v);
         if (s !== '') ids.push(s);
       } catch {}
     };
     try {
       // Common explicit fields
       add(raw && raw.cid); add(raw && raw.profile_cid); add(raw && raw.pid);
       add(raw && raw.id); add(raw && raw.profile_id); add(raw && raw.uuid); add(raw && raw.uid);
       add(raw && raw.profile_uuid); add(raw && raw.profile_uid);
       add(norm && norm.cid);
       // Dynamically include any key that clearly looks like an ID:
       // matches: id, _id, cid, _cid, uuid, _uuid, uid, _uid (case-insensitive, word/underscore boundary)
       const looksLikeIdKey = (k) => /(^|_)(cid|uuid|uid|id)$/i.test(k || '');
       const harvest = (obj) => {
         if (!obj || typeof obj !== 'object') return;
         for (const [k, v] of Object.entries(obj)) {
           if (!looksLikeIdKey(k)) continue;
           // Avoid obvious non-identifiers like empty strings or arrays/objects
           if (Array.isArray(v) || (v && typeof v === 'object')) continue;
           add(v);
         }
       };
       harvest(raw);
       harvest(norm);
     } catch {}
     // Deduplicate while preserving first occurrence
     const out = [];
     const seen = new Set();
     for (const v of ids) { if (!seen.has(v)) { out.push(v); seen.add(v); } }
     return out;
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

  function getAll() { return DATA.slice(); }
  return { load, loadFromRows, loadFromParquet, search, getByCid, resolveId, size, getIdMapSize, getSource, getAll, embed };
})();

window.KNN = KNN;
