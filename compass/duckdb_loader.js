// DuckDB-Wasm helper to load vectors/profiles from Parquet served under /dataset
// Exposes window.DuckVec with init(), loadVectors(), loadProfiles()

import * as duckdb from '@duckdb/duckdb-wasm';

const DuckVec = (() => {
  let db = null;
  let conn = null;

  async function init() {
    if (db) return;
    const mainWorkerUrl = new URL('@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js', import.meta.url);
    const wasmUrl = new URL('@duckdb/duckdb-wasm/dist/duckdb-eh.wasm', import.meta.url);
    const worker = new Worker(mainWorkerUrl);
    // Quiet logger
    let logger;
    try {
      if (duckdb.ConsoleLogger) {
        logger = new duckdb.ConsoleLogger(duckdb.LogLevel && (duckdb.LogLevel.ERROR || duckdb.LogLevel.WARNING));
        if (logger && typeof logger.setLogLevel === 'function') logger.setLogLevel((duckdb.LogLevel && (duckdb.LogLevel.ERROR || duckdb.LogLevel.WARNING)) || 2);
      }
    } catch {}
    if (!logger) logger = { debug(){}, info(){}, warn(){}, error(){}, log(){} };
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(wasmUrl.toString());
    conn = await db.connect();
    try {
      await conn.query('INSTALL httpfs;');
      await conn.query('LOAD httpfs;');
      await conn.query('SET enable_http_metadata_cache=true;');
    } catch (e) {
      console.warn('DuckDB httpfs install/load failed (may already be loaded):', e?.message || e);
    }
  }

  async function describeParquet(absUrl) {
    const rows = [];
    try {
      const res = await conn.query(`DESCRIBE SELECT * FROM read_parquet('${absUrl}')`);
      for (let i = 0; i < res.numRows; i++) {
        const r = res.get(i);
        const name = r.column_name || r.COLUMN_NAME || r.name || '';
        const type = r.column_type || r.COLUMN_TYPE || r.type || '';
        if (name) rows.push({ name: String(name), type: String(type || '') });
      }
    } catch (e) {
      console.warn('DuckDB DESCRIBE failed:', e && e.message || e);
    }
    return rows;
  }

  function toArrayMaybe(x) {
    try {
      if (Array.isArray(x)) return x;
      if (x && typeof x === 'object') {
        if (typeof x.toArray === 'function') return x.toArray();
        if (ArrayBuffer.isView(x)) return Array.from(x);
      }
      if (typeof x === 'string') {
        const v = JSON.parse(x);
        return Array.isArray(v) ? v : [];
      }
    } catch {}
    return [];
  }

  async function loadVectors(parquetUrl = '/dataset/pdb_profile_vectors.parquet') {
    await init();
  const idColsBase = ['cid', 'pid', 'profile_cid', 'id', 'profile_id', 'uuid', 'uid', 'profile_uuid', 'profile_uid'];
  const vecCols = ['vector', 'embedding', 'embeddings', 'vec', 'values', 'vectors', 'features', 'feat', 'embedding_vector', 'vector_64', 'embedding_64'];
    const absUrl = new URL(parquetUrl, (globalThis.location && globalThis.location.origin) || 'http://localhost:3000').toString();
    try {
      const head = await fetch(absUrl, { method: 'HEAD' });
      if (!head.ok) throw new Error(`HTTP ${head.status}`);
    } catch (e) {
      console.error('DuckDB vectors: parquet not reachable at', absUrl, e && e.message || e);
      throw new Error(`Parquet not found at ${absUrl}`);
    }
    const schema = await describeParquet(absUrl);
    // Expand id candidates dynamically based on schema
    const dynId = Array.from(new Set(
      schema.map(s => s.name).filter(n => /(^|_)(cid|uuid|uid|id)$/i.test(n))
    ));
    const idCols = Array.from(new Set([...idColsBase, ...dynId]));
    const present = new Set(schema.map(c => c.name));
    const presentIds = idCols.filter(c => present.has(c));
    const presentVecs = vecCols.filter(c => present.has(c));
    let res; let used = null; let lastErr = null;
    // Strategy 1: list or json vector column
    for (const idc of (presentIds.length ? presentIds : idCols)) {
      for (const vc of (presentVecs.length ? presentVecs : vecCols)) {
        const t = (schema.find(s => s.name === vc) || {}).type || '';
        const isList = /list|\[\]/i.test(t);
        const isJsonish = /json|varchar|string/i.test(t);
        const selects = [`CAST(${idc} AS VARCHAR) AS cid`];
        if (isList) selects.push(`list_transform(${vc}, x -> CAST(x AS DOUBLE)) AS vector`);
        else if (isJsonish) selects.push(`list_transform(from_json(${vc}), x -> CAST(x AS DOUBLE)) AS vector`);
        else selects.push(`list_transform(${vc}, x -> CAST(x AS DOUBLE)) AS vector`);
        const sql = `SELECT ${selects.join(', ')} FROM read_parquet('${absUrl}')`;
        try {
          res = await conn.query(sql);
          used = { idc, vc };
          lastErr = null;
          break;
        } catch (e) { lastErr = e; }
      }
      if (used) break;
    }
    // Strategy 2: fixed columns v0..v63
    if (!used && schema.length) {
      const vecFixed = schema.map(s => s.name).filter(n => /^v(\d+)$/.test(n)).sort((a,b)=>parseInt(a.slice(1))-parseInt(b.slice(1)));
      if (vecFixed.length >= 8) {
        for (const idc of (presentIds.length ? presentIds : idCols)) {
          const parts = vecFixed.map(c => `CAST(${c} AS DOUBLE)`);
          const sql = `SELECT CAST(${idc} AS VARCHAR) AS cid, list_value(${parts.join(',')}) AS vector FROM read_parquet('${absUrl}')`;
          try { res = await conn.query(sql); used = { idc, vc: `fixed(${vecFixed.length})` }; break; } catch (e) { lastErr = e; }
        }
      }
    }
    if (!res || !used) {
      console.error('DuckDB vectors autodetect failed:', lastErr);
      throw new Error(`DuckDB could not locate id/vector columns in Parquet (${parquetUrl}). Tried ids=[${idCols.join(', ')}]; vectors=[${vecCols.join(', ')}]. Ensure the file contains an id column and a vector column.`);
    }
    console.info(`DuckDB vectors: using id='${used.idc}', vector='${used.vc}' from ${absUrl}`);
    const out = [];
    for (let i = 0; i < res.numRows; i++) {
      const row = res.get(i);
      const cid = row && row.cid != null ? String(row.cid) : '';
      const arr = toArrayMaybe(row && row.vector).map(Number);
      if (cid && arr && arr.length) out.push({ cid, vector: arr });
    }
    // Harmonize dimensions: pick the mode length and normalize all rows to that length (pad/truncate and coerce non-finite to 0)
    const counts = new Map();
    for (const r of out) counts.set(r.vector.length, (counts.get(r.vector.length) || 0) + 1);
    let modeLen = 0, modeCnt = -1;
    for (const [L, c] of counts.entries()) { if (c > modeCnt || (c === modeCnt && L > modeLen)) { modeLen = L; modeCnt = c; } }
    let padded = 0, truncated = 0, fixedNonFinite = 0, dropped = 0;
    const fixed = [];
    for (const r of out) {
      if (!r || !r.cid || !Array.isArray(r.vector) || r.vector.length === 0) { dropped++; continue; }
      const v = r.vector;
      const arr = new Array(modeLen);
      if (v.length >= modeLen) {
        for (let j = 0; j < modeLen; j++) { const x = v[j]; const y = Number.isFinite(x) ? x : 0; if (!Number.isFinite(x)) fixedNonFinite++; arr[j] = y; }
        if (v.length > modeLen) truncated++;
      } else {
        for (let j = 0; j < v.length; j++) { const x = v[j]; const y = Number.isFinite(x) ? x : 0; if (!Number.isFinite(x)) fixedNonFinite++; arr[j] = y; }
        for (let j = v.length; j < modeLen; j++) arr[j] = 0;
        padded++;
      }
      fixed.push({ cid: r.cid, vector: arr });
    }
    if (padded || truncated || dropped || fixedNonFinite) console.warn(`DuckDB vectors: normalized=${fixed.length}/${out.length} dim=${modeLen} (padded=${padded}, truncated=${truncated}, nonFinite->0=${fixedNonFinite})`);
    return fixed;
  }

  async function loadProfiles(parquetUrl = '/dataset/pdb_profiles_normalized.parquet') {
    await init();
  const idColsBase = ['cid', 'pid', 'profile_cid', 'id', 'profile_id', 'uuid', 'uid', 'profile_uuid', 'profile_uid'];
    const nameCols = ['display_name', 'displayName', 'name', 'profile_name', 'title', 'profile_title'];
    const mbtiCols = ['mbti', 'mbti_type', 'mbti_code'];
    const socCols = ['socionics', 'socionics_type', 'socionics_code'];
    const big5Cols = ['big5', 'bigfive', 'big_five', 'big5_code'];
    const descCols = ['description', 'bio', 'about', 'summary'];
    const absUrl = new URL(parquetUrl, (globalThis.location && globalThis.location.origin) || 'http://localhost:3000').toString();
    try {
      const head = await fetch(absUrl, { method: 'HEAD' });
      if (!head.ok) throw new Error(`HTTP ${head.status}`);
    } catch (e) {
      console.error('DuckDB profiles: parquet not reachable at', absUrl, e && e.message || e);
      throw new Error(`Parquet not found at ${absUrl}`);
    }
    const schema = await describeParquet(absUrl);
    const have = new Set(schema.map(c => c.name));
    const dynId = Array.from(new Set(
      schema.map(s => s.name).filter(n => /(^|_)(cid|uuid|uid|id)$/i.test(n))
    ));
    const idCols = Array.from(new Set([...idColsBase, ...dynId]));
    const filterCols = (cols) => cols.filter(c => have.has(c));
    const idTry = filterCols(idCols).length ? filterCols(idCols) : idCols;
    function coalesceExpr(cols) {
      const present = filterCols(cols);
      if (!present.length) return `''`;
      return present.map(c => `TRY_CAST(${c} AS VARCHAR)`).join(' , ');
    }
    let res; let lastErr = null; let chosen = null;
    for (const idc of idTry) {
      const nameExpr = `NULLIF(TRIM(COALESCE(${coalesceExpr(nameCols)}, '')), '')`;
      const mbtiExpr = `COALESCE(${coalesceExpr(mbtiCols)}, '')`;
      const socExpr = `COALESCE(${coalesceExpr(socCols)}, '')`;
      const big5Expr = `COALESCE(${coalesceExpr(big5Cols)}, '')`;
      // Also project commonly useful id columns when available for downstream resolvers
      const extraIdExprs = filterCols(idCols).map(c => `TRY_CAST(${c} AS VARCHAR) AS ${c}`);
      const sql = `
        SELECT
          CAST(${idc} AS VARCHAR) AS cid,
          COALESCE(${nameExpr}, '') AS name,
          ${mbtiExpr} AS mbti,
          ${socExpr} AS socionics,
          ${big5Expr} AS big5,
          COALESCE(${coalesceExpr(descCols)}, '') AS description,
          TRIM(CONCAT_WS(' ', COALESCE(${nameExpr}, ''), ${mbtiExpr}, ${socExpr}, ${big5Expr}, COALESCE(${coalesceExpr(descCols)}, ''))) AS text
          ${extraIdExprs.length ? ',' + extraIdExprs.join(',') : ''}
        FROM read_parquet('${absUrl}')
      `;
      try { res = await conn.query(sql); chosen = { idc }; break; } catch (e) { lastErr = e; }
    }
    if (!res || !chosen) {
      console.error('DuckDB profiles autodetect failed:', lastErr);
      throw new Error(`DuckDB could not locate id column in profiles Parquet (${parquetUrl}). Tried ids=[${idCols.join(', ')}].`);
    }
    console.info(`DuckDB profiles: using id='${chosen.idc}' from ${absUrl}`);
    const out = [];
    for (let i = 0; i < res.numRows; i++) {
      const r = res.get(i);
      const base = {
        cid: r && r.cid != null ? String(r.cid) : undefined,
        name: r && r.name || undefined,
        mbti: r && r.mbti || undefined,
        socionics: r && r.socionics || undefined,
        big5: r && r.big5 || undefined,
        description: r && r.description || undefined,
        text: r && r.text || ''
      };
      // Attach any extra id columns that were selected
      for (const c of idCols) {
        if (r && Object.prototype.hasOwnProperty.call(r, c) && r[c] != null) {
          base[c] = String(r[c]);
        }
      }
      out.push(base);
    }
    // Try to fill missing names from CSV if present
    try { await fillNamesFromCsv(out); } catch (e) { console.warn('DuckDB profiles: name backfill skipped:', e && e.message || e); }
    return out;
  }

  async function fillNamesFromCsv(rows) {
    if (!rows || !rows.length) return;
    const csvUrl = new URL('/dataset/pdb_profiles_flat.csv', (globalThis.location && globalThis.location.origin) || 'http://localhost:3000').toString();
    // Quick existence check
    try {
      const head = await fetch(csvUrl, { method: 'HEAD' });
      if (!head.ok) return; // not present
    } catch { return; }
    // Identify id and name columns from CSV schema
    let schema = [];
    try {
      const res = await conn.query(`DESCRIBE SELECT * FROM read_csv_auto('${csvUrl}')`);
      for (let i = 0; i < res.numRows; i++) {
        const r = res.get(i);
        const name = r.column_name || r.COLUMN_NAME || r.name || '';
        const type = r.column_type || r.COLUMN_TYPE || r.type || '';
        if (name) schema.push({ name: String(name), type: String(type || '') });
      }
    } catch { return; }
    const have = new Set(schema.map(s => s.name));
    const dynId = Array.from(new Set(schema.map(s => s.name).filter(n => /(^|_)(cid|uuid|uid|id)$/i.test(n))));
    const idCols = Array.from(new Set(['cid','pid','profile_cid','id','profile_id','uuid','uid','profile_uuid','profile_uid', ...dynId]));
    const nameCols = schema.map(s => s.name).filter(n => /(^name$|display_?name|profile_?name|full_?name|title)$/i.test(n));
    const idc = idCols.find(c => have.has(c));
    if (!idc || !nameCols.length) return;
    const nameExpr = `NULLIF(TRIM(COALESCE(${nameCols.filter(c=>have.has(c)).map(c=>`TRY_CAST(${c} AS VARCHAR)`).join(', ')})), '')`;
    const sql = `SELECT TRY_CAST(${idc} AS VARCHAR) AS cid, ${nameExpr} AS name FROM read_csv_auto('${csvUrl}')`;
    const map = new Map();
    try {
      const res = await conn.query(sql);
      for (let i = 0; i < res.numRows; i++) {
        const r = res.get(i);
        const cid = r && r.cid ? String(r.cid) : '';
        const nm = r && r.name ? String(r.name) : '';
        if (cid && nm) map.set(cid, nm);
      }
    } catch { return; }
    if (!map.size) return;
    let filled = 0;
    for (const r of rows) {
      if (!r) continue;
      if (!r.name || !String(r.name).trim()) {
        const nm = map.get(r.cid);
        if (nm) { r.name = nm; if (!r.text) r.text = nm; filled++; }
      }
    }
    if (filled) console.info(`DuckDB profiles: filled ${filled} missing names from CSV`);
  }

  // --- Parquet export helpers ---
  async function exportVectorsParquet(rows) {
    await init();
    if (!Array.isArray(rows) || !rows.length) throw new Error('No rows');
    // Register JSON in DuckDB FS and COPY to Parquet
    const inPath = `/tmp_vec_${Date.now()}.json`;
    const outPath = `/out_vec_${Date.now()}.parquet`;
    try {
      if (typeof db.registerFileText === 'function') {
        await db.registerFileText(inPath, JSON.stringify(rows));
      } else if (typeof db.registerFileBuffer === 'function') {
        const enc = new TextEncoder();
        await db.registerFileBuffer(inPath, enc.encode(JSON.stringify(rows)));
      } else {
        throw new Error('DuckDB file registration API not available');
      }
      await conn.query(`COPY (SELECT * FROM read_json_auto('${inPath}')) TO '${outPath}' (FORMAT 'PARQUET')`);
      if (typeof db.copyFileToArrayBuffer === 'function') {
        const buf = await db.copyFileToArrayBuffer(outPath);
        return new Uint8Array(buf);
      }
      throw new Error('DuckDB copyFileToArrayBuffer not available');
    } finally {
      try { if (typeof db.unregisterFile === 'function') await db.unregisterFile(inPath); } catch {}
      try { if (typeof db.unregisterFile === 'function') await db.unregisterFile(outPath); } catch {}
    }
  }

  async function exportProfilesParquet(rows) {
    await init();
    if (!Array.isArray(rows) || !rows.length) throw new Error('No rows');
    const inPath = `/tmp_prof_${Date.now()}.json`;
    const outPath = `/out_prof_${Date.now()}.parquet`;
    try {
      if (typeof db.registerFileText === 'function') {
        await db.registerFileText(inPath, JSON.stringify(rows));
      } else if (typeof db.registerFileBuffer === 'function') {
        const enc = new TextEncoder();
        await db.registerFileBuffer(inPath, enc.encode(JSON.stringify(rows)));
      } else {
        throw new Error('DuckDB file registration API not available');
      }
      await conn.query(`COPY (SELECT * FROM read_json_auto('${inPath}')) TO '${outPath}' (FORMAT 'PARQUET')`);
      if (typeof db.copyFileToArrayBuffer === 'function') {
        const buf = await db.copyFileToArrayBuffer(outPath);
        return new Uint8Array(buf);
      }
      throw new Error('DuckDB copyFileToArrayBuffer not available');
    } finally {
      try { if (typeof db.unregisterFile === 'function') await db.unregisterFile(inPath); } catch {}
      try { if (typeof db.unregisterFile === 'function') await db.unregisterFile(outPath); } catch {}
    }
  }

  return { init, loadVectors, loadProfiles, exportVectorsParquet, exportProfilesParquet };
})();

window.DuckVec = DuckVec;
