// DuckDB-Wasm helper to load vectors from Parquet served under /dataset
// Exposes window.DuckVec with init() and loadVectors(parquetUrl)

import * as duckdb from '@duckdb/duckdb-wasm';

const DuckVec = (() => {
  let db = null;
  let conn = null;

  async function init() {
    if (db) return;
    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);
    const worker = new Worker(bundle.mainWorker);
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    conn = await db.connect();
    // Enable HTTPFS so DuckDB can read from /dataset
    try {
      await conn.query('INSTALL httpfs;');
      await conn.query('LOAD httpfs;');
      await conn.query("SET enable_http_metadata_cache=true;");
    } catch (e) {
      console.warn('DuckDB httpfs install/load failed (may already be loaded):', e?.message || e);
    }
  }

  async function loadVectors(parquetUrl = '/dataset/pdb_profile_vectors.parquet') {
    await init();
    // Try common column names for id and vector fields
    const idCols = ['cid', 'profile_cid', 'id', 'profile_id'];
    const vecCols = ['vector', 'embedding', 'embeddings', 'vec', 'values'];
    let res;
    let used = null;
    let lastErr = null;
    for (const idc of idCols) {
      for (const vc of vecCols) {
        const sql = `SELECT CAST(${idc} AS VARCHAR) AS cid, list_transform(${vc}, x -> CAST(x AS DOUBLE)) AS vector FROM read_parquet('${parquetUrl}')`;
        try {
          res = await conn.query(sql);
          used = { idc, vc };
          lastErr = null;
          break;
        } catch (e) {
          lastErr = e;
          continue;
        }
      }
      if (used) break;
    }
    if (!res || !used) {
      console.error('DuckDB vectors autodetect failed:', lastErr);
      throw new Error(`DuckDB could not locate id/vector columns in Parquet (${parquetUrl}). Tried ids=${idCols.join(', ')}; vectors=${vecCols.join(', ')}.`);
    }
    const out = [];
    for (let i = 0; i < res.numRows; i++) {
      const row = res.get(i);
      const cid = String(row.cid);
      const vec = row.vector;
      if (!cid || !vec) continue;
      // Ensure numeric array
      const arr = Array.isArray(vec) ? vec.map(Number) : [];
      if (arr.length) out.push({ cid, vector: arr });
    }
    return out;
  }

  async function loadProfiles(parquetUrl = '/dataset/pdb_profiles_normalized.parquet') {
    await init();
    // Autodetect typical columns
    const idCols = ['cid', 'profile_cid', 'id', 'profile_id'];
    const nameCols = ['name', 'profile_name'];
    const mbtiCols = ['mbti', 'mbti_type'];
    const socCols = ['socionics', 'socionics_type', 'socionics_code'];
    const big5Cols = ['big5', 'bigfive', 'big_five', 'big5_code'];

    function coalesceExpr(cols) { return cols.map(c => `TRY_CAST(${c} AS VARCHAR)`).join(' , '); }

    let res;
    let lastErr = null;
    let chosen = null;
    for (const idc of idCols) {
      // Build COALESCE chains for optional columns
      const nameExpr = `COALESCE(${coalesceExpr(nameCols)}, '')`;
      const mbtiExpr = `COALESCE(${coalesceExpr(mbtiCols)}, '')`;
      const socExpr = `COALESCE(${coalesceExpr(socCols)}, '')`;
      const big5Expr = `COALESCE(${coalesceExpr(big5Cols)}, '')`;
      const sql = `
        SELECT
          CAST(${idc} AS VARCHAR) AS cid,
          ${nameExpr} AS name,
          ${mbtiExpr} AS mbti,
          ${socExpr} AS socionics,
          ${big5Expr} AS big5,
          TRIM(
            CONCAT_WS(' ', ${nameExpr}, ${mbtiExpr}, ${socExpr}, ${big5Expr})
          ) AS text
        FROM read_parquet('${parquetUrl}')
      `;
      try {
        res = await conn.query(sql);
        chosen = { idc };
        lastErr = null;
        break;
      } catch (e) {
        lastErr = e;
        continue;
      }
    }
    if (!res || !chosen) {
      console.error('DuckDB profiles autodetect failed:', lastErr);
      throw new Error(`DuckDB could not locate id column in profiles Parquet (${parquetUrl}). Tried ids=${idCols.join(', ')}.`);
    }
    const out = [];
    for (let i = 0; i < res.numRows; i++) {
      const r = res.get(i);
      out.push({
        cid: r.cid ? String(r.cid) : undefined,
        name: r.name || undefined,
        mbti: r.mbti || undefined,
        socionics: r.socionics || undefined,
        big5: r.big5 || undefined,
        text: r.text || ''
      });
    }
    return out;
  }

  return { init, loadVectors, loadProfiles };
})();

window.DuckVec = DuckVec;
