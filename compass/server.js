// Combined Express server for static frontend and RAG API
const express = require('express');
const path = require('path');

const app = express();
// Disable etag/304 to avoid stale client caches in dev
app.disable('etag');
app.set('etag', false);
const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;

// Expose raw dataset directory for optional in-browser DuckDB/Parquet loading
const fs = require('fs');
const DATASET_DIR = path.join(__dirname, '..', 'data', 'bot_store');

// Helper: set no-store cache headers to avoid stale content in dev/preview
function setNoStore(res) {
  try {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    res.setHeader('Surrogate-Control', 'no-store');
  } catch {}
}

// Lightweight liveness endpoint for smoke tests and monitoring
app.get('/health', (_req, res) => {
  res.json({ ok: true, status: 'ok', time: Date.now() });
});

// Dev-only: force client to clear site data (cache, storage, cookies, JS contexts)
app.get('/dev/clear-site-data', (req, res) => {
  setNoStore(res);
  res.setHeader('Clear-Site-Data', '"cache", "cookies", "storage", "executionContexts"');
  res.status(200).send('Clear-Site-Data issued. Reload the app.');
});

// Place dataset routes BEFORE static middleware so SPA fallback doesn't catch them
app.get('/dataset/:name', (req, res) => {
  // Strip any path separators and keep simple filenames only
  const raw = String(req.params.name || '');
  const safe = raw.replace(/[^a-zA-Z0-9_.-]/g, '');
  const filePath = path.join(DATASET_DIR, safe);
  console.log(`[dataset] GET ${raw} -> ${filePath}`);
  fs.stat(filePath, (err, stat) => {
    if (err || !stat || !stat.isFile()) {
      res.status(404).send(`Not found: ${safe}`);
      return;
    }
  // Set explicit content-type for certain binary formats
    if (/\.parquet$/i.test(safe)) {
      res.setHeader('Content-Type', 'application/octet-stream');
    } else if (/\.(index|bin)$/i.test(safe)) {
      res.setHeader('Content-Type', 'application/octet-stream');
    }
  setNoStore(res);
    res.sendFile(filePath, (sendErr) => {
      if (sendErr) res.status(sendErr.statusCode || 404).end();
    });
  });
});

app.get('/dataset', (req, res) => {
  fs.readdir(DATASET_DIR, (err, files) => {
    if (err) return res.status(500).json({ error: String(err) });
    res.json({ dir: DATASET_DIR, files });
  });
});

// Also serve dataset files statically for direct file access
app.use('/dataset', express.static(DATASET_DIR, {
  etag: false,
  lastModified: false,
  maxAge: 0,
  setHeaders: (res) => setNoStore(res)
}));

// Serve static files from Vite build (dist) and public assets
app.use(express.static(path.join(__dirname, 'dist'), {
  etag: false,
  lastModified: false,
  maxAge: 0,
  setHeaders: (res) => setNoStore(res)
}));
app.use(express.static(path.join(__dirname, 'public'), {
  etag: false,
  lastModified: false,
  maxAge: 0,
  setHeaders: (res) => setNoStore(res)
}));

// Fallback to index.html for SPA routing (only for non-API, non-static requests)
app.use((req, res, next) => {
  if (
    req.method === 'GET' &&
    !req.path.startsWith('/api/') &&
    !req.path.startsWith('/dataset') &&
    !req.path.includes('.')
  ) {
  setNoStore(res);
  res.sendFile(path.join(__dirname, 'dist', 'index.html'));
  } else {
    next();
  }
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}/`);
  console.log(`Static dataset at http://localhost:${PORT}/pdb_profiles.json`);
  console.log(`Dataset directory mapped: ${DATASET_DIR} -> /dataset/:name`);
});
