// Combined Express server for static frontend and RAG API
const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;

// Expose raw dataset directory for optional in-browser DuckDB/Parquet loading
const fs = require('fs');
const DATASET_DIR = path.join(__dirname, '..', 'data', 'bot_store');

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
app.use('/dataset', express.static(DATASET_DIR));

// Serve static files from Vite build (dist) and public assets
app.use(express.static(path.join(__dirname, 'dist')));
app.use(express.static(path.join(__dirname, 'public')));

// Fallback to index.html for SPA routing (only for non-API, non-static requests)
app.use((req, res, next) => {
  if (
    req.method === 'GET' &&
    !req.path.startsWith('/api/') &&
    !req.path.startsWith('/dataset') &&
    !req.path.includes('.')
  ) {
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
