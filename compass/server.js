// Combined Express server for static frontend and RAG API
const express = require('express');
const path = require('path');
const searchParquet = require('./search_parquet');

const app = express();
const PORT = 3000;

// Serve static files from Vite build (dist)
app.use(express.static(path.join(__dirname, 'dist')));

// RAG search API endpoint
app.get('/api/search', async (req, res) => {
  const query = req.query.q || '';
  try {
    const results = await searchParquet(query);
    res.json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Fallback to index.html for SPA routing (only for non-API, non-static requests)
app.use((req, res, next) => {
  if (req.method === 'GET' && !req.path.startsWith('/api/') && !req.path.includes('.')) {
    res.sendFile(path.join(__dirname, 'dist', 'index.html'));
  } else {
    next();
  }
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}/`);
  console.log(`RAG search API at http://localhost:${PORT}/api/search?q=your_query`);
});
