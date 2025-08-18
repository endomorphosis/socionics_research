// Deprecated: legacy Express API for CSV search (client-side search replaced it)
const express = require('express');
const searchCsv = require('./search_csv');

const app = express();
const PORT = 3030;

app.get('/api/search', async (req, res) => {
  const query = req.query.q || '';
  try {
    const results = await searchCsv(query);
    if (!Array.isArray(results)) {
      console.error('searchCsv did not return an array:', results);
      return res.json([]);
    }
    res.json(results);
  } catch (err) {
    console.error('API error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`RAG search API running at http://localhost:${PORT}/api/search?q=your_query`);
});
