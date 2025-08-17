// Node.js script to search a Parquet file and return results as JSON
// Usage: node search_parquet.js <query>

const parquet = require('parquetjs-lite');
const path = require('path');

const PARQUET_PATH = path.resolve(__dirname, '../data/bot_store/pdb_profiles_flat.parquet');
console.log('searchParquet: using PARQUET_PATH =', PARQUET_PATH);

async function searchParquet(query) {
  const results = [];
  const q = query.trim().toLowerCase();
  console.log('searchParquet: query =', q);
  try {
    console.log('searchParquet: opening file at', PARQUET_PATH);
    const reader = await parquet.ParquetReader.openFile(PARQUET_PATH);
    const cursor = reader.getCursor();
    const record = await cursor.next();
    console.log('First record:', record);
    await reader.close();
    return [];
  } catch (err) {
    console.error('searchParquet error:', err && err.message);
    console.error('searchParquet error stack:', err && err.stack);
    console.error('searchParquet: attempted to open file at', PARQUET_PATH);
    const fs = require('fs');
    try {
      const stats = fs.statSync(PARQUET_PATH);
      console.error('searchParquet: file stats:', stats);
    } catch (statErr) {
      console.error('searchParquet: file stat error:', statErr.message);
    }
    return [];
  }
}

if (require.main === module) {
  const query = process.argv[2] || '';
  searchParquet(query).then(results => {
    console.log(JSON.stringify(results, null, 2));
  }).catch(err => {
    console.error('Error:', err);
    process.exit(1);
  });
}

module.exports = searchParquet;
