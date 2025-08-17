// Node.js script to search a CSV file and return results as JSON
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');

const CSV_PATH = path.resolve(__dirname, '../data/bot_store/pdb_profiles_flat.csv');
console.log('searchCsv: using CSV_PATH =', CSV_PATH);

async function searchCsv(query, limit = 50) {
  return new Promise((resolve, reject) => {
    const results = [];
    const q = query.trim().toLowerCase();
    let count = 0;
    fs.createReadStream(CSV_PATH)
      .pipe(csv())
      .on('data', (row) => {
        count++;
        if (Object.values(row).some(
          v => typeof v === 'string' && v.toLowerCase().includes(q)
        )) {
          results.push(row);
        }
        if (results.length >= limit) {
          this.destroy();
        }
      })
      .on('end', () => {
        console.log(`searchCsv: scanned ${count} records, found ${results.length}`);
        resolve(results);
      })
      .on('error', (err) => {
        console.error('searchCsv error:', err.message);
        reject(err);
      });
  });
}

if (require.main === module) {
  const query = process.argv[2] || '';
  searchCsv(query).then(results => {
    console.log(JSON.stringify(results, null, 2));
  }).catch(err => {
    console.error('Error:', err);
    process.exit(1);
  });
}

module.exports = searchCsv;
