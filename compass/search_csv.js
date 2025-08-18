// Node.js script to search a CSV file and return results as JSON
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');

const CSV_PATH = path.resolve(__dirname, '../data/bot_store/pdb_profiles_flat.csv');
console.log('searchCsv: using CSV_PATH =', CSV_PATH);

function parseArgs(argv) {
  const out = { query: '', limit: 50, preview: true };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--no-preview') out.preview = false;
    else if (a === '--limit' && i + 1 < argv.length) {
      const n = parseInt(argv[++i], 10);
      if (!Number.isNaN(n) && n > 0) out.limit = n;
    } else if (!a.startsWith('--') && !out.query) {
      out.query = a;
    }
  }
  return out;
}

async function searchCsv(query, limit = 50, preview = true) {
  return new Promise((resolve, reject) => {
    const results = [];
    const firstRows = [];
    const q = query.trim().toLowerCase();
    let count = 0;
    const stream = fs.createReadStream(CSV_PATH)
      .pipe(csv())
      .on('data', (row) => {
        count++;
        if (preview && firstRows.length < 5) {
          firstRows.push(row);
        }
        if (Object.values(row).some(
          v => typeof v === 'string' && v.toLowerCase().includes(q)
        )) {
          results.push(row);
        }
        if (results.length >= limit) {
          stream.destroy();
        }
      })
      .on('end', () => {
        if (preview) {
          console.log('First 5 rows from CSV:', JSON.stringify(firstRows, null, 2));
        }
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
  const { query, limit, preview } = parseArgs(process.argv);
  searchCsv(query, limit, preview).then(results => {
    console.log(`searchCsv: found ${results.length} results.`);
    if (results.length > 0) {
      // Print a compact first result summary to avoid huge logs
      const r = results[0];
      const short = { cid: r.cid, name: r.name, mbti: r.mbti, socionics: r.socionics, big5: r.big5 };
      console.log('First result (compact):', JSON.stringify(short));
    } else {
      console.log('No results found for query:', query);
    }
  }).catch(err => {
    console.error('Error:', err);
    process.exit(1);
  });
}

module.exports = searchCsv;
