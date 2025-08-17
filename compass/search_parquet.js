// Node.js script to search a Parquet file and return results as JSON
// Usage: node search_parquet.js <query>

const parquet = require('parquetjs-lite');
const path = require('path');

const PARQUET_PATH = path.resolve(__dirname, '../data/bot_store/pdb_profiles.parquet');
console.log('searchParquet: using PARQUET_PATH =', PARQUET_PATH);

// Sample data for demonstration and fallback
const SAMPLE_PERSONALITIES = [
  { label: 'Albert Einstein', x: 0.8, y: 0.3, z: 0.9, color: '#e6194b', type: 'scientist', socionics: 'ILE', description: 'Theoretical physicist' },
  { label: 'Marie Curie', x: 0.6, y: -0.4, z: 0.8, color: '#3cb44b', type: 'scientist', socionics: 'LII', description: 'Nobel Prize winner in physics and chemistry' },
  { label: 'Leonardo da Vinci', x: 0.9, y: 0.7, z: 0.95, color: '#ffe119', type: 'artist', socionics: 'IEE', description: 'Renaissance polymath' },
  { label: 'Nikola Tesla', x: -0.2, y: 0.8, z: 0.85, color: '#4363d8', type: 'inventor', socionics: 'ILI', description: 'Electrical engineer and inventor' },
  { label: 'Winston Churchill', x: 0.7, y: -0.6, z: -0.3, color: '#e6194b', type: 'politician', socionics: 'SLE', description: 'British Prime Minister' },
  { label: 'Jane Austen', x: -0.4, y: 0.6, z: -0.2, color: '#3cb44b', type: 'writer', socionics: 'EII', description: 'English novelist' },
  { label: 'Steve Jobs', x: 0.3, y: 0.9, z: 0.7, color: '#ffe119', type: 'entrepreneur', socionics: 'EIE', description: 'Apple co-founder' },
  { label: 'Sherlock Holmes', x: -0.8, y: -0.9, z: 0.6, color: '#4363d8', type: 'fictional', socionics: 'LII', description: 'Fictional detective' },
  { label: 'Mozart', x: 0.1, y: 0.8, z: 0.4, color: '#e6194b', type: 'musician', socionics: 'SEE', description: 'Classical composer' },
  { label: 'Gandhi', x: -0.6, y: 0.4, z: -0.8, color: '#3cb44b', type: 'leader', socionics: 'EII', description: 'Indian independence leader' }
];

async function searchParquet(query) {
  const q = query.trim().toLowerCase();
  console.log('searchParquet: query =', q);
  
  // If no query, return empty results
  if (!q) {
    return [];
  }
  
  try {
    // First try to read from parquet file
    console.log('searchParquet: opening file at', PARQUET_PATH);
    const reader = await parquet.ParquetReader.openFile(PARQUET_PATH);
    const cursor = reader.getCursor();
    const results = [];
    
    let record;
    while (record = await cursor.next()) {
      // Search in relevant fields (adjust based on actual schema)
      const searchText = (
        (record.label || '') + ' ' +
        (record.name || '') + ' ' + 
        (record.type || '') + ' ' +
        (record.socionics || '') + ' ' +
        (record.description || '')
      ).toLowerCase();
      
      if (searchText.includes(q)) {
        results.push({
          label: record.label || record.name || 'Unknown',
          x: parseFloat(record.x || Math.random() * 2 - 1),
          y: parseFloat(record.y || Math.random() * 2 - 1),
          z: parseFloat(record.z || Math.random() * 2 - 1),
          color: record.color || '#888888',
          type: record.type || 'unknown',
          socionics: record.socionics || '',
          description: record.description || ''
        });
      }
      
      // Limit results to prevent overwhelming the UI
      if (results.length >= 10) break;
    }
    
    await reader.close();
    console.log(`searchParquet: found ${results.length} results`);
    return results;
    
  } catch (err) {
    console.error('searchParquet error:', err && err.message);
    console.log('searchParquet: falling back to sample data');
    
    // Fallback to sample data
    const results = SAMPLE_PERSONALITIES.filter(person => 
      person.label.toLowerCase().includes(q) ||
      person.type.toLowerCase().includes(q) ||
      person.socionics.toLowerCase().includes(q) ||
      person.description.toLowerCase().includes(q)
    );
    
    console.log(`searchParquet: sample data returned ${results.length} results`);
    return results.slice(0, 10); // Limit to 10 results
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
