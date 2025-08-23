# Personality Database Viewer

A comprehensive dashboard for searching, scraping, editing, and querying personality database data from the `data/bot_store` parquet/duckdb datastore.

## Features

- **Search & Browse**: Search through personality profiles with advanced filtering
- **Data Management**: Edit, import, export, and validate data
- **Scraper Controls**: Full scraping of personality-database.com using Playwright or Selenium
- **Query Builder**: Execute custom SQL queries on the data
- **Real-time Progress**: Live updates during scraping operations

## Technology Stack

- **Frontend**: Vanilla JavaScript with Vite, DuckDB WASM for data queries
- **Backend**: Node.js with Express for API endpoints  
- **Scraping**: Playwright (primary) with Selenium fallback
- **Data Storage**: Parquet files with DuckDB for querying

## Installation

1. Install dependencies:
```bash
cd data/viewer
npm install
```

2. Install Python dependencies for scraping (if not already available):
```bash
pip install playwright selenium
python -m playwright install chromium
```

3. For Chrome/Chromium for Selenium:
```bash
# On Ubuntu/Debian
sudo apt-get install chromium-browser

# Or download Chrome manually
```

## Usage

### Development Mode

Start the development server:
```bash
npm run dev
```

This will start:
- Frontend development server on http://localhost:5173
- Backend API server on http://localhost:3001

### Production Mode

Build and serve the application:
```bash
npm run build
npm start
```

### Standalone Scraper

You can run the scraper independently:

```bash
# Full scrape using Playwright
node scraper/index.js full-scrape --browser playwright

# Scrape specific profile
node scraper/index.js profile "https://www.personality-database.com/profile/12345" --browser selenium

# Search and scrape results
node scraper/index.js search "INTJ characters" --browser playwright

# Scrape specific category
node scraper/index.js category "https://www.personality-database.com/category/anime" --browser selenium
```

## Dashboard Features

### Search & Browse Panel
- Text search across profile names, MBTI types, Socionics types, and descriptions
- Filter by MBTI type, Socionics type, and data source
- Toggle between table and card view
- Pagination for large datasets
- Click profiles to edit them

### Edit Data Panel
- Import data from Parquet, CSV, or JSON files
- Export data in various formats
- Create backups of current data
- Normalize data formats
- Remove duplicates
- Validate data integrity
- Edit individual profiles inline

### Scraper Controls Panel
- Configure scraping parameters (delay, max pages, browser engine)
- Start full scrape of personality-database.com
- Run incremental scrapes for updates
- Scrape specific profiles by URL
- Real-time progress tracking with logs
- Stop scraping operations

### Query Builder Panel
- Execute custom SQL queries on the dataset
- Pre-built query templates for common operations
- View results in tabular format
- Save frequently used queries

## Data Sources

The viewer works with data stored in `../bot_store/`:
- `pdb_profiles_normalized.parquet` - Main normalized profile data
- `pdb_profiles.parquet` - Raw profile data (fallback)
- `pdb_profile_vectors.parquet` - Profile embeddings (if available)
- `pdb_faiss.index` - FAISS vector index (if available)

## Scraping Configuration

### Headers Setup
For optimal scraping results, create `../../.secrets/pdb_headers.json` with browser headers:

```json
{
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  "Referer": "https://www.personality-database.com/",
  "Origin": "https://www.personality-database.com",
  "Cookie": "session=your_session_cookie_here"
}
```

### Rate Limiting
- Default delay: 1000ms between requests
- Configurable via the dashboard
- Respects website rate limits

## API Endpoints

- `POST /api/scraper/start` - Start scraping process
- `POST /api/scraper/stop` - Stop scraping process  
- `GET /api/scraper/status/:id` - Get scraper status
- `GET /api/scraper/progress/:id` - SSE progress updates
- `POST /api/scraper/profile` - Scrape specific profile
- `GET /api/scraper/available` - Check available scrapers
- `GET /api/data/export` - Export data

## Browser Compatibility

- **Playwright**: Recommended, more reliable, better performance
- **Selenium**: Fallback option, requires Chrome/Chromium installed
- Automatic fallback if primary scraper fails

## Troubleshooting

### Scraper Issues
- Ensure Playwright/Selenium dependencies are installed
- Check that Chrome/Chromium is available for Selenium
- Verify network connectivity to personality-database.com
- Check browser console for JavaScript errors

### Data Loading Issues  
- Verify parquet files exist in `../bot_store/`
- Check file permissions
- Ensure DuckDB WASM loads properly (check browser console)

### Performance
- Large datasets may take time to load initially
- Consider using pagination for better performance
- Query builder has result limits to prevent browser crashes

## Integration with Existing Pipeline

This viewer integrates with the existing socionics research pipeline:
- Uses the same data format as the existing `bot/pdb_cli.py` tool
- Compatible with existing parquet files and FAISS indices
- Can invoke existing scraping processes
- Maintains data consistency with the main pipeline

## Development

### File Structure
```
data/viewer/
├── index.html          # Main dashboard HTML
├── main.js            # Main application logic
├── style.css          # Styles
├── duckdb-loader.js   # DuckDB integration
├── scraper-client.js  # Frontend scraper interface
├── server.js          # Express API server
├── package.json       # Dependencies
├── vite.config.js     # Vite configuration
└── scraper/
    ├── index.js           # Scraper manager
    ├── playwright-scraper.js  # Playwright implementation
    └── selenium-scraper.js    # Selenium implementation
```

### Contributing
- Follow existing code style
- Test scraping with both Playwright and Selenium
- Ensure data compatibility with existing pipeline
- Add error handling for edge cases