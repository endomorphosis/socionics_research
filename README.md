# IPDB - Integrated Personality Database

A comprehensive Node.js-based personality database platform with wikia-style collaborative interface for socionics research.

## 🚀 Pure Node.js Architecture

This platform is built entirely in Node.js with no Python dependencies, providing:

- **Personality Database**: Complete character database with MBTI, Socionics, and Enneagram typing
- **Wikia-Style Interface**: Community-driven collaborative platform  
- **Real-Time Data**: Live statistics and interactive browsing
- **Advanced Search**: Taxonomy-based organization and filtering
- **Comparison Tools**: Head-to-head and panel-view character analysis
- **Rating System**: Community voting and confidence tracking

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/endomorphosis/socionics_research.git
cd socionics_research

# Install dependencies
npm install

# Build the database (ingest parquet data)
npm run build

# Start the development server
npm run dev
```

## 🛠️ Available Scripts

```bash
npm run dev      # Start development server with auto-reload
npm run build    # Process parquet data and build database
npm run serve    # Start production server
npm run start    # Start server (alias for serve)
npm run ingest   # Re-import parquet data to database
npm test         # Run test suite
```

## 🌐 Usage

### Quick Start

```bash
# Start the server
npm start

# Open in browser
http://localhost:3000/app
```

### Development Mode

```bash
# Start with auto-reload
npm run dev

# Make changes to files and server will restart automatically
```

### Database Operations

```bash
# Import fresh data from parquet files
npm run build

# Re-ingest data if needed
npm run ingest
```

## 🏗️ Architecture Overview

### Backend Components

- **`wikia_server.cjs`** - Express.js server with REST API
- **`database-manager.cjs`** - SQLite database manager
- **`parquet_reader_nodejs.cjs`** - Pure Node.js parquet parser
- **`ingest_parquet_data.cjs`** - Data ingestion pipeline

### Frontend Features

- **Dashboard** - System statistics and overview
- **Browse Characters** - Searchable character database
- **Head-to-Head** - Compare two characters
- **Panel View** - Compare four characters simultaneously  
- **Upload Pictures** - Image management for characters
- **Rating System** - Community personality type voting

### Database Schema

```sql
-- Entities (characters, people, etc.)
entities: id, name, description, entity_type, source, metadata

-- Personality ratings and votes
ratings: id, entity_id, rater_id, system_name, type_code, confidence

-- User management
users: id, username, display_name, role, experience_level

-- System metadata
personality_systems: id, name, description, type_count
personality_types: id, system_id, code, name, description
```

## 🔄 Data Pipeline

```
Parquet Files → Node.js Parser → SQLite Database → Web Interface
     ↓              ↓               ↓              ↓
3,917 records → JSON extraction → SQL storage → Live statistics
```

### Data Sources

- **`data/bot_store/pdb_profiles.parquet`** - Main character database
- **`data/bot_store/pdb_profiles_flat.csv`** - CSV fallback format
- **Real-time user ratings** - Community contributions

## 📊 API Endpoints

### Character Data
```bash
GET  /api/stats          # System statistics
GET  /api/entities       # List all entities
GET  /api/entities/:id   # Get specific entity
POST /api/entities       # Create new entity
PUT  /api/entities/:id   # Update entity
```

### Search & Browse
```bash
GET  /api/search?q=term    # Search characters
GET  /api/filter           # Filter by category/type
GET  /api/compare          # Character comparison data
```

### Personality Systems
```bash
GET  /api/personality-systems     # List typing systems
GET  /api/personality-types       # List personality types  
POST /api/ratings                 # Submit personality rating
GET  /api/ratings/:entityId       # Get entity ratings
```

### Health & Status
```bash
GET  /health            # Server health check
GET  /api/info          # API information
GET  /api/status        # Database status
```

## 🎯 Key Features

### 1. Collaborative Character Sheets
- Real-time editing of character information
- Version history and change tracking
- Multi-user collaborative editing
- Role-based access control

### 2. Advanced Search & Organization
- Real-time search across names, descriptions, types
- Taxonomy filtering (Anime, Movies, TV Shows, Books, Games, Comics)
- Sort by popularity, rating, recent updates
- Visual filter management with active filter tags

### 3. Interactive Browsing
- **Grid View**: Card-based character browsing
- **List View**: Compact character listings
- **Table View**: Structured data presentation
- Pagination for large datasets

### 4. Personality Comparison Tools
- **Head-to-Head**: Detailed two-character analysis
- **Panel View**: Four-character group dynamics
- Side-by-side personality type comparison
- Confidence level visualization

### 5. Community Features
- User rating system with confidence levels
- Comment threads for character discussions
- Image upload and character association
- Contributor leaderboards and statistics

## 🔧 Configuration

### Server Configuration
```javascript
// Default configuration
const config = {
    port: 3000,
    database: './data/ipdb.db',
    cors: true,
    logging: true,
    rateLimit: true
};

// Environment variables
PORT=3000
NODE_ENV=production
DATABASE_PATH=./data/ipdb.db
```

### Database Configuration
```javascript
// SQLite settings
const dbConfig = {
    filename: './data/ipdb.db',
    timeout: 5000,
    busyTimeout: 30000,
    synchronous: 'NORMAL',
    journalMode: 'WAL'
};
```

## 📈 Performance

### Database Performance
- **SQLite**: Handles 100k+ character records efficiently
- **Indexing**: Optimized queries for search and filtering
- **Caching**: In-memory cache for frequent queries
- **Connection pooling**: Efficient database connections

### Frontend Performance  
- **Lazy loading**: Characters loaded on-demand
- **Pagination**: Efficient large dataset handling
- **Client-side caching**: Reduced API calls
- **Optimized rendering**: Fast UI updates

## 🛡️ Security Features

- **Input validation**: All API inputs sanitized
- **SQL injection protection**: Parameterized queries
- **XSS prevention**: Content sanitization
- **Rate limiting**: API abuse prevention
- **CORS configuration**: Controlled cross-origin access

## 🧪 Testing

```bash
# Run all tests
npm test

# Test database connectivity
node ipdb/database-manager.cjs

# Test data ingestion
node ipdb/ingest_parquet_data.cjs

# Test parquet reader
node ipdb/parquet_reader_nodejs.cjs
```

## 🚀 Deployment

### Production Deployment
```bash
# Install dependencies
npm install --production

# Build database
npm run build

# Start production server
npm run serve
```

### Docker Deployment
```dockerfile
FROM node:18
WORKDIR /app
COPY package*.json ./
RUN npm ci --production
COPY . .
RUN npm run build
EXPOSE 3000
CMD ["npm", "start"]
```

### Environment Setup
```bash
# Production environment
NODE_ENV=production
PORT=3000
DATABASE_PATH=/data/ipdb.db

# Development environment  
NODE_ENV=development
PORT=3000
DATABASE_PATH=./data/ipdb.db
DEBUG=true
```

## 📚 Contributing

### Development Setup
```bash
# Fork and clone
git clone https://github.com/your-username/socionics_research.git
cd socionics_research

# Install dev dependencies
npm install

# Start development server
npm run dev

# Make changes and test
npm test
```

### Code Style
- Use Node.js CommonJS modules (`.cjs`)
- Follow ESLint configuration
- Add JSDoc comments for functions
- Test all changes before committing

### Adding Features
1. **Database changes**: Update schema in `database-manager.cjs`
2. **API endpoints**: Add routes to `wikia_server.cjs`
3. **Frontend**: Modify HTML/CSS/JS in server templates
4. **Data processing**: Extend `parquet_reader_nodejs.cjs`

## 📄 License

MIT License - see LICENSE file for details.

## 🤝 Support

For questions, issues, or contributions:
- Open an issue on GitHub
- Submit a pull request
- Contact the maintainer team

---

**Built with ❤️ for the socionics research community**