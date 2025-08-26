# IPDB - Enhanced Integrated Personality Database

## Overview

The Enhanced IPDB (Integrated Personality Database) is a comprehensive system for socionics research with advanced features including **DuckDB integration**, **vector similarity search**, **multi-language APIs**, and **WebAssembly support**. This system enables systematic collection, analysis, and querying of personality data with high performance and cross-platform compatibility.

## 🚀 New Enhanced Features

### Multi-Database Support
- **DuckDB Integration**: High-performance analytical database with native Parquet support
- **SQLite Fallback**: Reliable embedded database for local development
- **Automatic Detection**: Seamlessly switches based on availability

### Vector Search Capabilities
- **hnswlib Integration**: Fast approximate nearest neighbor search
- **Cosine Similarity**: Optimized for personality type similarity
- **Batch Processing**: Efficient similarity calculations
- **WebAssembly Support**: Client-side vector search in browsers

### Multi-Language API
- **Python**: Full database manager with NumPy integration
- **TypeScript/JavaScript**: Complete API with Node.js and browser support
- **REST API**: Cross-language HTTP endpoints
- **WebAssembly**: Browser-compatible WASM modules

### Performance Enhancements
- **Native Parquet**: Direct Parquet file reading with DuckDB
- **Streaming Import**: Memory-efficient data loading
- **Indexed Search**: Optimized queries and filtering
- **Concurrent Processing**: Multi-threaded operations

## 📦 Installation

### Prerequisites

```bash
# Install Node.js dependencies
npm install

# Install Python dependencies (optional)
pip install duckdb hnswlib numpy pandas requests

# For development
npm install -g typescript ts-node
```

### Package Installation

```bash
# Install the IPDB package
npm install socionics-research-ipdb

# Or clone and build locally
git clone <repository>
cd socionics_research
npm install
npm run build
```

## 🎯 Quick Start

### TypeScript/JavaScript Usage

```typescript
import { IPDBManagerJS } from 'socionics-research-ipdb';

// Initialize with DuckDB (auto-detected)
const db = new IPDBManagerJS('./my_database.db');
await db.initialize();

// Import data from Parquet
await db.importFromParquet('./data/pdb_profiles_normalized.parquet');

// Create entity and add embedding
const entity = await db.createEntity({
  id: '',
  name: 'Hermione Granger',
  entity_type: 'fictional_character',
  description: 'Brilliant witch from Harry Potter'
});

// Add vector embedding for similarity search
const embedding = new Float32Array(384); // Your embedding here
await db.addEmbedding(entity.id, embedding);

// Vector similarity search
const results = await db.vectorSearch(queryEmbedding, 10);
console.log('Similar entities:', results);
```

### Python Integration

```python
from ipdb.python_api import IPDBPythonClient, IPDBNumpyIntegration
import numpy as np

# Connect to API
client = IPDBPythonClient()

# Create entity
entity = client.create_entity(
    name="Albert Einstein",
    entity_type="person", 
    description="Theoretical physicist"
)

# Generate and add embedding
numpy_helper = IPDBNumpyIntegration()
embedding = numpy_helper.generate_embedding("brilliant physicist")
client.add_entity_embedding(entity['id'], embedding)

# Vector search
results = client.vector_search(embedding, k=5)
print(f"Found {len(results)} similar entities")
```

### WebAssembly Browser Usage

```html
<!DOCTYPE html>
<html>
<head>
    <script src="/api/sdk/browser.js"></script>
</head>
<body>
    <script>
        async function runExample() {
            const client = new IPDBClient('/api');
            
            // Get entities
            const entities = await client.getEntities({limit: 10});
            console.log('Entities:', entities);
            
            // Vector search
            const embedding = new Float32Array(384);
            const results = await client.vectorSearch(embedding, 5);
            console.log('Search results:', results);
        }
        
        runExample();
    </script>
</body>
</html>
```

## 🌐 REST API Server

Start the multi-language REST API server:

```bash
# Start the API server
npm run demo  # Runs TypeScript demo with API server
# or
node ipdb/api.js

# API available at http://localhost:3001
```

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check and capabilities |
| `GET` | `/api/info` | API information and documentation |
| `GET` | `/api/entities` | List entities with pagination |
| `POST` | `/api/entities` | Create new entity |
| `GET` | `/api/entities/:id/typings` | Get entity personality typings |
| `POST` | `/api/entities/:id/embeddings` | Add vector embedding |
| `POST` | `/api/search/vector` | Vector similarity search |
| `POST` | `/api/search/text` | Text-based entity search |
| `POST` | `/api/import/parquet` | Import Parquet files |
| `POST` | `/api/users` | Create user account |
| `GET` | `/api/sdk/browser.js` | Browser SDK download |

## Database Architecture

### Core Entity Tables

#### `entities`
Stores information about entities that can be personality typed:
- **Purpose**: Central registry of all persons, fictional characters, public figures
- **Key fields**: `id`, `name`, `entity_type`, `source`, `external_id`
- **Supports**: Multiple entity types (person, fictional_character, public_figure)

#### `users` 
Manages user accounts for raters and annotators:
- **Purpose**: User authentication and role management
- **Key fields**: `id`, `username`, `role`, `experience_level`, `qualifications`
- **Roles**: annotator, panel_rater, adjudicator, admin

### Personality Typing System

#### `personality_systems`
Defines supported typing systems:
- **Purpose**: Registry of different personality theories (Socionics, MBTI, etc.)
- **Pre-populated with**: Socionics, MBTI, Big Five, Enneagram

#### `personality_types`
Individual types within each system:
- **Purpose**: All possible type codes (ILE, INTJ, RCOEI, etc.)
- **Relationship**: Many-to-one with personality_systems

#### `personality_functions`
Functions within systems (for detailed analysis):
- **Purpose**: Socionics functions (Ne, Ti, Fe, etc.) and their confidence scores
- **Use case**: Detailed function-level analysis and typing rationale

### Rating and Typing Management

#### `rating_sessions`
Organized typing activities:
- **Purpose**: Group related typing activities into sessions
- **Types**: individual, panel, consensus
- **Methodologies**: structured_interview, video_analysis, text_analysis, composite_review

#### `typing_judgments`
Core table for personality type assignments:
- **Purpose**: Store individual rater's typing decisions
- **Key fields**: `entity_id`, `rater_id`, `system_id`, `type_id`, `confidence`
- **Supports**: Notes, rationale, methodology tracking

#### `type_probability_distributions`
Probability distributions across types:
- **Purpose**: When raters give probability distributions instead of single types
- **Use case**: Uncertainty quantification and soft typings

#### `function_confidence_scores`
Function-level confidence ratings:
- **Purpose**: Detailed Socionics function analysis
- **Use case**: Fine-grained typing rationale and model validation

### Consensus and Reliability

#### `consensus_sessions`
Formal consensus meetings:
- **Purpose**: Resolve disagreements between raters
- **Outcomes**: consensus_reached, majority_decision, no_consensus
- **Tracks**: Moderator, participants, final decisions

#### `reliability_metrics`
Inter-rater reliability calculations:
- **Purpose**: Track agreement metrics over time
- **Metrics**: Krippendorff's alpha, Fleiss' kappa, ICC
- **Granularity**: Per entity, system, or session

### Data Integration

#### `data_sources`
External data source registry:
- **Purpose**: Track integration with Parquet files and other data sources
- **Supports**: parquet, json, csv, api sources

#### `entity_data_mappings`
Link entities to external data:
- **Purpose**: Connect internal entities to external IDs (CIDs, PIDs)
- **Use case**: Traceability and data lineage

## Usage Examples

### Python API

```python
from ipdb.database_manager import IPDBManager, UserRole, ExperienceLevel

# Initialize database
db = IPDBManager("/path/to/database.db")
db.initialize_database()

# Import existing PDB data
db.import_pdb_data("data/bot_store/pdb_profiles_normalized.parquet")

# Create users
admin = db.create_user("admin", "admin@example.com", UserRole.ADMIN)
rater1 = db.create_user("rater1", "rater1@example.com", UserRole.PANEL_RATER, ExperienceLevel.EXPERT)
rater2 = db.create_user("rater2", "rater2@example.com", UserRole.PANEL_RATER, ExperienceLevel.INTERMEDIATE)

# Create a rating session
session_id = db.create_rating_session(
    name="Fictional Characters Typing Session #1",
    description="Panel typing of fictional characters from literature",
    methodology="composite_review",
    session_type="panel",
    created_by=admin.id
)

# Get entities to rate
entities = db.get_entities(limit=50)
print(f"Found {len(entities)} entities available for rating")

# View typing summary for an entity
if entities:
    entity_id = entities[0]['id']
    summary = db.get_typing_summary(entity_id)
    print(f"Current typings for {entities[0]['name']}:")
    for s in summary:
        print(f"  {s['system_display']}: {s['type_code']} (confidence: {s['avg_confidence']:.2f})")
```

### SQL Queries

```sql
-- Get entities that need more ratings
SELECT e.name, e.entity_type, COUNT(tj.id) as rating_count
FROM entities e
LEFT JOIN typing_judgments tj ON e.id = tj.entity_id
GROUP BY e.id, e.name, e.entity_type
HAVING rating_count < 3
ORDER BY rating_count, e.name;

-- Calculate inter-rater agreement for a specific system
SELECT 
    ps.display_name as system,
    pt.code as type,
    COUNT(DISTINCT tj.rater_id) as rater_count,
    COUNT(tj.id) as total_judgments,
    AVG(tj.confidence) as avg_confidence
FROM typing_judgments tj
JOIN personality_systems ps ON tj.system_id = ps.id
LEFT JOIN personality_types pt ON tj.type_id = pt.id
WHERE ps.name = 'socionics'
GROUP BY ps.id, pt.id
HAVING rater_count > 1
ORDER BY total_judgments DESC;

-- Find entities with high disagreement (need consensus)
SELECT 
    e.name,
    e.id,
    ps.display_name as system,
    COUNT(DISTINCT tj.type_id) as different_types_assigned,
    COUNT(tj.id) as total_judgments,
    STDEV(tj.confidence) as confidence_std
FROM entities e
JOIN typing_judgments tj ON e.id = tj.entity_id
JOIN personality_systems ps ON tj.system_id = ps.id
WHERE tj.type_id IS NOT NULL
GROUP BY e.id, e.name, ps.id, ps.display_name
HAVING different_types_assigned > 2 OR confidence_std > 0.3
ORDER BY different_types_assigned DESC, confidence_std DESC;
```

## Integration with Existing Data

The schema seamlessly integrates with existing Parquet files:

### PDB Profiles Integration

1. **Normalized Profiles** (`pdb_profiles_normalized.parquet`):
   - Each row becomes an `entity` record
   - CID values stored in `entity_data_mappings`
   - Existing type assignments imported as `typing_judgments`

2. **Profile Vectors** (`pdb_profile_vectors.parquet`):
   - Vector embeddings can be stored as metadata
   - Linked via CID mapping for semantic search

3. **Raw Profiles** (`pdb_profiles.parquet`):
   - Full JSON payloads preserved in metadata
   - Enables rich analysis and re-processing

### Migration Process

```python
# Complete migration example
db = IPDBManager("socionics_research.db")
db.initialize_database()

# Import PDB data
db.import_pdb_data(
    "data/bot_store/pdb_profiles_normalized.parquet",
    "data/bot_store/pdb_profile_vectors.parquet"
)

# Import creates:
# - 3,537+ entity records
# - Data source tracking
# - CID mappings for traceability
# - Existing type assignments as judgments
```

## Research Workflow Support

### 1. Entity Preparation
- Import entities from various sources
- Enrich with metadata and descriptions
- Establish data lineage through mappings

### 2. User Management
- Create user accounts with appropriate roles
- Track qualifications and experience levels
- Manage access controls

### 3. Rating Session Design
- Define methodology and session type
- Select entities for rating
- Assign raters and set parameters

### 4. Data Collection
- Raters make typing judgments
- System tracks confidence, rationale, timing
- Support for probability distributions

### 5. Quality Assurance
- Calculate inter-rater reliability metrics
- Identify entities needing consensus
- Track rater performance over time

### 6. Consensus Process
- Formal consensus sessions for disagreements
- Track discussion outcomes
- Final type assignments with audit trail

## Behavioral Indicators Extension

The schema includes support for detailed behavioral coding:

### `behavioral_indicators`
Registry of observable behaviors:
- Discourse patterns (interruption rate, topic shifts)
- Prosodic features (speech rate, pitch variation)  
- Lexical patterns (abstract vs concrete language)
- Nonverbal behaviors (emotional expression)

### `behavioral_annotations`
Link entities to behavioral observations:
- Quantified ratings on behavioral dimensions
- Confidence tracking for each annotation
- Support for multiple raters per indicator

## Scalability and Performance

### Indexing Strategy
- Primary keys on all core tables
- Foreign key indexes for joins
- Composite indexes on frequently queried combinations
- Full-text search support for names and descriptions

### Partitioning Considerations
For large-scale deployments:
- Partition `typing_judgments` by date or session
- Separate active vs historical data
- Archive completed sessions periodically

### Query Optimization
- Views for common aggregations
- Materialized summaries for dashboard queries
- Efficient reliability calculation procedures

## Security and Privacy

### Data Protection
- User authentication and authorization
- Role-based access control
- Audit logging of all modifications
- De-identification options for sensitive data

### Consent Management
- Integration points for consent tracking
- Data retention policies
- Right-to-deletion support

## Future Enhancements

### Planned Features
- Real-time collaboration support
- Advanced statistical analysis functions
- Machine learning model integration
- API endpoints for web applications
- Mobile-friendly interfaces

### Extensibility
- JSON metadata fields for flexibility
- Plugin architecture for custom metrics
- Integration hooks for external systems
- Configurable workflow states

## Support and Documentation

For questions and support:
1. Review this documentation
2. Check the example code in `database_manager.py`
3. Examine test cases for usage patterns
4. Consult the research team for methodology questions

## Version History

- **v1.0** (2025-01): Initial schema design with core functionality
- **v1.1** (planned): Enhanced reliability metrics and consensus support
- **v2.0** (planned): Real-time collaboration and API endpoints