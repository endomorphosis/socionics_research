# Personality Database Collector

A comprehensive data collection and analysis system for personality database information with IPFS CID-based storage, vector search capabilities, and parquet file storage.

## Overview

This system collects personality typing data from personality-database.com, stores it efficiently in parquet files using IPFS Content Identifiers (CIDs) as primary keys, and provides semantic search capabilities through vector embeddings.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Data Collection Pipeline                      │
├─────────────────────────────────────────────────────────────────┤
│ API Client → Data Models → Storage Layer → Vector Search         │
│     ↓             ↓            ↓              ↓                 │
│ Rate-limited   Structured   Parquet +     FAISS/Numpy           │
│ Pagination     Validation   IPFS CIDs     Embeddings            │
└─────────────────────────────────────────────────────────────────┘

Storage Structure:
├── persons/           # Character/person profiles
├── ratings/           # Personality type ratings  
├── profiles/          # Complete profiles (person + ratings)
└── vectors/           # Vector embeddings for search
```

## Key Features

### 🔑 IPFS CID Primary Keys
- Every data record gets a unique Content Identifier (CID) based on content
- Enables consistent data deduplication and joining across tables
- Content-addressable storage ensures data integrity

### 📊 Parquet Storage
- Efficient columnar storage format
- Excellent compression and query performance
- Schema evolution support
- Compatible with big data tools (Spark, Dask, etc.)

### 🔍 Vector Search
- Semantic search using sentence transformers
- FAISS integration for high-performance similarity search
- Support for finding similar personalities and types
- Cosine similarity scoring

### 🌐 API Integration
- Rate-limited client respecting API constraints
- Automatic pagination handling
- Comprehensive error handling and retries
- Support for multiple endpoint types

### ⚡ CLI Interface
- Simple commands for data collection and management
- Built-in statistics and monitoring
- Search interface for exploring collected data

## Installation

### Prerequisites
- Python 3.11+
- Required packages: pandas, pyarrow, requests, numpy
- Optional: sentence-transformers, faiss-cpu (for advanced features)

### Setup
```bash
# Clone the repository
git clone <repository-url>
cd socionics_research

# Install dependencies
pip install pandas pyarrow requests numpy base58

# Optional: Install ML packages for full functionality  
pip install sentence-transformers faiss-cpu
```

## Quick Start

### 1. Run the Demo
```bash
python demo.py
```

This demonstrates the complete workflow:
- Creates sample personality data
- Stores in parquet with IPFS CIDs
- Generates vector embeddings
- Performs semantic searches
- Shows data joining capabilities

### 2. Collect Real Data
```bash
# Collect profiles from personality-database.com
python personality_db_collector/cli.py collect --max-profiles 1000

# Generate embeddings for search
python personality_db_collector/cli.py embed

# Search the data
python personality_db_collector/cli.py search "logical detective personality"

# View statistics
python personality_db_collector/cli.py stats
```

## Data Models

### PersonData
Represents a character or person from the database:
```python
@dataclass
class PersonData:
    id: int
    name: str
    description: Optional[str] = None
    category_name: Optional[str] = None  # e.g., "TV Shows", "Literature"
    ipfs_cid: Optional[str] = None       # Auto-generated primary key
```

### RatingData
Personality type ratings for persons:
```python
@dataclass
class RatingData:
    person_id: int
    personality_type: str        # e.g., "ILE", "ENTP"
    personality_system: str      # e.g., "socionics", "mbti"
    confidence: Optional[float]  # Rating confidence score
    votes_count: Optional[int]   # Number of votes
    ipfs_cid: Optional[str] = None
```

### ProfileData
Complete profile combining person and ratings:
```python
@dataclass 
class ProfileData:
    person: PersonData
    ratings: List[RatingData]
    socionics_ratings: List[RatingData]  # Filtered by system
    mbti_ratings: List[RatingData]
    ipfs_cid: Optional[str] = None
```

### VectorData
Vector embeddings for semantic search:
```python
@dataclass
class VectorData:
    ipfs_cid: str              # Links to original data
    vector: List[float]        # Embedding vector
    vector_model: str          # Model used for embedding
    source_text: str           # Text that was embedded
    source_type: str           # e.g., "person_profile", "rating"
```

## API Usage

### Basic Collection
```python
from personality_db_collector import PersonalityDBClient, ParquetStorage

# Initialize components
client = PersonalityDBClient(rate_limit_delay=0.12)  # ~500 req/min
storage = ParquetStorage("./data")

# Collect data
profiles = []
for batch in client.get_all_profiles(max_profiles=100):
    for raw_profile in batch:
        person = client.convert_raw_to_person_data(raw_profile)
        # Process and store...
```

### Storage Operations
```python
# Store data with automatic CID generation
person_cids = storage.store_persons(persons)
rating_cids = storage.store_ratings(ratings)

# Load data by CIDs
specific_persons = storage.load_persons(cids=["QmAbc123..."])
all_ratings = storage.load_ratings()

# Get statistics
stats = storage.get_storage_stats()
print(f"Stored {stats['persons']['count']} persons")
```

### Vector Search
```python
from personality_db_collector import VectorSearchEngine

# Initialize search engine
search_engine = VectorSearchEngine(storage)

# Generate embeddings
persons_df = storage.load_persons()
person_vectors = search_engine.embed_persons(persons_df)
storage.store_vectors(person_vectors)

# Build search index
search_engine.build_search_index()

# Search
results = search_engine.search("logical detective personality", top_k=5)
for result in results:
    print(f"Score: {result['score']:.3f} - {result['source_text']}")
```

## CLI Reference

### collect
Collect data from personality-database.com API:
```bash
python personality_db_collector/cli.py collect [OPTIONS]

Options:
  --max-profiles INTEGER    Maximum profiles to collect
  --category-id INTEGER     Filter by category ID  
  --rate-limit FLOAT        Request delay in seconds (default: 0.12)
```

### embed
Generate vector embeddings for stored data:
```bash  
python personality_db_collector/cli.py embed [OPTIONS]

Options:
  --model TEXT             Sentence transformer model (default: all-MiniLM-L6-v2)
```

### search
Search stored data using semantic similarity:
```bash
python personality_db_collector/cli.py search QUERY [OPTIONS]

Options:
  --top-k INTEGER          Number of results (default: 10)
  --model TEXT             Embedding model to use
```

### stats
Display storage and index statistics:
```bash
python personality_db_collector/cli.py stats
```

## Configuration

### Rate Limiting
The API client respects personality-database.com's rate limits:
- Default: 0.12 seconds between requests (~500/minute)
- Configurable via `rate_limit_delay` parameter
- Automatic retry with exponential backoff

### Storage Paths
Default storage structure:
```
./data/personality_db/
├── persons/persons.parquet
├── ratings/ratings.parquet  
├── profiles/profiles.parquet
└── vectors/vectors.parquet
```

Customize via `storage_path` parameter.

### Vector Models
Supported sentence transformer models:
- `all-MiniLM-L6-v2` (default, fast, good quality)
- `all-mpnet-base-v2` (slower, higher quality)
- `all-distilroberta-v1` (balanced)

## Performance Considerations

### Storage
- Parquet files provide excellent compression (typically 5-10x smaller than JSON)
- Columnar format enables efficient querying and filtering
- Automatic schema inference and evolution

### Search
- FAISS index provides sub-millisecond search on 100K+ vectors
- Numpy fallback for environments without FAISS
- Cosine similarity for semantic relevance

### Memory Usage
- Lazy loading of data from parquet files
- Streaming API collection to handle large datasets
- Configurable batch sizes for memory efficiency

## Testing

Run the test suite:
```bash
# Run all tests
python -m pytest personality_db_collector/tests/

# Run specific test class
python -m pytest personality_db_collector/tests/test_storage.py::TestParquetStorage

# Run with coverage
python -m pytest --cov=personality_db_collector
```

Test coverage includes:
- Data model serialization/deserialization
- Storage operations (store, load, dedupe)
- Vector search functionality
- API client error handling
- CLI command execution

## Data Schema

### Parquet Schema
The system automatically generates optimized parquet schemas:

**persons.parquet:**
```
id: int64
name: string
description: string
category_name: string
subcategory_name: string
ipfs_cid: string (primary key)
created_at: timestamp
metadata: string (JSON)
```

**ratings.parquet:**
```
person_id: int64
personality_type: string
personality_system: string
rating_value: string
confidence: double
votes_count: int64
ipfs_cid: string (primary key)
```

**vectors.parquet:**
```
ipfs_cid: string (foreign key)
vector: list<float>
vector_model: string
source_text: string
source_type: string
created_at: timestamp
metadata: string (JSON)
```

### IPFS CID Generation
CIDs are generated using content-based hashing:
1. Serialize data to deterministic JSON
2. Hash with SHA-256
3. Encode as CIDv1 with base32 encoding
4. Result: `bafkreig...` format identifier

Benefits:
- Content deduplication (identical content = same CID)
- Data integrity verification
- Consistent cross-references between tables
- Compatible with IPFS ecosystem

## Troubleshooting

### Common Issues

**sentence-transformers not available:**
- Install with: `pip install sentence-transformers`
- System falls back to mock embeddings for testing

**FAISS not available:**
- Install with: `pip install faiss-cpu`
- System uses numpy-based similarity search

**API rate limiting:**
- Increase `rate_limit_delay` parameter
- Check personality-database.com API status

**Memory issues with large datasets:**
- Reduce `batch_size` in collection
- Process data in chunks
- Use streaming operations

### Debug Mode
Enable verbose logging:
```bash
python personality_db_collector/cli.py --verbose collect
```

Or programmatically:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Extending the System

### Adding New Personality Systems
To support additional systems beyond MBTI/Socionics:

1. Update `RatingData` validation
2. Add filtering logic in `ProfileData`  
3. Update embedding generation for new rating types

### Custom Vector Models
To use custom embedding models:

1. Subclass `VectorSearchEngine`
2. Override `generate_embeddings()` method
3. Ensure consistent dimensions across all vectors

### Additional Data Sources
To collect from other personality databases:

1. Create new API client following `PersonalityDBClient` pattern
2. Implement data conversion to existing models
3. Use same storage and search infrastructure

## Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature-name`
3. Add tests for new functionality
4. Ensure all tests pass: `python -m pytest`
5. Submit pull request with detailed description

## License

[Add appropriate license information]

## Support

For issues and questions:
1. Check existing GitHub issues
2. Review troubleshooting section
3. Create new issue with:
   - System information
   - Error messages
   - Steps to reproduce
   - Expected behavior