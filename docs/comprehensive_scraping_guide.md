# Comprehensive Personality Database Scraping

This guide explains how to use the enhanced scraping system to collect tens of thousands of personality profiles from the Personality Database (personality-database.com) systematically.

## Overview

The comprehensive scraping system combines multiple strategies to maximize personality profile collection:

1. **Systematic Keyword Search** - Uses extensive keyword lists covering franchises, characters, names, and personality types
2. **Iterative Relationship Expansion** - Follows profile relationships to discover connected personalities  
3. **Intelligent Filtering** - Focuses on character profiles while maintaining broad coverage
4. **State Persistence** - Resumes from interruptions without losing progress
5. **Progress Monitoring** - Tracks progress towards the goal of 10,000+ profiles

## Quick Start

### 1. Setup Authentication

First, set up authentication headers from an active Personality Database session:

```bash
# Create secrets directory
mkdir -p .secrets

# Copy template and edit with real browser headers
cp data/bot_store/headers.template.json .secrets/pdb_headers.json

# Edit the file with headers from your browser's developer tools
# Include: User-Agent, Cookie, Accept, Accept-Language, Origin, Referer
```

### 2. Launch Comprehensive Scraping

Use the interactive launcher for guided setup:

```bash
./scripts/launch_comprehensive_scrape.sh
```

Or run directly with the comprehensive script:

```bash
./scripts/comprehensive_personality_scrape.sh
```

### 3. Monitor Progress

Check scraping progress and statistics:

```bash
./scripts/scrape_progress_monitor.sh
```

## Scraping Strategies

### Quick Start (Recommended for initial collection)
- **Duration**: 2-4 hours
- **Resources**: Moderate (90 RPM, 3 concurrent)
- **Coverage**: Popular franchises and character-rich content
- **Best for**: Getting started, building foundation dataset

```bash
RPM=90 CONCURRENCY=3 PAGES=3 SWEEP_PAGES=15 ./scripts/comprehensive_personality_scrape.sh
```

### Intensive (Maximum coverage)  
- **Duration**: 4-8 hours
- **Resources**: High (120 RPM, 5 concurrent)
- **Coverage**: Comprehensive keyword expansion, deep relationship traversal
- **Best for**: Reaching 10,000+ profiles quickly

```bash
RPM=120 CONCURRENCY=5 PAGES=5 SWEEP_PAGES=25 INITIAL_FRONTIER_SIZE=3000 ./scripts/comprehensive_personality_scrape.sh
```

### Conservative (Rate-limit safe)
- **Duration**: 8-12 hours  
- **Resources**: Low (60 RPM, 2 concurrent)
- **Coverage**: Careful, thorough scraping
- **Best for**: Avoiding rate limits, long-term collection

```bash
RPM=60 CONCURRENCY=2 PAGES=2 SWEEP_PAGES=10 ./scripts/comprehensive_personality_scrape.sh
```

## Key Features

### Multi-Phase Collection

1. **Phase 1: Systematic Keyword Search**
   - Processes 300+ base keywords plus 200+ comprehensive keywords
   - Searches franchises, character names, personality types
   - Expands character-rich subcategories and boards
   - Follows hint terms for discovery

2. **Phase 2: Iterative Relationship Expansion**  
   - Uses discovered profiles as seeds for BFS expansion
   - Fetches related profiles for each discovered ID
   - Performs name-based searches on related items
   - Sweeps a-z, 0-9 queries for broad coverage

3. **Phase 3: Coverage Analysis**
   - Generates detailed progress reports
   - Exports normalized data for analysis
   - Creates character-focused datasets
   - Builds optimized search indices

### Intelligent Filtering

- **Character Focus**: Prioritizes `isCharacter=true` profiles
- **Franchise Expansion**: Discovers characters from popular media
- **Relationship Networks**: Maps connections between profiles  
- **Quality Filtering**: Removes duplicates via content addressing

### State Management

- **Persistent State**: Survives interruptions and restarts
- **Progress Tracking**: Detailed logging of collection phases
- **Resume Capability**: Continues from last successful state
- **Cache Optimization**: Reduces redundant API calls

## Configuration Options

### Environment Variables

```bash
# Rate limiting
RPM=120                          # Requests per minute
CONCURRENCY=4                    # Parallel requests
TIMEOUT=30                       # HTTP timeout seconds

# Search strategy  
PAGES=5                         # Pages per keyword search
SWEEP_PAGES=30                  # Pages per sweep query (a-z, 0-9)
MAX_NO_PROGRESS_PAGES=5         # Stop after N no-progress pages
INITIAL_FRONTIER_SIZE=2000      # Initial seed size for expansion

# Feature toggles
USE_FRANCHISE_EXPANSION=1       # Expand franchise keywords
USE_CHARACTER_FILTERING=1       # Focus on character profiles

# File paths
KEYWORDS_FILE=data/bot_store/large_keywords.txt
ADDITIONAL_KEYWORDS_FILE=data/bot_store/comprehensive_keywords.txt
HEADERS_FILE=.secrets/pdb_headers.json
```

### Advanced Configuration

For custom setups, directly configure the underlying CLI commands:

```bash
# Custom keyword search
pdb-cli search-keywords \
  --query-file your_keywords.txt \
  --limit 40 --pages 5 --until-empty \
  --filter-characters --expand-subcategories \
  --auto-embed --auto-index

# Custom relationship expansion  
pdb-cli scan-all \
  --max-iterations 0 --search-names \
  --sweep-queries "a,b,c,d,e" --sweep-until-empty \
  --scrape-v1 --use-state
```

## Monitoring and Analysis

### Progress Monitoring

```bash
# Detailed progress report
./scripts/scrape_progress_monitor.sh

# Quick coverage check
pdb-cli coverage --sample 15

# Real-time log monitoring
tail -f data/bot_store/comprehensive_scrape_progress.log
```

### Data Analysis

```bash
# Search collected profiles
pdb-cli search-faiss "curious detective" --top 10

# Export character profiles
pdb-cli export-characters --sample 20

# Generate normalized dataset
pdb-cli export --out profiles_normalized_$(date +%Y%m%d).parquet

# Analyze profile relationships
pdb-cli edges-analyze --top 5
```

## Keyword Management

### Adding New Keywords

Add high-value keywords to maximize discovery:

```bash
# Edit comprehensive keywords file
nano data/bot_store/comprehensive_keywords.txt

# Add franchise-specific terms:
# - Popular TV shows, movies, games
# - Character archetypes and roles  
# - Cultural references and mythology
# - International names and terms
```

### Keyword Strategy Tips

**High-yield keyword types**:
- Popular franchises (Marvel, DC, Harry Potter, etc.)
- Character archetypes (hero, villain, detective, etc.)  
- Media properties with rich character development
- Mythology and folklore characters
- Common names that might match character names

**Discovery techniques**:
- Monitor trending queries: `pdb-cli hot-queries`
- Analyze successful searches: `pdb-cli diagnose-query --contains "keyword"`
- Extract hints from collected profiles: `--chase-hints`

## Troubleshooting

### Authentication Issues

```bash
# Test authentication
pdb-cli auth-check --keyword "harry potter"

# Update headers if needed
cp .secrets/pdb_headers.json .secrets/pdb_headers.json.backup
# Get new headers from browser and update file
```

### Rate Limiting

```bash
# Reduce request rate
RPM=60 CONCURRENCY=2 ./scripts/comprehensive_personality_scrape.sh

# Enable caching to reduce duplicate requests
export PDB_CACHE=1
```

### Low Discovery Rate

```bash
# Check keyword effectiveness
pdb-cli diagnose-query --contains "keyword_name"

# Add more specific keywords
nano data/bot_store/comprehensive_keywords.txt

# Increase search depth
PAGES=10 SWEEP_PAGES=50 ./scripts/comprehensive_personality_scrape.sh
```

### Data Quality Issues

```bash
# Compact and deduplicate
pdb-cli compact-raw --replace

# Focus on characters only
USE_CHARACTER_FILTERING=1 ./scripts/comprehensive_personality_scrape.sh

# Rebuild search index
pdb-cli index --out data/bot_store/pdb_faiss.index
```

## Expected Results

With proper configuration and sufficient run time, the comprehensive scraping system should achieve:

- **10,000+ unique profiles** (target goal)
- **Character-rich dataset** focused on personality profiles
- **Relationship networks** mapping connections between profiles
- **Search capabilities** for discovering relevant profiles
- **Normalized data** ready for research and analysis

### Performance Benchmarks

| Strategy | Duration | Expected Profiles | Resource Usage |
|----------|----------|-------------------|----------------|
| Quick    | 2-4 hrs  | 5,000-8,000      | Moderate       |
| Intensive| 4-8 hrs  | 10,000-15,000    | High           |
| Extended | 12+ hrs  | 15,000-25,000    | Variable       |

*Results depend on authentication status, keyword effectiveness, and API availability*

## Integration with Research Pipeline

The collected personality profiles integrate with the broader socionics research pipeline:

```bash
# Generate embeddings for semantic search
pdb-cli embed --force

# Export for statistical analysis
pdb-cli export --out research_dataset.parquet

# Analyze type distributions  
pdb-cli analyze --method kl-divergence

# Build relationship networks
pdb-cli edges-export --out network_analysis.parquet
```

This comprehensive approach ensures maximum personality profile collection while maintaining data quality and research utility.