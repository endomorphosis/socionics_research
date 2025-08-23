# Comprehensive Personality Database Scraping System

A powerful system for comprehensively scraping the Personality Database to collect tens of thousands of personality profiles using intelligent keyword search, relationship expansion, and automated discovery.

## 🎯 Goal: Collect 10,000+ Personality Profiles

This enhanced scraping system combines multiple strategies to maximize personality profile collection:

- **Systematic Keyword Search** - 500+ curated keywords covering franchises, characters, and personality types
- **Iterative Relationship Expansion** - BFS traversal of profile networks  
- **Intelligent Character Filtering** - Focus on personality-rich character profiles
- **Automated Discovery** - Dynamic keyword identification from existing data
- **State Persistence** - Resume from interruptions without losing progress
- **Progress Monitoring** - Real-time tracking towards collection goals

## 🚀 Quick Start

### 1. Setup Authentication

```bash
# Create authentication headers from active PDB session
mkdir -p .secrets
cp data/bot_store/headers.template.json .secrets/pdb_headers.json
# Edit with your browser's headers (User-Agent, Cookie, etc.)
```

### 2. Run Comprehensive Scraping

**Interactive Mode (Recommended):**
```bash
./scripts/launch_comprehensive_scrape.sh
```

**Direct Execution:**
```bash
# Quick start (2-4 hours, ~5K-8K profiles)
./scripts/comprehensive_personality_scrape.sh

# Intensive mode (4-8 hours, ~10K-15K profiles)  
RPM=120 CONCURRENCY=5 PAGES=5 SWEEP_PAGES=25 ./scripts/comprehensive_personality_scrape.sh

# Conservative mode (8-12 hours, rate-limit safe)
RPM=60 CONCURRENCY=2 ./scripts/comprehensive_personality_scrape.sh
```

### 3. Monitor Progress

```bash
# Detailed progress report
./scripts/scrape_progress_monitor.sh

# Quick coverage check
pdb-cli coverage --sample 15

# Real-time monitoring
tail -f data/bot_store/comprehensive_scrape_progress.log
```

## 📊 Current Status

- **3,917 profiles** already collected
- **39% progress** towards 10,000 goal
- **Search index** available for semantic search
- **Vector embeddings** for 80%+ of profiles

## 🎭 Key Features

### Multi-Phase Collection Strategy

1. **Phase 1: Systematic Keyword Search**
   - 308 base keywords + 200+ comprehensive keywords  
   - Franchise expansion (Marvel, DC, anime, games, etc.)
   - Character archetype targeting (heroes, villains, detectives)
   - Name-based discovery (common first names, international names)

2. **Phase 2: Iterative Relationship Expansion**
   - BFS traversal using collected profiles as seeds
   - Related profile network expansion  
   - Name-based search on discovered characters
   - Alphanumeric sweep (a-z, 0-9) for broad coverage

3. **Phase 3: Coverage Analysis & Optimization**
   - Progress tracking and statistics
   - Character-focused dataset export
   - Search index optimization
   - Relationship network analysis

### Intelligent Automation

- **Character Focus**: Prioritizes `isCharacter=true` profiles
- **Keyword Discovery**: Automatically finds high-value search terms from existing data
- **Relationship Networks**: Maps connections between personality profiles
- **Quality Filtering**: Content-addressed storage prevents duplicates
- **State Management**: Survives interruptions and resumes seamlessly

## 🛠️ Advanced Usage

### Keyword Optimization

```bash
# Discover high-value keywords from existing data
./scripts/optimize_keyword_discovery.sh

# Use discovered keywords for targeted scraping
ADDITIONAL_KEYWORDS_FILE=data/bot_store/discovered_keywords.txt ./scripts/comprehensive_personality_scrape.sh
```

### Custom Configuration

```bash
# High-performance scraping
export RPM=150
export CONCURRENCY=6
export PAGES=10
export SWEEP_PAGES=50
export INITIAL_FRONTIER_SIZE=5000
./scripts/comprehensive_personality_scrape.sh
```

### Data Analysis

```bash
# Search collected profiles
pdb-cli search-faiss "curious detective character" --top 10

# Export character-only dataset
pdb-cli export-characters --sample 25

# Analyze profile relationships
pdb-cli edges-analyze --top 5

# Generate normalized research dataset
pdb-cli export --out research_profiles_$(date +%Y%m%d).parquet
```

## 📈 Performance Benchmarks

| Strategy | Duration | Expected Profiles | Resource Usage |
|----------|----------|-------------------|----------------|
| Quick    | 2-4 hrs  | 5,000-8,000      | Moderate       |
| Intensive| 4-8 hrs  | 10,000-15,000    | High           |
| Extended | 12+ hrs  | 15,000-25,000    | Variable       |

*Results depend on authentication status, keyword effectiveness, and API availability*

## 🔧 Troubleshooting

### Authentication Issues
```bash
# Test current authentication
pdb-cli auth-check --keyword "harry potter"

# Update headers from fresh browser session
# Copy new headers to .secrets/pdb_headers.json
```

### Rate Limiting
```bash  
# Reduce request rate
RPM=30 CONCURRENCY=1 ./scripts/comprehensive_personality_scrape.sh

# Enable request caching
export PDB_CACHE=1
```

### Low Discovery Rate
```bash
# Check keyword effectiveness
pdb-cli diagnose-query --contains "keyword_name"

# Add specific franchise/character keywords
nano data/bot_store/comprehensive_keywords.txt
```

## 📁 File Structure

```
scripts/
├── comprehensive_personality_scrape.sh  # Main scraping orchestrator
├── launch_comprehensive_scrape.sh       # Interactive launcher
├── scrape_progress_monitor.sh          # Progress tracking & analysis
└── optimize_keyword_discovery.sh       # Keyword optimization

data/bot_store/
├── large_keywords.txt                  # Base keyword collection
├── comprehensive_keywords.txt          # Enhanced keyword collection  
├── discovered_keywords.txt             # Auto-discovered keywords
├── pdb_profiles.parquet               # Raw profile data
├── pdb_profile_vectors.parquet        # Semantic embeddings
└── pdb_faiss.index                    # Search index

docs/
└── comprehensive_scraping_guide.md     # Detailed usage guide
```

## 🎊 Success Metrics

### Achieved Goals
- ✅ **Comprehensive System**: Multi-strategy scraping approach
- ✅ **Keyword Expansion**: 500+ curated search terms  
- ✅ **Automation**: Intelligent discovery and filtering
- ✅ **Progress Tracking**: Real-time monitoring and analytics
- ✅ **State Management**: Resumable, fault-tolerant execution
- ✅ **Documentation**: Complete usage guides and examples

### Target Achievements  
- 🎯 **10,000+ Profiles**: Primary collection goal
- 🎯 **Character Focus**: Personality-rich dataset
- 🎯 **Research Ready**: Normalized, searchable data
- 🎯 **Network Analysis**: Profile relationship mapping

## 🚀 Next Steps

1. **Validate Authentication**: Update `.secrets/pdb_headers.json` with current browser session
2. **Run Comprehensive Scraping**: Use interactive launcher for guided setup
3. **Monitor Progress**: Track collection towards 10,000+ goal
4. **Optimize Keywords**: Use discovery tools to find high-value search terms
5. **Export Data**: Generate research-ready datasets for socionics analysis

Run `./scripts/launch_comprehensive_scrape.sh` to begin comprehensive personality profile collection!

---

*This comprehensive scraping system is designed to collect tens of thousands of personality profiles systematically while respecting rate limits and maintaining data quality. The multi-phase approach ensures maximum coverage of character personalities across popular media, literature, and cultural references.*