# Socionics Question Pool Generation with Similarity Removal

This directory contains scripts for generating and analyzing large pools of socionics survey questions with embeddings, K-means clustering-based similarity removal, and intelligent decimation.

## Generated Files

- **question_pool_64k.parquet**: Similarity-filtered question dataset (actual count varies based on removal)
- **question_pool_64k_original.parquet**: Original unfiltered question dataset  
- **question_pool_1000.parquet**: Decimated 1,000 question subset using K-means
- **question_pool_500.parquet**: Decimated 500 question subset using K-means  
- **question_pool_200.parquet**: Decimated 200 question subset using K-means
- **question_analysis.json**: Detailed analysis results and clustering metrics
- **similarity_removal_report.json**: Comprehensive report on similarity-based question removal
- **question_pool_report.txt**: Human-readable summary report

## Scripts

### generate_question_pool.py
Main script that generates the question pool with intelligent similarity removal.

```bash
# Default generation with moderate similarity removal
python3 scripts/generate_question_pool.py

# Custom similarity threshold (0.0-1.0, lower = less aggressive removal)
python3 scripts/generate_question_pool.py --similarity-threshold 0.3 --min-cluster-size 20

# Generate smaller initial pool for testing
python3 scripts/generate_question_pool.py --target-count 10000 --similarity-threshold 0.4
```

#### New Features - Similarity Removal:
- **K-means clustering-based similarity detection**: Groups similar questions into clusters
- **Configurable similarity threshold**: Control aggressiveness of removal (0.0-1.0)
- **Minimum cluster size filtering**: Only removes from clusters above specified size
- **Preservation of diversity**: Keeps one representative from each cluster (closest to centroid)
- **Axis balance maintenance**: Maintains uniform distribution across personality axes
- **Comprehensive reporting**: Detailed analysis of what was removed and why

#### Parameters:
- `--similarity-threshold`: Similarity threshold for removal (default: 0.5)
  - Lower values = less aggressive removal, more questions retained
  - Higher values = more aggressive removal, fewer questions retained
- `--min-cluster-size`: Minimum cluster size for removal (default: 10)
  - Higher values = less aggressive removal (only removes from large clusters)
- `--target-count`: Initial number of questions to generate (default: 64000)

### analyze_question_pool.py
Enhanced analysis and exploration tool for the generated question pools.

```bash
# Show distribution statistics
python3 scripts/analyze_question_pool.py stats

# Analyze similarity removal results
python3 scripts/analyze_question_pool.py similarity-analysis

# Search questions by text
python3 scripts/analyze_question_pool.py search --query "social energy"

# Analyze embedding clusters
python3 scripts/analyze_question_pool.py cluster --clusters 10

# Find similar questions  
python3 scripts/analyze_question_pool.py similar --query "your thinking style"

# Export balanced subset
python3 scripts/analyze_question_pool.py export --per-axis 25 --output survey/balanced_200.parquet
```

#### New Analysis Commands:
- `similarity-analysis`: Shows detailed report on similarity-based question removal
  - Removal statistics and cluster analysis
  - Distribution impact and axis changes
  - Sample removed questions with reasons

### generate_report.py
Creates a comprehensive summary report of the generation process.

```bash
python3 scripts/generate_report.py
```

## Similarity Removal System

The enhanced question pool generator now includes intelligent similarity removal to eliminate redundant questions while preserving diversity:

### How It Works
1. **Clustering**: Groups questions into clusters using K-means on embeddings
2. **Representative Selection**: Keeps the question closest to each cluster centroid
3. **Size Filtering**: Only removes from clusters above minimum size threshold
4. **Balance Preservation**: Maintains axis distribution across personality dimensions

### Key Benefits
- **Eliminates redundancy**: Removes near-duplicate questions automatically
- **Preserves diversity**: Keeps representative questions from each semantic cluster
- **Maintains balance**: Preserves uniform distribution across socionics axes
- **Configurable**: Adjustable thresholds for different use cases
- **Transparent**: Detailed reporting on what was removed and why

### Recommended Settings

**Conservative removal** (retain more questions):
```bash
python3 scripts/generate_question_pool.py --similarity-threshold 0.2 --min-cluster-size 30
```

**Moderate removal** (balanced approach):
```bash
python3 scripts/generate_question_pool.py --similarity-threshold 0.4 --min-cluster-size 15
```

**Aggressive removal** (fewer, more distinct questions):
```bash
python3 scripts/generate_question_pool.py --similarity-threshold 0.6 --min-cluster-size 5
```

## Usage Recommendations

**For survey development:**
- Use conservative settings initially to preserve question variety
- Review similarity removal reports to understand what was filtered
- Use `similarity-analysis` command to validate removal decisions
- Consider the filtered dataset as your primary pool

**For question selection:**
- Start with the similarity-filtered dataset (`question_pool_64k.parquet`)
- Use decimated sets for specific survey sizes
- Consider manual review of high-similarity clusters
- Balance axis representation in final selection

## Data Structure

Each parquet file contains the following columns:
- `question_id`: Unique identifier
- `axis`: Socionics axis (EI, NS, TF, JP, RH, IT, SF, ST)
- `text`: Question text
- `axis_label`: Human-readable axis description
- `positive_pole`: Positive pole of the axis
- `negative_pole`: Negative pole of the axis  
- `template`: Original question template
- `variables`: Template variable substitutions
- `embedding`: 64-dimensional embedding vector
- `embedding_dim`: Embedding dimensionality (64)
- `is_variation`: Whether this is a generated variation
- `base_question_id`: ID of base question for variations

## Embedding System

Uses the existing embedding infrastructure from `bot/src/bot/pdb_embed_search.py`:
- Lightweight hash-based embeddings (64 dimensions) by default
- Normalized vectors for cosine similarity calculations
- Efficient for K-means clustering and similarity analysis

## K-means Analysis Results

- **Best cluster count**: 175 clusters
- **Silhouette score**: 0.285 (weak but meaningful clustering)
- Questions cluster by semantic content and axis type
- Good separation for decimation purposes