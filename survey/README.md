# Socionics Question Pool Generation

This directory contains scripts for generating and analyzing large pools of socionics survey questions with embeddings for K-means clustering and similarity analysis.

## Generated Files

- **question_pool_64k.parquet**: Full 64,000 question dataset with embeddings
- **question_pool_1000.parquet**: Decimated 1,000 question subset using K-means
- **question_pool_500.parquet**: Decimated 500 question subset using K-means  
- **question_pool_200.parquet**: Decimated 200 question subset using K-means
- **question_analysis.json**: Detailed analysis results and clustering metrics
- **question_pool_report.txt**: Human-readable summary report

## Scripts

### generate_question_pool.py
Main script that generates the question pool from templates and semantic variations.

```bash
python3 scripts/generate_question_pool.py
```

Features:
- Generates 64,000 questions from 8 socionics axes (EI, NS, TF, JP, RH, IT, SF, ST)
- Creates semantic variations using text transformations
- Computes embeddings using existing embedding infrastructure
- Performs K-means clustering analysis to find optimal cluster count
- Generates decimated subsets with uniform distribution across axes
- Saves all data in parquet format for efficient storage and access

### analyze_question_pool.py
Analysis and exploration tool for the generated question pools.

```bash
# Show distribution statistics
python3 scripts/analyze_question_pool.py stats

# Search questions by text
python3 scripts/analyze_question_pool.py search --query "social energy"

# Analyze embedding clusters
python3 scripts/analyze_question_pool.py cluster --clusters 10

# Find similar questions  
python3 scripts/analyze_question_pool.py similar --query "your thinking style"

# Export balanced subset
python3 scripts/analyze_question_pool.py export --per-axis 25 --output survey/balanced_200.parquet
```

### generate_report.py
Creates a comprehensive summary report of the generation process.

```bash
python3 scripts/generate_report.py
```

## Usage Recommendations

**For survey development:**
- Use `question_pool_200.parquet` for initial testing
- Use `question_pool_500.parquet` for pilot studies  
- Use `question_pool_1000.parquet` for full surveys

**For question decimation:**
- K-means clustering with k=175 provides good separation
- Consider manual review of cluster representatives
- Balance axis representation in final selection

**For uniform distribution:**
- Generated sets show good uniformity across axes (uniformity score ~0.1-0.2)
- Consider balanced sampling for specific applications
- Monitor for axis bias in final question selection

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