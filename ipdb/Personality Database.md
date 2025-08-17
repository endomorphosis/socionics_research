# Personality Database Integration Project

**Updated**: 2025-08-16  
**Status**: Integrated into main pipeline ([`docs/pdb_pipeline.md`](../docs/pdb_pipeline.md))

## Project Objectives

This component focuses on collecting and analyzing personality type data from the Personality Database (personality-database.com) to support empirical socionics research. The integration serves several key research purposes:

### Data Collection Goals
- **Comprehensive Profile Ingestion**: Systematic collection from PDB API endpoints (v1/v2)
- **Content-Addressed Storage**: IPFS CID-based deduplication and integrity verification
- **Vector Embeddings**: Semantic search capabilities for profile discovery and analysis
- **Relationship Networks**: Mapping inter-profile connections for social validation studies

### Research Applications
- **Type Comparison Studies**: Cross-system analysis (Socionics, MBTI, Big Five)
- **Survey Question Discovery**: Statistical divergence analysis to identify discriminative items
- **Community Interest Tracking**: Analysis of trending search patterns and profile popularity
- **Large-Scale Validation**: Statistical analysis with thousands of profiles for empirical grounding

### Privacy & Ethics Compliance
- **Public Data Only**: Exclusively uses publicly available profile information
- **Research Purpose**: Data used for academic research with appropriate attribution
- **No Personal Identification**: Focuses on type patterns, not individual identification
- **Transparent Methodology**: All analysis code and methods openly available

## Technical Implementation

The PDB integration is implemented through:
- **CLI Tool**: Comprehensive command-line interface (`pdb-cli`) for data operations
- **Parquet Storage**: Efficient storage with `(cid, payload_bytes)` and `(cid, vector)` schemas  
- **FAISS Indexing**: Optimized vector search for large-scale similarity queries
- **Statistical Analysis**: KL/JS divergence tools for survey question analysis

## Integration with Main Project

This PDB data serves as a foundational dataset for the broader socionics research project, providing:
- Ground truth type labels for validation studies
- Large-scale survey response patterns for indicator development  
- Community interest signals for research prioritization
- Network relationship data for social validation approaches

**For detailed technical documentation, see [`docs/pdb_pipeline.md`](../docs/pdb_pipeline.md) and [`bot/README.md`](../bot/README.md#personality-database-pdb-integration).**