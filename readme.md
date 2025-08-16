# Socionics Research Project

![CI](https://github.com/endomorphosis/socionics_research/actions/workflows/ci.yml/badge.svg)

**Purpose**: Build a transparent, empirically rigorous infrastructure to evaluate and, if justified, refine or falsify core claims of Socionics (information metabolism, functional model, intertype relations) using open science practices.

## Table of Contents
- [Quick Start](#quick-start)
- [Documentation Index](#documentation-index)
- [Current Status & Focus](#current-status--focus-milestone-m0--foundation)
- [Community & LLM Integration](#community--llm-integration-summary)
- [Bot & Research Infrastructure](#bot--research-infrastructure-implemented-v02-core)
- [Open Science Practices](#open-science-practices)
- [Contributing](#contributing)
- [Roadmap](#roadmap-high-level)
- [License](#license)
- [Development](#development)

## Quick Start

**For Researchers**: Start with [`docs/intro_socionics_research.md`](docs/intro_socionics_research.md) for theoretical background, then explore [`docs/data_schema.md`](docs/data_schema.md) and [`docs/operational_indicators.md`](docs/operational_indicators.md) for methodological details.

**For Developers**: See the [Development](#development) section for setup instructions. Run tests with `cd bot && pytest` after following the installation steps.

**For Community Members**: The Discord bot documentation is at [`bot/README.md`](bot/README.md). For data contribution and consent information, see [`docs/ethics_consent_outline.md`](docs/ethics_consent_outline.md).

**PDB CLI (Ingestion) Quickstart**:
```bash
# one-time setup
python -m venv .venv
source .venv/bin/activate
cd bot && pip install -e .[dev] && cd ..

# configure v2 headers (browser-like) and enable cache
export PDB_CACHE=1
export PDB_API_BASE_URL=https://api.personality-database.com/api/v2
export PDB_API_HEADERS="$(tr -d '\n' < .secrets/pdb_headers.json)"

# explore & ingest (basic commands)
PYTHONPATH=bot/src python -m bot.pdb_cli peek profiles --params '{"limit":3}'
PYTHONPATH=bot/src python -m bot.pdb_cli search-top "psychology" --only-profiles --pages 1 --limit 20
PYTHONPATH=bot/src python -m bot.pdb_cli coverage --sample 10

# relationship network analysis
PYTHONPATH=bot/src python -m bot.pdb_cli edges-report --top 15
PYTHONPATH=bot/src python -m bot.pdb_cli edges-analyze --top 3 --per-component-top 5
PYTHONPATH=bot/src python -m bot.pdb_cli edges-export --out data/bot_store/pdb_profile_edges_components.parquet

# discovery-first scan (stateful + cached)
PYTHONPATH=bot/src python -m bot.pdb_cli \
	--rpm 90 --concurrency 3 --timeout 30 \
	scan-all --max-iterations 0 \
	--search-names --limit 20 --pages 1 --until-empty \
	--sweep-queries psychology,personality,mbti \
	--sweep-pages 5 --sweep-until-empty \
	--max-no-progress-pages 3 \
	--auto-embed --auto-index --index-out data/bot_store/pdb_faiss.index \
	--use-state

# troubleshooting helpers
PYTHONPATH=bot/src python -m bot.pdb_cli cache-clear
PYTHONPATH=bot/src python -m bot.pdb_cli diagnose-query --contains "psychology"
```

See [`docs/pdb_pipeline.md`](docs/pdb_pipeline.md) for comprehensive CLI documentation and troubleshooting guides.
See [`docs/pdb_pipeline.md`](docs/pdb_pipeline.md) for full pipeline details and `scan-all` behavior.

## Documentation Index
- **Intro / Conceptual Overview**: [`docs/intro_socionics_research.md`](docs/intro_socionics_research.md) - Academic introduction to socionics theory and research framework
- **Data Schema**: [`docs/data_schema.md`](docs/data_schema.md) - Structured data formats for research storage and analysis
- **Operational Indicators**: [`docs/operational_indicators.md`](docs/operational_indicators.md) - Behavior-first observable indicators and measurement protocols
- **Literature Review Matrix**: [`docs/literature_review_matrix.md`](docs/literature_review_matrix.md) - Comprehensive review and quality assessment of relevant research
- **Annotation Protocol**: [`docs/annotation_protocol.md`](docs/annotation_protocol.md) - Standardized procedures for data annotation and typing judgments
- **Ethics & Consent**: [`docs/ethics_consent_outline.md`](docs/ethics_consent_outline.md) - Ethical framework and participant consent procedures
- **PDB Pipeline**: [`docs/pdb_pipeline.md`](docs/pdb_pipeline.md) - Personality Database integration and processing pipeline
- **Bot Documentation**: [`bot/README.md`](bot/README.md) - Discord bot implementation and usage guide
 - **Data Artifacts (PDB)**: See Appendix A in [`docs/data_schema.md`](docs/data_schema.md#appendix-a-pdb-storage-artifacts) for parquet schemas and FAISS index mapping

## Current Status & Focus (Milestone M0 → Foundation)

**Version**: 0.2.0 (Updated: 2025-08-16)

### Completed ✓
1. ✓ Discord bot v0.2 core implementation with privacy-first design
2. ✓ Basic JSON schemas for person/session/annotation/typing entities
3. ✓ Operational indicators framework and seed definitions (20+ indicators)
4. ✓ Literature review matrix with quality scoring methodology
5. ✓ Ethics and consent framework draft

### In Progress 🔄
1. 🔄 Finalize v0.2 JSON schemas with enhanced validation rules
2. 🔄 Expand indicator catalogue (target: 120 definitions; current: ~25 seed definitions)
3. 🔄 Discord community governance plan and moderation guidelines
4. 🔄 LLM chatbot guardrails testing and evaluation harness
5. 🔄 Reliability calibration dataset preparation (target: 50 segments × 3 raters)

### Next Steps 📋
1. 📋 Deploy Discord server with initial community guidelines
2. 📋 Implement annotation interface prototype
3. 📋 Establish inter-rater reliability benchmarks
4. 📋 Begin pilot data collection with consented participants

## Community & LLM Integration (Summary)
We will use a moderated Discord server as an opt-in ecological data source and participant engagement hub. A purpose-built LLM chatbot will:
- Guide consent (/consent command) and report data usage transparency.
- Deliver structured elicitation prompts for balanced linguistic sampling.
- Provide an FAQ (no type feedback allowed).
- Offer optional annotation assist suggestions to internal raters (never auto-apply labels).

Safeguards: explicit tagging for research-use messages; PII scrubbing pipeline; guardrail tests blocking typing/diagnostic claims; audit logs linking each data row to consent tier & pipeline version.

## Bot & Research Infrastructure (Implemented v0.2 Core)

**Status**: Production-ready core features with ongoing enhancements

### Current Bot Features (Privacy-First Design)
- **Vector Ingestion** (`/ingest_channel`): Stores ONLY embeddings + hashed user & token hashes
- **Hybrid Search**: Keyword hashed token pre-filter + semantic vector ranking
- **Content Guardrails**: Prevents type assignment & diagnostic claims with red-team testing
- **Theory Explanations** (`/theory`): Retrieval-augmented summaries with document embedding store
- **Rate Limiting**: Command-specific and search category limits for fair usage
- **Data Management**: Message purge (`/purge_message`) and salt rotation CLI for privacy resets
- **Context Assembly** (`/llm_context`): Returns metadata JSON only (no raw content exposure)
- **Structured Logging**: Optional JSON logs with privacy controls (`SOCIONICS_JSON_LOGS=true`)
- **Metrics Endpoint**: Prometheus scrape endpoint (optional) for operational monitoring

### Planned Service Enhancements (Roadmap)
- **API Layer**: FastAPI integration for external tool compatibility
- **Enhanced Storage**: PostgreSQL metadata + object storage for multimodal datasets
- **Advanced Audio**: Diarization & acoustic feature extraction (pyannote.audio integration)
- **Orchestration**: Scheduled embeddings & audit processes (Prefect or custom)

## Open Science Practices

### Research Transparency
- **Preregistration**: All confirmatory studies preregistered on OSF before data collection
- **Open Data**: Derived, de-identified feature matrices (Tier 2) released under permissive license
- **Version Control**: All protocols & prompt libraries use semantic versioning for reproducibility
- **Audit Reports**: Quarterly bias & performance audits published in this repository

### Reproducibility Standards
- **Code Availability**: All analysis scripts and data processing pipelines openly available
- **Environment Specification**: Containerized environments for computational reproducibility
- **Data Lineage**: Complete provenance tracking from raw data to final results
- **Model Transparency**: LLM prompts, hyperparameters, and evaluation metrics fully documented

## Contributing

**Current Status**: Early-stage development - structured contributions welcome

### How to Contribute
- **Issues & Discussions**: Open an issue to discuss proposals before implementation
- **Indicator Proposals**: Provide operational definitions with empirical rationale and citations
- **Methodological Critiques**: Identify potential biases, confounds, or methodological improvements
- **Translation Offers**: Help translate key regional sources (Russian/Lithuanian priority)

### Contribution Guidelines
- All new constructs must include literature citations and empirical justification
- Follow existing code style and documentation patterns
- Include tests for any code contributions
- Respect privacy-first design principles in all data handling

## Roadmap (High-Level)

### Phase 1: Foundation & Corpus Assembly (Q4 2025)
- ✓ Core infrastructure and bot implementation
- 🔄 Discord community establishment and initial recruitment
- 🔄 Reliability benchmarks for annotation protocols
- 📋 Initial linguistic corpus collection (target: 100 participants)

### Phase 2: Measurement Model Development (Q1-Q2 2026)
- 📋 Item Response Theory (IRT) analysis of indicators
- 📋 Exploratory and Confirmatory Factor Analysis
- 📋 Cross-validation with established personality measures

### Phase 3: Structural Validation (Q3-Q4 2026)
- 📋 Predictive modeling of interpersonal outcomes
- 📋 Network analysis of intertype relations
- 📋 Replication studies with independent samples

### Phase 4: Dyadic & Network Effects (2027)
- 📋 Controlled interaction studies
- 📋 Longitudinal relationship tracking
- 📋 Group dynamics and team composition effects

### Phase 5: Applied Research (2028+)
- 📋 Intervention studies and applications
- 📋 Longitudinal stability assessment
- 📋 Cross-cultural validation studies

## License
See `LICENSE`.

## Disclaimer
Socionics constructs are under empirical evaluation here; no interpretive feedback constitutes psychological advice or diagnosis.

## Development

### Run Tests
Ensure Python 3.11 and a virtual environment, then:
```
cd bot
pip install .[dev]
export SOCIONICS_DISCORD_TOKEN=dummy
export SOCIONICS_HASH_SALT=local_salt
pytest
```

### Run Bot Locally (Ephemeral)
```
cd bot
export SOCIONICS_DISCORD_TOKEN=your_token_here
export SOCIONICS_HASH_SALT=your_random_salt
python -m bot.main
```

### Docker
```
cd bot
docker build -t socionics-bot:latest .
docker run --rm -e SOCIONICS_DISCORD_TOKEN=your_token -e SOCIONICS_HASH_SALT=your_salt socionics-bot:latest
```

### Metrics
If enabled (default), a Prometheus scrape endpoint is exposed on `:9108/metrics` inside the container.

### Environment Variables (SOCIONICS_ Prefix)

**Essential Configuration**
- `DISCORD_TOKEN` (required): Bot authentication token
- `HASH_SALT` (required): Salt for privacy-preserving hash functions

**Performance & Behavior Settings**
- `EMBED_MODEL` (default: `sentence-transformers/all-MiniLM-L6-v2`): Embedding model selection
- `LIGHTWEIGHT_EMBEDDINGS=true`: Use hash-based 64-dim test embedder for development
- `RATE_LIMIT_PER_MIN` (default: 15): General command rate limit per user
- `SEARCH_RATE_LIMIT_PER_MIN` (default: 30): Search-specific rate limit
- `RETRIEVAL_TOP_K` (default: 4): Number of top results for semantic search
- `MAX_CONTEXT_RESULTS` (default: 20): Maximum results in context assembly

**Privacy & Operations**
- `JSON_LOGS=true`: Enable structured JSON logging (default: false)
- `ENABLE_METRICS=true`: Enable Prometheus metrics endpoint (default: true)
- `ADMIN_ROLE_IDS=id1,id2,id3`: Comma-separated Discord role IDs for admin commands
- `DATA_DIR=custom_path` (default: `data/bot_store`): Custom data storage location

### Salt Rotation Procedure (Privacy Reset)

**When to Rotate**: 
- Quarterly for production deployments
- After any potential security incident
- When participants request data deletion

**Steps**:
1. **Generate New Salt**: Choose cryptographically random salt (≥16 chars recommended)
2. **Run Rotation Script**: `python -m bot.maintenance NEW_SALT -y`
3. **Update Environment**: Set `SOCIONICS_HASH_SALT=NEW_SALT` in deployment configuration
4. **Restart Service**: Bot restart required (old hashed store archived under `backup_<timestamp>/`)

**Important**: Historic data cannot be re-identified after rotation, ensuring forward secrecy.

### Security & Privacy Implementation

**Data Minimization Principles**
- **No Raw Storage**: No message text stored - only embeddings and salted hashes
- **Hash-Based Identity**: User IDs protected with salted SHA256 (rotation supported)
- **Token Hashing**: Only first 100 tokens per message hashed for search indexing
- **Forward Secrecy**: Salt rotation makes historic re-identification impossible

**Privacy Controls**
- **Right to Deletion**: Purge command removes all traces by message ID
- **Audit Transparency**: JSONL logs record minimal metadata (timestamp, event, counts)
- **Consent Tiers**: Multi-level consent system with granular data usage controls
- **Secure Storage**: Structured JSON logging optional; route to secure sink in production

**Security Architecture**
- **Environment Isolation**: All secrets via environment variables only
- **Minimal Attack Surface**: Stateless bot design with containerized deployment
- **Regular Rotation**: Quarterly salt rotation recommended for production

---
**Project Status**: Foundation phase (v0.2.0) - Updated: 2025-08-16  
**Bot Implementation**: Core features stable, community deployment in progress
