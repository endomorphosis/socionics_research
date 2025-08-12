# Socionics Research Project

![CI](https://github.com/endomorphosis/socionics_research/actions/workflows/ci.yml/badge.svg)

Purpose: Build a transparent, empirically rigorous infrastructure to evaluate and, if justified, refine or falsify core claims of Socionics (information metabolism, functional model, intertype relations) using open science practices.

## Documentation Index
- Intro / Conceptual Overview: `docs/intro_socionics_research.md`
- Data Schema Draft: `docs/data_schema.md`
- Operational Indicators (behavior-first): `docs/operational_indicators.md`
- Literature Review Matrix: `docs/literature_review_matrix.md`
- Annotation & Typing Protocol: `docs/annotation_protocol.md`
- Ethics & Consent Outline: `docs/ethics_consent_outline.md`

## Current Focus (Milestone M0 → Foundation)
1. Finalize v0.1 JSON Schemas for person/session/annotation/typing.
2. Expand indicator catalogue (target: 120 definitions; current: seed subset).
3. Stand up Discord community plan + governance draft.
4. Specify LLM chatbot guardrails & evaluation harness.
5. Seed reliability calibration dataset (50 segments × 3 raters).

## Community & LLM Integration (Summary)
We will use a moderated Discord server as an opt-in ecological data source and participant engagement hub. A purpose-built LLM chatbot will:
- Guide consent (/consent command) and report data usage transparency.
- Deliver structured elicitation prompts for balanced linguistic sampling.
- Provide an FAQ (no type feedback allowed).
- Offer optional annotation assist suggestions to internal raters (never auto-apply labels).

Safeguards: explicit tagging for research-use messages; PII scrubbing pipeline; guardrail tests blocking typing/diagnostic claims; audit logs linking each data row to consent tier & pipeline version.

## Bot & Research Infrastructure (Implemented v0.1 Core)
Current bot features (privacy-first):
- Vector ingestion (/ingest_channel) storing ONLY embeddings + hashed user & token hashes.
- Hybrid search (keyword hashed token pre-filter + semantic vector ranking).
- Guardrails preventing type assignment & diagnostic claims.
- Retrieval-augmented theory summaries (/theory) with doc embedding store.
- Rate limiting (command + search categories).
- Purge (/purge_message) and salt rotation CLI resetting hashed stores.
- LLM context assembly (/llm_context) returns metadata JSON only.
- Structured JSON logging (opt-in via SOCIONICS_JSON_LOGS=true).
- Metrics endpoint (Prometheus scrape) optional.

Planned service layers (future roadmap):
- API layer (FastAPI) for external tool integration.
- Postgres metadata + object storage for richer multimodal datasets.
- Advanced diarization & acoustic features (pyannote.audio) integration.
- Orchestration (Prefect or custom) for scheduled embeddings & audits.

## Open Science Practices
- Preregistration on OSF for confirmatory phases.
- Public release of derived, de-identified feature matrices (Tier 2) under permissive license.
- Versioned protocols & prompt libraries (semantic versioning).
- Quarterly bias & performance audits published in repository.

## Contributing
Early phase; please open an issue to discuss additions (indicator proposals, methodological critiques, translation offers). Provide citations & rationale for new constructs.

## Roadmap (High-Level)
- Phase 1: Corpus assembly, reliability benchmarks.
- Phase 2: Measurement model development (IRT, factor analysis).
- Phase 3: Structural validation & predictive modeling.
- Phase 4: Network & dyadic relation tests.
- Phase 5: Longitudinal stability & intervention RCT.

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

### Environment Variables (Prefix SOCIONICS_)
Essential:
- DISCORD_TOKEN (required)
- HASH_SALT (required)

Behavior / Performance:
- EMBED_MODEL (default sentence-transformers/all-MiniLM-L6-v2)
- LIGHTWEIGHT_EMBEDDINGS=true (hash-based 64-dim test embedder)
- RATE_LIMIT_PER_MIN (default 15)
- SEARCH_RATE_LIMIT_PER_MIN (default 30)
- RETRIEVAL_TOP_K (default 4)
- MAX_CONTEXT_RESULTS (default 20)

Privacy / Ops:
- JSON_LOGS=true (structured logs)
- ENABLE_METRICS=true
- ADMIN_ROLE_IDS=comma_separated_ids (enforces role check; else manage_messages fallback)
- DATA_DIR=custom_data_path (default data/bot_store)

Salt Rotation Procedure:
1. Choose new random salt (>=16 chars recommended).
2. Run: `python -m bot.maintenance NEW_SALT -y`
3. Update deployment secret ENV (HASH_SALT) to NEW_SALT.
4. Restart bot (old hashed store archived under backup_<ts>/).

### Security & Privacy Notes
- No raw message text stored: only embeddings, hashed user IDs (salted SHA256), hashed tokens (first 100 per message).
- Salt rotation implemented (see procedure) for forward-secrecy style reset; historic re-identification impossible post-rotation.
- Purge command supports right-to-be-forgotten by message ID (removes vector + token hashes).
- Audit log (JSONL) records minimal event metadata (timestamp, event, counts).
- Structured JSON logging optional; disable or route to secure sink in production.

---
Status: Foundation draft (Updated: 2025-08-11, features synced with bot v0.1 core)
