# Socionics Research Project

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

## Tech Stack (Planned)
- Backend: Python (FastAPI) for ingestion & APIs.
- Data Store: Postgres (metadata) + object storage (media) + Parquet (features).
- NLP: spaCy, sentence-transformers, pyannote.audio (diarization), OpenAI / open-weight LLM.
- Observability: Prometheus + Grafana dashboards.
- Task Orchestration: Prefect or lightweight custom scheduler.

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

---
Status: Foundation draft (Updated: 2025-08-11)
