# Annotation & Typing Protocol (v0.2)

**Updated**: 2025-08-16  
**Status**: Production-ready workflow with reliability benchmarks

**Purpose**: Provide a standardized, reproducible process for generating behavioral annotations and convergent typing judgments while minimizing theoretical circularity and maximizing inter-rater reliability.

## 2. Roles
- Annotator: Codes observable indicators.
- Panel Rater: Integrates indicators + qualitative cues for type hypothesis (blinded to self-report).
- Adjudicator: Resolves disagreements, monitors drift.
- Data Engineer: Maintains tooling & data integrity.

## 3. Workflow Overview
1. Ingestion & Transcription
2. Segmentation & Diarization
3. Automated Feature Pre-computation
4. Manual Annotation (indicator subset)
5. Inter-rater Reliability Check
6. Panel Typing (independent)
7. Consensus Meeting (if low agreement)
8. Final Typing Record & Confidence Distribution

## 4. Segmentation Guidelines
- Segment length target: 2–15 seconds or 5–40 tokens.
- Split at major syntactic closure or clear speaker handover.
- Merge micro-utterances (<2 tokens) into adjacent segment unless they carry distinct pragmatic force.

## 5. Diarization Quality Criteria
- Diarization DER (Diarization Error Rate) < 10% on validation subset.
- Manual correction for overlapping speech > 3 seconds.

## 6. Annotation Procedure
- Tool surfaces one segment at a time (randomized order for some indicators to reduce sequential bias).
- Annotators select applicable indicators (multi-label) + enter confidence (0–1 sliders default 0.5).
- Time per segment guideline: <30s average after training.

## 7. Indicator Coding Rules (Example)
Indicator: INT_INTERRUPTION_RATE
Rule: Mark overlap when second speaker begins before 200ms of preceding pause and original speaker resumes within 1s; exclude backchannel minimal encouragers.

## 8. Reliability Monitoring
- Weekly dashboard: per-indicator alpha/ICC with trend lines.
- Trigger recalibration if 3-week moving average < threshold.
- Drift test set (frozen) recoded monthly; compare deltas.

## 9. Panel Typing Process
- Each rater reviews 15-minute composite sample (balanced across contexts) + aggregated indicator summary (no function labels).
- Raters assign probability distribution across 16 types (sum=1) + free-text rationale.
- Minimum calibration: pass two gold-standard cases (>0.60 probability on correct type).

## 10. Consensus & Adjudication
- Compute Jensen-Shannon divergence among rater distributions; if mean JSD > 0.25, convene consensus.
- Consensus rule: Weighted discussion, optionally produce composite distribution (average) or adopt mixture if distinct clusters.

## 11. Documentation & Audit Trail
- Store raw rater distributions, timestamps, rationale texts.
- Maintain versioned protocol; changes logged (semantic versioning).

## 12. Bias Mitigation
- Remove explicit self-typed labels from all annotation UIs.
- Rotate order of presented participants.
- Periodic blindness checks: inject synthetic disguised samples.

## 13. Tooling Requirements (MVP)
- Web UI (React) + backend (FastAPI) + Postgres.
- Auth + role-based access control.
- Real-time reliability computation pipeline (Kafka or lightweight queue optional if scaling >5 annotators).

## 14. Data Security
- Encrypt at rest (disk) and in transit (TLS).
- Pseudonymize person_id early; separation of key file.
- Access logging & monthly review.

## 15. Ethical Oversight
- Annual protocol review by independent advisor.
- Participant right to withdraw triggers purge workflow (flag references + remove within 30 days).

## 16. KPIs
- Median annotation latency < 25s.
- Inter-rater alpha >= 0.70 majority indicators.
- Panel correct calibration accuracy (gold cases) >= 0.75.

## 17. Future Enhancements
- Active learning suggestion of low-consensus segments.
- Semi-automated rationales (NLP summary) for rater support.
- Cross-language annotation with translation alignment confidence.
