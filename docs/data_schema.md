# Data Schema Specification (v0.2)

**Updated**: 2025-08-16  
**Status**: Production-ready with ongoing enhancements

**Purpose**: Define a minimal, extensible schema for storing multi-modal Socionics research data with transparency and interoperability.

## 1. Entities & Levels
- Person (participant)
- Session (an interaction, interview, survey completion, recording event)
- Modality Artifact (text transcript, audio clip, video file, survey response set)
- Annotation (unit-level coded segment)
- Typing Judgment (per rater, per type hypothesis)

## 2. File/Directory Layout (Proposed)
```
/data
  /persons
    person_<id>.json
  /sessions
    session_<id>.json
  /artifacts
    <artifact_id>.<ext>
  /transcripts
    <artifact_id>.jsonl  # segmented transcript
  /annotations
    annotation_<id>.json
  /typings
    typing_<person_id>_<panel|algo|self>.json
  /derivatives
    features_<artifact_id>.parquet
```

## 3. Person Schema (person_<id>.json)
```
{
  "person_id": "UUID",
  "demographics": {
    "age": 29,
    "gender": "self_described",
    "culture": "US",
    "native_language": "en",
    "education_level": "bachelor",
    "occupation": "student"
  },
  "consent": {
    "version": "1.0.0",
    "date": "2025-08-16",
    "modalities_approved": ["text","audio","video"],
    "data_sharing_tier": "anonymized"
  },
  "self_reports": {
    "big_five": {"o": 34, "c": 28, "e": 12, "a": 30, "n": 18},
    "socionics_self_type": "ILE",
    "other_typologies": {"mbti": "ENTP"}
  }
}
```

## 4. Session Schema (session_<id>.json)
```
{
  "session_id": "UUID",
  "person_ids": ["UUID1","UUID2"],
  "context": {
    "setting": "interview_remote",
    "task": "abstract_discussion",
    "duration_seconds": 1320,
    "date": "2025-08-16"
  },
  "artifacts": ["a123-text","a124-audio","a125-video"],
  "metadata": {
    "language": "en",
    "recording_software": "OBS 30.0",
    "transcriber": "whisper-large-v3",
    "transcription_confidence_mean": 0.92
  }
}
```

## 5. Transcript Format (JSONL segments)
Each line a JSON object with time-aligned segment.
```
{"segment_id":"s1","artifact_id":"a124-audio","speaker_id":"UUID1","start":0.0,"end":4.2,"text":"So I tend to explore multiple angles at once"}
{"segment_id":"s2","artifact_id":"a124-audio","speaker_id":"UUID2","start":4.3,"end":7.1,"text":"Could you narrow it down?"}
```

## 6. Annotation Schema (annotation_<id>.json)
```
{
  "annotation_id": "UUID",
  "segment_id": "s1",
  "scheme_version": "0.2.0",
  "layers": {
    "function_candidate": ["Ne"],
    "discourse_act": "elaboration",
    "affect_display": {"valence": "pos","intensity": 0.4},
    "epistemic_modality": "speculative"
  },
  "rater_id": "R123",
  "confidence": 0.78,
  "timestamp": "2025-08-16T14:22:35Z"
}
```

## 7. Typing Judgment Schema (typing_<person_id>_<source>.json)
```
{
  "person_id": "UUID1",
  "source": "panel",  // panel | self | algo
  "panel_id": "PANEL_A",
  "date": "2025-08-16",
  "method": "structured_interview_v1",
  "type_assignment": "ILE",
  "function_confidences": {"Ne":0.86,"Ti":0.74,"Fe":0.41,"Si":0.25,"Ni":0.18,"Te":0.33,"Fi":0.22,"Se":0.27},
  "notes": "Strong divergent ideation and structural framing; low comfort focus.",
  "inter_rater": {
    "rater_ids": ["R123","R129","R140"],
    "agreement_metric": {"alpha": 0.72}
  }
}
```

## 8. Derived Feature Matrix (features_<artifact_id>.parquet)
Column groups:
- lexical_* (counts, tf-idf)
- syntactic_* (dependency metrics)
- prosody_* (pitch_mean, jitter, shimmer)
- discourse_* (turn_length_mean, topic_shift_rate)
- nonverbal_* (if video processed)
- meta.* (artifact_id, person_id)

## 9. ID & Versioning Conventions
- All IDs lowercase alphanumeric + short type prefix optional.
- Semantic version for schemes (major change if backward incompatibility).

## 10. Privacy & De-identification
- Replace person_id with salted hash in shared tiers.
- Remove or generalize proper nouns in transcripts (NER redaction pipeline). 

## 11. Validation Checklist
- JSON schema validation per entity.
- Referential integrity (all foreign keys resolve).
- Temporal consistency (segment times non-overlapping per speaker).
- Value ranges (confidence 0-1, durations >0).

## 12. Implementation Status & Next Steps

### Completed ✓
- ✓ JSON schema validation framework implemented
- ✓ Core entity definitions stabilized
- ✓ Privacy and de-identification protocols established
- ✓ Basic integrity validation checklist

### Next Steps 📋
- 📋 **Formal JSON Schema Files**: Publish machine-readable `.schema.json` files with full validation rules
- 📋 **Ingestion Pipeline**: Create automated scripts with comprehensive integrity testing harness  
- 📋 **Data Dictionary**: Complete mapping of each feature to computation method with literature citations
- 📋 **Schema Evolution**: Implement backward-compatible versioning system for schema updates
