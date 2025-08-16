# Observable & Operational Indicators (v0.2)

**Updated**: 2025-08-16  
**Status**: Expanded indicator catalog with reliability framework

**Purpose**: Provide low-theory, behavior-first indicators that can later be mapped (or not) to Socionics functional constructs. Avoid circular language and ensure empirical testability.

## 1. Principles
- Describe observable behavior, not inferred cognition.
- Coders do not see target's claimed type during annotation.
- Each indicator has: code, definition, inclusion criteria, exclusion criteria, example, reliability notes.

## 2. Indicator Template
```
Code: LEX_TOPIC_SHIFT_RATE
Domain: Discourse
Definition: Number of topic boundary transitions per minute (algorithmic segmentation + human adjudication).
Inclusion: Clear shift to semantically distinct topic cluster.
Exclusion: Minor elaborations or subtopic expansions.
Computation: automatic segmentation (embedding similarity < 0.55 threshold) + manual correction.
Example: "Anyway, leaving that aside—did you read about the Mars mission?"
Reliability: Target ICC > 0.75
Related Hypotheses: Elevated in divergent ideation profiles (candidate Ne)
```

## 3. Candidate Indicator List (Abbreviated)
| Code | Domain | Short Definition | Hypothesis Mapping (Tentative) |
|------|--------|------------------|--------------------------------|
| LEX_POSSIBILITY_MODAL_DENSITY | Lexical | Rate of modal verbs (could, might) per 100 tokens | Ne
| LEX_TEMPORAL_SEQUENCER_DENSITY | Lexical | Rate of temporal connectives (then, afterward) | Ni
| LEX_BODILY_COMFORT_TERMS | Lexical | Count of comfort/homeostasis words | Si
| LEX_FORCE_AGENCY_VERBS | Lexical | Count of assertive force verbs (push, take) | Se
| LEX_RELATIONAL_AFFECTION_TERMS | Lexical | Terms of interpersonal closeness (friendship, bond) | Fi
| LEX_AFFECT_EXPRESSION_TERMS | Lexical | Expressive emotional state words (excited, thrilled) | Fe
| LEX_SYSTEMATICITY_TERMS | Lexical | Abstract structural nouns (framework, schema) | Ti
| LEX_EFFICIENCY_METRIC_TERMS | Lexical | Productivity/metric nouns (output, KPI) | Te
| SYN_DEPTH_MEAN | Syntactic | Mean dependency tree depth | Ti (possible)
| SYN_IMPERATIVE_RATE | Syntactic | Imperative clauses per 100 clauses | Se
| SYN_MODAL_COMPLEXITY | Syntactic | Unique modal constructions per 100 clauses | Ne/Ni
| DIS_TOPIC_SHIFT_RATE | Discourse | Topic boundaries per minute | Ne
| DIS_CAUSAL_CHAIN_LENGTH | Discourse | Mean length of causal link sequences | Ni
| DIS_PRECISION_CLAR_REQUESTS | Discourse | Clarifying questions requesting specification | Ti
| DIS_VALIDATION_FEEDBACK_RATE | Discourse | Backchannels of affirmation | Fe
| PARA_PITCH_VARIABILITY | Paralinguistic | SD of pitch across voiced frames | Fe
| PARA_SPEECH_RATE_ADAPTATION | Paralinguistic | % change adapting to partner rate | Fe/Fi
| PARA_PAUSE_MEAN | Paralinguistic | Mean silent pause duration | Ti/Ni (processing) vs. contrast with Te
| INT_INTERRUPTION_RATE | Interactional | Overlapping speech initiations per minute | Se
| INT_SUPPORTIVE_OVERLAP_RATE | Interactional | Minimal encouragers without floor taking | Fe
| INT_BOUNDARY_SETTING_STATEMENTS | Interactional | Explicit scope/agenda setting statements | Te/Se
| INT_HEDGING_RATE | Interactional | Hedges (sort of, maybe) per 100 tokens | Ne/Fi

## 4. Detailed Definitions (Sample)
```
Code: DIS_CAUSAL_CHAIN_LENGTH
Definition: Average number of sequential causal connectors linking events within a single narrative turn.
Inclusion: Uses explicit connectors (because, therefore, so, which led to) or implicit causal verbs (caused, resulted).
Exclusion: Simple two-element cause-effect pairs.
Scoring: For each narrative turn, count connectors forming a chain length >=2; average across turns.
Example: "Because we missed the window, the launch was delayed, which meant the team had to re-allocate resources, so our timeline slipped." (Chain length = 3)
```

```
Code: LEX_BODILY_COMFORT_TERMS
Definition: Frequency per 100 tokens of words referencing internal physical states or comfort (warmth, soreness, hunger, cozy).
Inclusion: Direct references to personal somatic sensation or comfort preference.
Exclusion: Metaphorical uses ("warm welcome") unless coder consensus includes list.
Normalization: per 100 tokens after stopword removal.
```

## 5. Reliability Strategy
- Pilot: 50 segments double-coded.
- Compute Krippendorff's alpha for categorical, ICC(2,k) for continuous.
- Retrain coders where metrics < threshold (alpha < 0.67 provisional).
- Maintain drift checks every 200 annotations (random 10% re-coded).

## 6. Automation Path
1. Lexical extraction via spaCy + domain term lexicons (curated, version-controlled).
2. Syntactic metrics from dependency parse trees.
3. Discourse segmentation using sentence-transformer embeddings + Bayesian segmentation.
4. Paralinguistic features via Praat or opensmile; alignment with transcripts.
5. Interactional overlaps detected from diarized timestamps (pyannote.audio pipeline).

## 7. Ethical Considerations
- Avoid attributing moral valence to indicators.
- Publish false positive/negative audits for automated detectors.

## 8. Current Status & Next Steps

### Completed ✓
- ✓ **Indicator Framework**: 25+ operational definitions with coding guidelines
- ✓ **Reliability Strategy**: Krippendorff's alpha and ICC protocols established  
- ✓ **Automation Pipeline**: Basic lexical and syntactic extraction implemented
- ✓ **Ethical Guidelines**: Safeguards against moral valence attribution

### In Progress 🔄
- 🔄 **Indicator Expansion**: Target 120 comprehensive definitions (current: ~25)
- 🔄 **JSON Schema**: Machine-readable indicator definitions with validation
- 🔄 **Calibration Dataset**: 50-segment reliability exercise with answer key

### Next Steps 📋  
- 📋 **Inter-Rater Reliability**: Complete pilot study with 3 independent coders
- 📋 **Automation Testing**: Validate automated detection against human coding
- 📋 **Cross-Linguistic**: Adapt indicators for multilingual contexts
