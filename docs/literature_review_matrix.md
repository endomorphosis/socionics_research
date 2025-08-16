# Literature Review Matrix (v0.2)

**Updated**: 2025-08-16  
**Status**: Systematic review framework with quality assessment

**Purpose**: Track and critically appraise sources relevant to Socionics, adjacent typologies, interpersonal dynamics, and methodological tools with transparent quality scoring.

## Column Definitions
| Column | Description |
|--------|-------------|
| id | Internal reference ID |
| citation | Full citation (APA) |
| year | Publication year |
| domain | Socionics | Typology | Personality | Network | Method |
| type | Peer-reviewed | Book | Preprint | Community | Unpublished |
| sample | N, population description |
| methods | Core methods used |
| measures | Instruments / constructs measured |
| key_findings | Concise empirical/takeaway summary |
| limitations | Methodological/theoretical caveats |
| relevance | How it informs current research program |
| quality_score | 1-5 heuristic (transparency, rigor) |
| replication_status | None | Attempted | Replicated | Failed |
| notes | Miscellaneous |

## Matrix (Seed Entries Illustrative)
| id | citation | year | domain | type | sample | methods | measures | key_findings | limitations | relevance | quality_score | replication_status | notes |
|----|---------|------|--------|------|--------|---------|---------|--------------|------------|-----------|----------------|--------------------|-------|
| S001 | Augustinavičiūtė, A. (Unpublished manuscripts). | 1970s | Socionics | Unpublished | n/a | Theoretical synthesis | n/a | Defines information metabolism & 16 types | Non-empirical | Primary theoretical origin | 1 | None | Translation variance |
| S002 | Bukalov, A. V. (1998). Journal articles. | 1998 | Socionics | Peer-reviewed (regional) | Unclear | Descriptive | Typological constructs | Expands function interpretations | Limited methodological transparency | Historical context | 2 | None | Language barrier |
| T010 | Jung, C. G. (1971). Psychological Types. | 1971 | Typology | Book | n/a | Theoretical | Attitudes/functions | Foundational function concepts | Non-empirical by modern standards | Background framework | 3 | Replicated (conceptual) | Translation nuances |
| P020 | McCrae & Costa (2008). Five-Factor Theory. | 2008 | Personality | Book chapter | Large cumulative | Review | Big Five traits | Trait stability, cross-cultural data | Not about Socionics | Comparative baseline | 5 | Replicated | Useful normative data |
| M030 | Kenny et al. (2006). Dyadic Data Analysis. | 2006 | Method | Book | n/a | Methodological | Dyadic models | Actor-Partner Interdependence | Not theory-specific | Provides modeling tools | 5 | Replicated | Statistical reference |
| M031 | Wasserman & Faust (1994). Social Network Analysis. | 1994 | Method | Book | n/a | Methodological | Network metrics | Graph frameworks | Not theory-specific | Relation matrix testing | 5 | Replicated | Classic |
| L040 | Yarkoni (2010). Personality in 100,000 words. | 2010 | Personality | Peer-reviewed | 694 blogs | Text mining | Linguistic markers | Correlates between word use & traits | Self-selection bias | Method analog for linguistic mapping | 4 | Replicated | Large corpus |

## Quality Scoring Heuristic
1 = speculative / low transparency
2 = limited methods / partial reporting
3 = conceptual clarity but limited data rigor
4 = solid empirical design or robust review
5 = high rigor, transparency, replicable methods

## Gaps Identified (Initial)
- Absence of large-N Socionics validation studies in indexed journals.
- Lack of preregistered intertype relational outcome studies.
- Scarcity of cross-cultural measurement invariance analyses.

## Pipeline for Expansion
1. Systematic search (databases: Scopus, PsycINFO; terms: "Socionics", "information metabolism", "intertype relations").
2. Snowball citation tracking from primary sources.
3. Inclusion filters (English/Russian/Lithuanian) with translation summary abstracts.
4. Dual coder quality scoring; resolve discrepancies by consensus.
5. Export matrix to CSV & maintain versioned updates.

## Planned Columns (Future)
- preregistration_id
- open_data (boolean)
- effect_sizes_reported (boolean)
- statistical_power_estimate

## Current Status & Next Actions

### Completed ✓
- ✓ **Quality Framework**: 5-point scoring system with clear criteria
- ✓ **Seed Entries**: Representative sample across quality levels
- ✓ **Gap Analysis**: Key methodological limitations identified

### In Progress 🔄
- 🔄 **Systematic Search**: PRISMA-style database search (Scopus, PsycINFO)
- 🔄 **Translation Project**: Priority regional sources (Russian/Lithuanian)
- 🔄 **Dual Coding**: Independent quality assessment with consensus protocols

### Next Steps 📋
- 📋 **Database Expansion**: Target 100+ sources with standardized extraction
- 📋 **Meta-Analysis**: Quantitative synthesis where methodologically appropriate  
- 📋 **CSV Export**: Machine-readable matrix for ongoing updates
