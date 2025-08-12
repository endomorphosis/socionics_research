"""Integration-style tests combining guardrails + theory summarization logic.

These do NOT invoke Discord; they simulate the sequence the /theory command
performs: guardrails -> (if allowed) summarize_theory with loaded doc embeddings.
"""

from bot.main import summarize_theory
from bot.guardrails import apply_guardrails


class _MockModel:
    def encode(self, text: str):
        import math
        vals = [float((ord(c) % 32)) for c in text.lower()[:32]] or [1.0]
        vals += [0.0] * (32 - len(vals))
        norm = math.sqrt(sum(v * v for v in vals)) or 1.0
        return [v / norm for v in vals]


def _build_docs(model: _MockModel):
    return [
        {"path": "docs/model_intro.md", "embedding": model.encode("Model A positions empirical")},
        {"path": "docs/duality.md", "embedding": model.encode("Duality relation hypothesis")},
    ]


def test_guardrails_block_type_request():
    guard = apply_guardrails("What type am I?")
    assert guard.blocked
    assert "direct_type_request" in guard.reasons


def test_summarize_with_docs_integration():
    model = _MockModel()
    docs = _build_docs(model)
    guard = apply_guardrails("Model A deep dive")
    assert not guard.blocked
    out = summarize_theory("Model A deep dive", model, docs)
    assert "Model A" in out
    # When docs exist, either base summary or augmentation lines should appear.
    assert "Doc:" in out or "Related docs" in out


def test_unknown_topic_with_docs_fallback():
    model = _MockModel()
    docs = _build_docs(model)
    topic = "Obscure construct"  # no canned summary branch
    guard = apply_guardrails(topic)
    assert not guard.blocked
    out = summarize_theory(topic, model, docs)
    # Should not be the strict 'Topic not found' because docs exist; expect fallback prefix
    assert ("No canned summary" in out) or ("Doc:" in out)
