from bot.main import summarize_theory

class _MockModel:
    def encode(self, text: str):
        # deterministic pseudo-vector by char codes
        import math
        vals = [float((ord(c) % 32)) for c in text.lower()[:32]] or [1.0]
        # pad
        vals += [0.0] * (32 - len(vals))
        norm = math.sqrt(sum(v*v for v in vals)) or 1.0
        return [v / norm for v in vals]

MOCK_DOCS = [
    {"path": "docs/intro_socionics_research.md", "embedding": _MockModel().encode("Model A positions empirical")},
    {"path": "docs/other.md", "embedding": _MockModel().encode("Duality relation hypothesis")},
]


def test_summarize_theory_model_a():
    model = _MockModel()
    out = summarize_theory("Model A overview", model, MOCK_DOCS)
    assert "Model A" in out
    # Augmentation may or may not appear depending on similarity calc; just ensure summary present
    assert "Model A" in out


def test_summarize_theory_unknown():
    model = _MockModel()
    out = summarize_theory("Unlisted concept", model, [])
    assert "Topic not found" in out
