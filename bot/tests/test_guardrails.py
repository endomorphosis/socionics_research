from bot.guardrails import apply_guardrails


def test_guardrails_block_type_assignment():
    res = apply_guardrails("What type am I?")
    assert res.blocked
    assert "direct_type_request" in res.reasons


def test_guardrails_allow_neutral():
    res = apply_guardrails("Explain Model A structure")
    assert not res.blocked
