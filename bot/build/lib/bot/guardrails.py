from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

TYPE_ASSIGNMENT_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\b(your|you're|you are)\s+(an?\s+)?(ILE|LII|ESE|SEI|SLE|LSI|EIE|IEI|LIE|ILI|SEE|ESI|IEE|EII|LSE|SLI)\b", re.I),
    re.compile(r"\bwhat\s+type\s+am\s+i\b", re.I),
]

PROHIBITED_TOPICS: list[re.Pattern[str]] = [
    re.compile(r"\bdiagnos(e|is|tic)\b", re.I),
]

@dataclass
class GuardrailResult:
    blocked: bool
    reasons: list[str]


def apply_guardrails(message: str) -> GuardrailResult:
    reasons: list[str] = []
    for pat in TYPE_ASSIGNMENT_PATTERNS:
        if pat.search(message):
            reasons.append("direct_type_request")
            break
    for pat in PROHIBITED_TOPICS:
        if pat.search(message):
            reasons.append("prohibited_topic")
            break
    return GuardrailResult(blocked=bool(reasons), reasons=reasons)


def redact(message: str, blocked: bool) -> str:
    if not blocked:
        return message
    return "[REDACTED_FOR_GUARDRAIL]"

__all__ = ["apply_guardrails", "redact", "GuardrailResult"]
