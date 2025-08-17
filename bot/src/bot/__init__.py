"""Socionics Discord Bot package.

Exports core components used in tests, with lazy imports to avoid
eager settings initialization during CLI usage.
"""

from __future__ import annotations

from typing import Any

__all__ = ["Ingestor", "apply_guardrails", "RateLimiter", "parse_time_range"]


def __getattr__(name: str) -> Any:  # PEP 562 lazy attributes
	if name == "Ingestor":
		from .ingest import Ingestor as _Ingestor

		return _Ingestor
	if name == "apply_guardrails":
		from .guardrails import apply_guardrails as _apply

		return _apply
	if name == "RateLimiter":
		from .utils import RateLimiter as _rl

		return _rl
	if name == "parse_time_range":
		from .utils import parse_time_range as _ptr

		return _ptr
	raise AttributeError(f"module 'bot' has no attribute {name!r}")
