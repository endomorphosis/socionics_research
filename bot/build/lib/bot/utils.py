from __future__ import annotations

import re
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Deque, Dict

import orjson

from .config import settings
from typing import Iterable, Protocol

TIME_PATTERN = re.compile(r"(\d+)([smhdw])", re.I)
UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

def parse_time_range(spec: str) -> tuple[float, float]:
    now = time.time()
    total = 0
    for amount, unit in TIME_PATTERN.findall(spec):
        total += int(amount) * UNIT_SECONDS[unit.lower()]
    start = now - total if total else now - 3600
    return start, now

class RateLimiter:
    def __init__(self, limit_per_min: int) -> None:
        self.limit = limit_per_min
        self.calls: Dict[int, Deque[float]] = defaultdict(deque)

    def allow(self, user_id: int) -> bool:
        window_start = time.time() - 60
        dq = self.calls[user_id]
        while dq and dq[0] < window_start:
            dq.popleft()
        if len(dq) >= self.limit:
            return False
        dq.append(time.time())
        return True

def audit_log(event: str, **fields) -> None:
    path = Path(settings.audit_log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"ts": datetime.utcnow().isoformat() + "Z", "event": event, **fields}
    with path.open("ab") as f:
        f.write(orjson.dumps(record) + b"\n")

def build_context_snippet(results: list[dict]) -> str:
    lines = []
    for r in results:
        ts = datetime.utcfromtimestamp(r["created_ts"]).isoformat() + "Z"
        lines.append(f"[{ts}] ch:{r['channel_id']} msg:{r['message_id']} author:{r['author_hash'][:8]} score:{r['score']:.3f}")
    return "\n".join(lines)

__all__ = ["parse_time_range", "RateLimiter", "audit_log", "build_context_snippet"]


class _GuildPermsProto(Protocol):  # pragma: no cover - structural typing
    manage_messages: bool


class _RoleProto(Protocol):  # pragma: no cover
    id: int


class _MemberProto(Protocol):  # pragma: no cover
    roles: Iterable[_RoleProto]
    guild_permissions: _GuildPermsProto


def has_admin_access(member: _MemberProto, admin_role_ids: set[int]) -> bool:
    """Return True if member has admin access according to configured roles / fallback.

    If admin_role_ids is non-empty, require intersection; otherwise fallback to manage_messages perm.
    """
    if admin_role_ids:
        role_ids = {r.id for r in getattr(member, "roles", [])}
        return bool(role_ids & admin_role_ids)
    return bool(getattr(member, "guild_permissions", None) and member.guild_permissions.manage_messages)

__all__.append("has_admin_access")
