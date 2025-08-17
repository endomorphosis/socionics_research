from __future__ import annotations

from typing import Any, Dict


def normalize_profile(p: Dict[str, Any]) -> Dict[str, Any]:
    name = (
        p.get("name")
        or p.get("title")
        or p.get("display_name")
        or p.get("username")
        or p.get("slug")
        or ""
    )
    desc = (
        p.get("description")
        or p.get("bio")
        or p.get("biography")
        or p.get("about")
        or ""
    )
    # common type fields as seen in various datasets
    mbti = p.get("mbti") or p.get("mbti_type") or p.get("type_mbti")
    socionics = p.get("socionics") or p.get("socionics_type") or p.get("type_socionics")
    big5 = p.get("big5") or p.get("big_five")

    # fallback: look into nested attributes if present
    attrs = p.get("attributes") or p.get("props") or {}
    if not mbti:
        mbti = attrs.get("mbti") or attrs.get("mbti_type")
    if not socionics:
        socionics = attrs.get("socionics") or attrs.get("socionics_type")
    if not big5:
        big5 = attrs.get("big5") or attrs.get("big_five")

    # ensure strings
    def s(v):
        return v if isinstance(v, str) else (str(v) if v is not None else None)

    return {
        "name": s(name),
        "description": s(desc),
        "mbti": s(mbti),
        "socionics": s(socionics),
        "big5": s(big5),
    }
