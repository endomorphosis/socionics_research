from __future__ import annotations

from typing import Any
from pathlib import Path
import importlib.util

import orjson


def _load_ipfs_module():
    # Try normal import first
    try:
        from ipdb.ipfs_multiformats import (  # type: ignore
            ipfs_multiformats_py,
            is_valid_cid as _is_valid,
            create_cid_from_bytes as _create_cid,
        )
        return ipfs_multiformats_py, _is_valid, _create_cid
    except Exception:
        pass
    # Try to load from repository path
    here = Path(__file__).resolve()
    repo_root = here.parents[3] if len(here.parents) >= 4 else here.parents[-1]
    candidate = repo_root / "ipdb" / "ipfs_multiformats.py"
    if candidate.exists():
        spec = importlib.util.spec_from_file_location("_ipfs_multiformats", str(candidate))
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)  # type: ignore[attr-defined]
            return mod.ipfs_multiformats_py, mod.is_valid_cid, getattr(mod, "create_cid_from_bytes")
    # As a final fallback, define minimal helpers
    def _create_cid_from_bytes(data: bytes) -> str:
        import hashlib, base64

        h = hashlib.sha256(data).digest()
        cid_bytes = bytes([0x01, 0x55, 0x12, 32]) + h
        return "b" + base64.b32encode(cid_bytes).decode("utf-8").lower().replace("=", "")

    class _shim:
        def __init__(self):
            pass

        def get_cid(self, content_or_path):
            if isinstance(content_or_path, (bytes, bytearray)):
                data = bytes(content_or_path)
            else:
                data = str(content_or_path).encode()
            return _create_cid_from_bytes(data)

    def _is_valid(_cid: str) -> bool:
        return isinstance(_cid, str) and _cid.startswith("baf") and len(_cid) > 20

    return _shim, _is_valid, _create_cid_from_bytes


_ipfs_class, _is_valid_cid_module, _create_cid_from_bytes = _load_ipfs_module()
_cid = _ipfs_class(metadata={"testing": False}) if hasattr(_ipfs_class, "__call__") else _ipfs_class()


def canonical_json_bytes(obj: Any) -> bytes:
    return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS)


def cid_from_object(obj: Any) -> str:
    content = canonical_json_bytes(obj)
    cid = _cid.get_cid(content)
    if not _is_valid_cid_module(cid):
        cid = _create_cid_from_bytes(content)
    return cid


def is_valid_cid(cid: str) -> bool:
    return _is_valid_cid_module(cid)


__all__ = ["cid_from_object", "is_valid_cid", "canonical_json_bytes"]
