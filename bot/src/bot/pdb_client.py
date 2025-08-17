from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
import hashlib
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter


import os as _os
BASE_URL = _os.getenv("PDB_API_BASE_URL", "https://api.personality-database.com/api/v1")


class RateLimitError(Exception):
    pass


@dataclass
class PdbClient:
    concurrency: int = 4
    rate_per_minute: int = 60
    timeout_s: float = 20.0
    base_url: Optional[str] = None
    headers: Optional[Dict[str, str]] = None

    def __post_init__(self) -> None:
        # Allow env to override defaults
        try:
            self.concurrency = int(os.getenv("PDB_CONCURRENCY", str(self.concurrency)))
        except Exception:
            pass
        try:
            self.rate_per_minute = int(os.getenv("PDB_RPM", str(self.rate_per_minute)))
        except Exception:
            pass
        try:
            self.timeout_s = float(os.getenv("PDB_TIMEOUT_S", str(self.timeout_s)))
        except Exception:
            pass
        # Base URL can be passed or come from env; fallback to module default
        if not self.base_url:
            self.base_url = os.getenv("PDB_API_BASE_URL", BASE_URL)
        self._sem = asyncio.Semaphore(self.concurrency)
        self._interval = max(1.0 / max(self.rate_per_minute / 60.0, 1e-6), 0.0)
        self._last_call = 0.0
        extra: Dict[str, str] = {}
        token = os.getenv("PDB_API_TOKEN")
        if token:
            extra["Authorization"] = f"Bearer {token}"
        hdrs_json = os.getenv("PDB_API_HEADERS")
        if hdrs_json:
            try:
                import json as _json
                extra.update(_json.loads(hdrs_json))
            except Exception:
                pass
        # Merge explicit headers last to allow CLI/user override
        if self.headers:
            try:
                extra.update(self.headers)
            except Exception:
                pass
        self._extra_headers = extra
        # simple file cache for GETs
        self._cache_enabled = os.getenv("PDB_CACHE", "0").lower() in {"1", "true", "yes"}
        cache_dir = os.getenv("PDB_CACHE_DIR") or os.path.join("data", "bot_store", "pdb_api_cache")
        self._cache_dir = Path(cache_dir)
        if self._cache_enabled:
            self._cache_dir.mkdir(parents=True, exist_ok=True)

    async def _throttle(self) -> None:
        import time

        now = time.time()
        wait = self._interval - (now - self._last_call)
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_call = time.time()

    def _cache_key(self, url: str, params: Optional[Dict[str, Any]]) -> Path:
        hasher = hashlib.sha256()
        hasher.update(url.encode())
        if params:
            try:
                import json as _json
                hasher.update(_json.dumps(params, sort_keys=True).encode())
            except Exception:
                hasher.update(str(params).encode())
        return self._cache_dir / (hasher.hexdigest() + ".json")

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        base = self.base_url or BASE_URL
        url = f"{base.rstrip('/')}/{path.lstrip('/')}"
        # cache read
        if self._cache_enabled:
            key = self._cache_key(url, params)
            if key.exists():
                try:
                    txt = key.read_text(encoding="utf-8")
                    import json as _json
                    return _json.loads(txt)
                except Exception:
                    pass
        async with self._sem:
            await self._throttle()
            base_headers = {
                "User-Agent": os.getenv(
                    "PDB_DEFAULT_UA",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                ),
                "Accept": os.getenv(
                    "PDB_DEFAULT_ACCEPT",
                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                ),
                "Accept-Language": os.getenv("PDB_DEFAULT_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
                # Many v2 endpoints respond more richly when these are present
                "Origin": os.getenv("PDB_DEFAULT_ORIGIN", "https://www.personality-database.com"),
                "Referer": os.getenv("PDB_DEFAULT_REFERER", "https://www.personality-database.com/"),
            }
            base_headers.update(self._extra_headers)
            async with httpx.AsyncClient(timeout=self.timeout_s, headers=base_headers) as client:
                async for attempt in AsyncRetrying(
                    reraise=True,
                    stop=stop_after_attempt(5),
                    wait=wait_exponential_jitter(initial=0.5, max=10),
                    retry=retry_if_exception_type((httpx.HTTPError, RateLimitError)),
                ):
                    with attempt:
                        resp = await client.get(url, params=params)
                        if resp.status_code == 429:
                            raise RateLimitError("rate limited")
                        resp.raise_for_status()
                        enc = (resp.headers.get("content-encoding") or "").lower()
                        ctype = (resp.headers.get("content-type") or "").lower()
                        body = resp.content
                        # Try direct parse first when JSON content-type
                        if "application/json" in ctype:
                            try:
                                import orjson as _orjson  # type: ignore
                                data = _orjson.loads(body)
                                if self._cache_enabled:
                                    pass
                                return data
                            except Exception:
                                pass
                        # Detect encoders by header or magic bytes
                        def _try_parse(b: bytes):
                            try:
                                import orjson as _orjson  # type: ignore
                                return _orjson.loads(b)
                            except Exception:
                                import json as _json
                                return _json.loads(b.decode("utf-8"))

                        parsed = None
                        # zstd
                        try:
                            is_zstd = ("zstd" in enc or "zst" in enc) or (
                                len(body) >= 4 and body[0] == 0x28 and body[1] == 0xB5 and body[2] == 0x2F and body[3] == 0xFD
                            )
                            if is_zstd:
                                import zstandard as zstd  # type: ignore
                                dctx = zstd.ZstdDecompressor()
                                raw = dctx.decompress(body)
                                parsed = _try_parse(raw)
                        except Exception:
                            parsed = None
                        # brotli
                        if parsed is None:
                            try:
                                if "br" in enc:
                                    import brotli  # type: ignore
                                    raw = brotli.decompress(body)
                                    parsed = _try_parse(raw)
                            except Exception:
                                parsed = None
                        # gzip
                        if parsed is None:
                            try:
                                if "gzip" in enc or (len(body) >= 2 and body[0] == 0x1F and body[1] == 0x8B):
                                    import gzip as _gzip
                                    raw = _gzip.decompress(body)
                                    parsed = _try_parse(raw)
                            except Exception:
                                parsed = None
                        # deflate (zlib)
                        if parsed is None:
                            try:
                                if "deflate" in enc:
                                    import zlib as _zlib
                                    raw = _zlib.decompress(body)
                                    parsed = _try_parse(raw)
                            except Exception:
                                parsed = None
                        if parsed is not None:
                            data = parsed
                        else:
                            # Last resort
                            data = resp.json()
                        if self._cache_enabled:
                            try:
                                import json as _json
                                key = self._cache_key(url, params)
                                key.write_text(_json.dumps(data), encoding="utf-8")
                            except Exception:
                                pass
                        return data

    async def fetch_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return await self._get(path, params)

    async def iter_profiles(self, cid: int, pid: int, limit: int = 100, start_offset: int = 0) -> AsyncIterator[dict]:
        offset = start_offset
        while True:
            params = {"offset": offset, "limit": limit, "cid": cid, "pid": pid, "cat_id": cid, "property_id": pid}
            data = await self._get("profiles", params)
            items = (
                data
                if isinstance(data, list)
                else data.get("data")
                or data.get("results")
                or data.get("profiles")
                or []
            )
            if not items:
                break
            for it in items:
                yield it
            if len(items) < limit:
                break
            offset += limit

    async def get_profile(self, profile_id: int) -> dict:
        # v1 single profile endpoint uses singular 'profile/{id}'
        return await self._get(f"profile/{profile_id}")

    async def iter_profiles_any(self, limit: int = 100, start_offset: int = 0) -> AsyncIterator[dict]:
        offset = start_offset
        while True:
            params = {"offset": offset, "limit": limit}
            data = await self._get("profiles", params)
            items = (
                data
                if isinstance(data, list)
                else data.get("data")
                or data.get("results")
                or data.get("profiles")
                or []
            )
            if not items:
                break
            for it in items:
                yield it
            if len(items) < limit:
                break
            offset += limit
