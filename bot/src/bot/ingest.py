from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import pandas as pd
from sentence_transformers import SentenceTransformer

from .config import settings


@dataclass
class IngestRecord:
    channel_id: int
    message_id: int
    author_hash: str
    created_ts: float
    vector: list[float]


class Ingestor:
    """Privacy-preserving message vector store (no raw text persisted)."""

    def __init__(self) -> None:
        # Optional lightweight embedder for testing environments
        if settings.lightweight_embeddings:
            class _Lite:
                def encode(self, text: str):  # simple hash-based vector returning np.array
                    import math
                    import numpy as np
                    buckets = [0.0] * 64
                    for tok in text.split():
                        h = int(hashlib.sha256(tok.lower().encode()).hexdigest(), 16)
                        buckets[h % 64] += 1
                    norm = math.sqrt(sum(v * v for v in buckets)) or 1.0
                    arr = np.array([v / norm for v in buckets], dtype=float)
                    return arr
            self.model = _Lite()
        else:
            self.model = SentenceTransformer(settings.embed_model)
        self.store_path = Path(settings.data_dir) / "message_vectors.parquet"
        self.tokens_path = Path(settings.data_dir) / "message_token_hashes.parquet"
        self._df_cache: pd.DataFrame | None = None
        self._tokens_cache: pd.DataFrame | None = None
        self._inverted_index: dict[str, set[int]] | None = None
        Path(settings.data_dir).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _hash_user(user_id: int) -> str:
        salt = settings.hash_salt.get_secret_value().encode()
        return hashlib.sha256(salt + str(user_id).encode()).hexdigest()[:32]

    def _load(self) -> pd.DataFrame:
        if self._df_cache is not None:
            return self._df_cache
        if self.store_path.exists():
            self._df_cache = pd.read_parquet(self.store_path)
        else:
            self._df_cache = pd.DataFrame(
                columns=[
                    "channel_id",
                    "message_id",
                    "author_hash",
                    "created_ts",
                    "vector",
                ]
            )
        return self._df_cache

    def _save(self) -> None:
        if self._df_cache is not None:
            self._df_cache.to_parquet(self.store_path, index=False)
        if self._tokens_cache is not None:
            self._tokens_cache.to_parquet(self.tokens_path, index=False)
    # inverted index is rebuilt on demand; not persisted yet

    def _load_tokens(self) -> pd.DataFrame:
        if self._tokens_cache is not None:
            return self._tokens_cache
        if self.tokens_path.exists():
            self._tokens_cache = pd.read_parquet(self.tokens_path)
        else:
            self._tokens_cache = pd.DataFrame(columns=["message_id", "token_hashes"])
        return self._tokens_cache

    def _build_inverted_index(self) -> None:
        token_df = self._load_tokens()
        index: dict[str, set[int]] = {}
        for _, row in token_df.iterrows():
            mid = int(row.message_id)
            for th in row.token_hashes:
                index.setdefault(th, set()).add(mid)
        self._inverted_index = index

    @staticmethod
    def _hash_token(token: str) -> str:
        salt = settings.hash_salt.get_secret_value().encode()
        return hashlib.sha256(salt + token.lower().encode()).hexdigest()[:16]

    async def ingest_messages(self, messages: Sequence) -> int:  # Sequence[discord.Message]
        df = self._load()
        existing_ids = set(zip(df.channel_id.astype(int), df.message_id.astype(int))) if not df.empty else set()
        new_rows = []
        token_df = self._load_tokens()
        token_existing = set(token_df.message_id.astype(int)) if not token_df.empty else set()
        token_rows = []
        for m in messages:  # type: ignore[assignment]
            key = (int(m.channel.id), int(m.id))
            if key in existing_ids:
                continue
            content = (m.content or "").strip()
            if not content:
                continue
            vec = self.model.encode(content).tolist()
            new_rows.append(
                {
                    "channel_id": int(m.channel.id),
                    "message_id": int(m.id),
                    "author_hash": self._hash_user(int(m.author.id)),
                    "created_ts": m.created_at.timestamp(),
                    "vector": vec,
                }
            )
            if int(m.id) not in token_existing:
                # simple whitespace tokenization; could swap for better later
                tokens = [t for t in content.split() if t]
                token_hashes = [self._hash_token(t) for t in tokens[:100]]  # cap to first 100 tokens
                token_rows.append({"message_id": int(m.id), "token_hashes": token_hashes})
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            if df.empty:
                df = new_df
            else:
                df = pd.concat([df, new_df], ignore_index=True, copy=False)
            self._df_cache = df
        if token_rows:
            new_token_df = pd.DataFrame(token_rows)
            if token_df.empty:
                token_df = new_token_df
            else:
                token_df = pd.concat([token_df, new_token_df], ignore_index=True, copy=False)
            self._tokens_cache = token_df
        if new_rows or token_rows:
            self._save()
            self._inverted_index = None  # invalidate index
        return len(new_rows)

    def keyword_filter(self, hashed_tokens: list[str]) -> set[int]:
        """Return message_ids containing all hashed tokens (intersection)."""
        token_df = self._load_tokens()
        if token_df.empty or not hashed_tokens:
            return set()
        if self._inverted_index is None:
            self._build_inverted_index()
        assert self._inverted_index is not None
        sets = []
        for h in hashed_tokens:
            s = self._inverted_index.get(h)
            if not s:
                return set()
            sets.append(s)
        # intersection
        result = set.intersection(*sets) if sets else set()
        return result

    def hash_query_tokens(self, tokens: list[str]) -> list[str]:
        return [self._hash_token(t) for t in tokens]

    def search(
        self,
        query_vec: list[float],
        top_k: int = 5,
        channel_id: int | None = None,
        author_hash: str | None = None,
        start_ts: float | None = None,
        end_ts: float | None = None,
    message_ids: set[int] | None = None,
    ) -> list[dict]:
        import numpy as np

        df = self._load()
        if df.empty:
            return []
        mask = [True] * len(df)
        if channel_id is not None:
            mask = mask & (df.channel_id.astype(int) == int(channel_id))  # type: ignore[operator]
        if author_hash is not None:
            mask = mask & (df.author_hash == author_hash)  # type: ignore[operator]
        if start_ts is not None:
            mask = mask & (df.created_ts >= start_ts)  # type: ignore[operator]
        if end_ts is not None:
            mask = mask & (df.created_ts <= end_ts)  # type: ignore[operator]
        filtered = df[mask]
        if message_ids is not None and len(message_ids) > 0:
            filtered = filtered[filtered.message_id.astype(int).isin(message_ids)]
        if filtered.empty:
            return []
        q = np.array(query_vec)
        mat = np.vstack(filtered.vector.to_list())
        # cosine similarity
        denom = (np.linalg.norm(mat, axis=1) * (np.linalg.norm(q) + 1e-9))
        scores = (mat @ q) / (denom + 1e-9)
        idx = np.argsort(-scores)[:top_k]
        results = []
        for i in idx:
            row = filtered.iloc[int(i)]
            results.append(
                {
                    "channel_id": int(row.channel_id),
                    "message_id": int(row.message_id),
                    "author_hash": row.author_hash,
                    "created_ts": float(row.created_ts),
                    "score": float(scores[i]),
                }
            )
        return results

    def purge_message(self, message_id: int) -> int:
        """Remove a message and its token hashes. Returns number removed (0 or 1)."""
        df = self._load()
        if df.empty:
            return 0
        before = len(df)
        df = df[df.message_id.astype(int) != int(message_id)]
        self._df_cache = df
        tdf = self._load_tokens()
        if not tdf.empty:
            tdf = tdf[tdf.message_id.astype(int) != int(message_id)]
            self._tokens_cache = tdf
        self._save()
        # invalidate index
        self._inverted_index = None
        return before - len(df)

__all__ = ["Ingestor"]
