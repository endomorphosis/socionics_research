from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import orjson
from sentence_transformers import SentenceTransformer

from .config import settings

DOC_EXTENSIONS = {".md", ".txt"}


def iter_docs(root: str | os.PathLike[str]) -> Iterable[tuple[str, str]]:
    root_path = Path(root)
    for path in root_path.rglob("*"):
        if path.suffix.lower() in DOC_EXTENSIONS and path.is_file():
            try:
                text = path.read_text(encoding="utf-8")
            except Exception:
                continue
            rel = path.relative_to(root_path).as_posix()
            yield rel, text


def build_vector_store(docs_root: str) -> None:
    data_dir = Path(settings.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    model = SentenceTransformer(settings.embed_model)
    records: list[dict[str, str | list[float]]] = []
    for rel, text in iter_docs(docs_root):
        emb = model.encode(text[:4000]).tolist()  # truncate overly long docs
        records.append({"path": rel, "embedding": emb})
    with (data_dir / "doc_embeddings.json").open("wb") as f:
        f.write(orjson.dumps(records))

def load_doc_embeddings() -> list[dict]:
    """Lazy load previously built doc embeddings; empty list if missing."""
    path = Path(settings.data_dir) / "doc_embeddings.json"
    if not path.exists():
        return []
    try:
        return orjson.loads(path.read_bytes())  # type: ignore[return-value]
    except Exception:
        return []

__all__ = ["build_vector_store", "load_doc_embeddings"]
