import asyncio
import types
import time
import tempfile
from pathlib import Path

from bot.ingest import Ingestor
from bot.config import settings

settings.lightweight_embeddings = True  # type: ignore

def _fake_message(msg_id: int, channel_id: int, author_id: int, content: str):
    m = types.SimpleNamespace(
        id=msg_id,
        channel=types.SimpleNamespace(id=channel_id),
        author=types.SimpleNamespace(id=author_id),
        created_at=types.SimpleNamespace(timestamp=lambda: time.time()),
        content=content,
    )
    return m

def test_hybrid_keyword_then_semantic():
    tmp = tempfile.mkdtemp(prefix="hybrid_store_")
    settings.data_dir = tmp  # type: ignore[attr-defined]
    Path(tmp).mkdir(parents=True, exist_ok=True)
    ing = Ingestor()
    msgs = [
        _fake_message(101, 5, 1, "vector alpha beta"),
        _fake_message(102, 5, 2, "gamma delta epsilon"),
    ]
    asyncio.run(ing.ingest_messages(msgs))
    hashed = ing.hash_query_tokens(["alpha"])
    mids = ing.keyword_filter(hashed)
    assert 101 in mids and 102 not in mids
    q_vec = ing.model.encode("alpha").tolist()
    results = ing.search(q_vec, top_k=3, message_ids=mids)
    assert any(r["message_id"] == 101 for r in results)
