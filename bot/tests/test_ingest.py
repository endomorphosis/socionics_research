import types
import time
import asyncio
import tempfile
from pathlib import Path

from bot.ingest import Ingestor
from bot.config import settings

# Force lightweight embeddings for test speed
settings.lightweight_embeddings = True  # type: ignore


def _fake_message(msg_id: int, channel_id: int, author_id: int, content: str, ts: float | None = None):
    m = types.SimpleNamespace()
    m.id = msg_id
    m.channel = types.SimpleNamespace(id=channel_id)
    m.author = types.SimpleNamespace(id=author_id)
    if ts is None:
        ts = time.time()
    m.created_at = types.SimpleNamespace(timestamp=lambda: ts)
    m.content = content
    return m


def _with_temp_store():
    tmp = tempfile.mkdtemp(prefix="ingest_test_")
    settings.data_dir = tmp  # type: ignore[attr-defined]
    Path(tmp).mkdir(parents=True, exist_ok=True)
    return tmp


def test_ingest_and_search():
    _with_temp_store()
    ing = Ingestor()
    msgs = [
        _fake_message(1, 10, 200, "alpha beta gamma"),
        _fake_message(2, 10, 201, "beta delta"),
    ]
    added = asyncio.run(ing.ingest_messages(msgs))
    assert added == 2
    q_vec = ing.model.encode("alpha").tolist()
    results = ing.search(q_vec, top_k=2)
    assert len(results) <= 2
    assert any(r["message_id"] == 1 for r in results)


def test_keyword_filter():
    _with_temp_store()
    ing = Ingestor()
    msgs = [
        _fake_message(3, 11, 300, "epsilon zeta"),
        _fake_message(4, 11, 301, "zeta eta theta"),
    ]
    asyncio.run(ing.ingest_messages(msgs))
    hashed = ing.hash_query_tokens(["zeta"])
    mids = ing.keyword_filter(hashed)
    assert 3 in mids or 4 in mids


def test_purge_message():
    _with_temp_store()
    ing = Ingestor()
    msg = _fake_message(50, 22, 999, "purge target")
    asyncio.run(ing.ingest_messages([msg]))
    assert any(r["message_id"] == 50 for r in ing.search(ing.model.encode("purge").tolist(), top_k=5))
    removed = ing.purge_message(50)
    assert removed == 1
    results_after = ing.search(ing.model.encode("purge").tolist(), top_k=5)
    assert all(r["message_id"] != 50 for r in results_after)
