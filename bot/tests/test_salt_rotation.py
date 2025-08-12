import os
from importlib import reload
from pathlib import Path


class _Msg:
    def __init__(self, channel_id: int, message_id: int, author_id: int, content: str):
        from datetime import datetime, timezone

        class _Chan:
            def __init__(self, cid: int):
                self.id = cid

        class _Author:
            def __init__(self, aid: int):
                self.id = aid

        self.channel = _Chan(channel_id)
        self.id = message_id
        self.author = _Author(author_id)
        self.content = content
        self.created_at = datetime.now(timezone.utc)


async def ingest_helper(ing, messages):
    await ing.ingest_messages(messages)


def test_rotate_salt_archives_and_changes_hash(tmp_path, monkeypatch):
    # Point data dir to tmp and set initial salt
    monkeypatch.setenv("SOCIONICS_DATA_DIR", str(tmp_path / "store"))
    monkeypatch.setenv("SOCIONICS_HASH_SALT", "oldsalt_12345678")
    # Ensure lightweight embeddings for speed
    monkeypatch.setenv("SOCIONICS_LIGHTWEIGHT_EMBEDDINGS", "true")
    monkeypatch.setenv("SOCIONICS_DISCORD_TOKEN", "dummy_token")
    # Reload config so env vars take effect
    import bot.config as cfg
    reload(cfg)
    # Import after reload so settings reflect patched env
    import bot.ingest as ingest_mod
    import bot.maintenance as maint_mod
    reload(ingest_mod)
    reload(maint_mod)
    Ingestor = ingest_mod.Ingestor  # type: ignore[attr-defined]
    rotate_salt = maint_mod.rotate_salt  # type: ignore[attr-defined]
    ing = Ingestor()
    msg = _Msg(1, 1001, 42, "Hello world")
    import asyncio

    asyncio.run(ingest_helper(ing, [msg]))
    first_hash = ing._hash_user(42)
    # Force save since ingest helper may not have triggered persistence depending on internal state
    if hasattr(ing, "_save"):
        ing._save()  # type: ignore[attr-defined]
    store_file = Path(cfg.settings.data_dir) / "message_vectors.parquet"
    # Parquet should exist after save
    assert store_file.exists(), f"Expected parquet not found at {store_file}"
    backup_dir = rotate_salt("newsalt_ABCDEFGH")
    assert Path(backup_dir).exists()
    # After rotation, original parquet should be moved
    assert not store_file.exists()
    # New ingestor with same author should produce different hash
    ing2 = Ingestor()
    asyncio.run(ingest_helper(ing2, [_Msg(1, 1002, 42, "Another message")]))
    new_hash = ing2._hash_user(42)
    assert new_hash != first_hash


