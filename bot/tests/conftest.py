import os

# Configure environment for tests before importing project modules
os.environ.setdefault("SOCIONICS_LIGHTWEIGHT_EMBEDDINGS", "true")
os.environ.setdefault("SOCIONICS_HASH_SALT", "test_salt")
os.environ.setdefault("SOCIONICS_DISCORD_TOKEN", "dummy")
