from __future__ import annotations

"""Maintenance utilities (e.g., rotating hashing salt).

Salt rotation strategy:
Because raw user IDs or message tokens are never persisted (only salted hashes),
we cannot re-derive new salted hashes for historical records. Therefore a salt
rotation necessarily starts a fresh store. We archive the previous parquet
files into a timestamped backup directory and update the in-memory settings
object so future ingests use the new salt.

Operational steps performed by rotate_salt:
1. Create backup directory under data_dir named `backup_<unix_ts>`.
2. Move message_vectors.parquet and message_token_hashes.parquet there if they exist.
3. (Optional) Leave doc_embeddings.json in place (it does not contain salted identifiers).
4. Update settings.hash_salt to the provided new value (SecretStr).

After calling rotate_salt, restart the bot process in production so any long-
lived references to the old settings are discarded. New ingests will rebuild
the inverted index lazily.
"""

from pathlib import Path
import time
import shutil
import argparse
import sys
from pydantic import SecretStr

from .config import settings

HASHED_FILES = [
    "message_vectors.parquet",
    "message_token_hashes.parquet",
]


def rotate_salt(new_salt: str, archive: bool = True) -> str:
    """Rotate the hashing salt for user & token hashes.

    Returns the backup directory path used.
    """
    new_salt = new_salt.strip()
    if not new_salt or len(new_salt) < 8:
        raise ValueError("New salt must be at least 8 characters for entropy.")
    # Short-circuit if same (no-op)
    if settings.hash_salt.get_secret_value() == new_salt:
        raise ValueError("New salt must differ from current salt.")
    data_dir = Path(settings.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    backup_dir = data_dir / f"backup_{int(time.time())}"
    backup_dir.mkdir(parents=True, exist_ok=False)
    if archive:
        for fname in HASHED_FILES:
            src = data_dir / fname
            if src.exists():
                shutil.move(str(src), backup_dir / fname)
    # Update in-memory settings for this process; callers should persist (e.g., .env) separately
    settings.hash_salt = SecretStr(new_salt)  # type: ignore[attr-defined]
    return str(backup_dir)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Rotate hashing salt for Socionics bot stores.")
    p.add_argument("new_salt", help="New salt value (min 8 chars, keep secret!)")
    p.add_argument("--no-archive", action="store_true", help="Do not move existing parquet files (they will remain and become orphaned).")
    p.add_argument("-y", "--yes", action="store_true", help="Skip interactive confirmation.")
    return p.parse_args(argv)


def cli(argv: list[str] | None = None) -> int:
    ns = _parse_args(argv or sys.argv[1:])
    if not ns.yes:
        resp = input("Rotate salt and archive current hashed stores? This resets ingested message vectors. [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1
    backup_dir = rotate_salt(ns.new_salt, archive=not ns.no_archive)
    print(f"New salt applied. Backup directory: {backup_dir}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(cli())

__all__ = ["rotate_salt", "cli"]
