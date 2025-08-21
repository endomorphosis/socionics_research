#!/usr/bin/env python3
from bot.pdb_cli import main
import sys

def run(query: str, out_path: str, *, contains: str | None = None, regex: str | None = None, top: int = 200, index_path: str = "data/bot_store/pdb_faiss_char.index"):
    argv = [
        "pdb_cli.py",
        "search-characters",
        query,
        "--top",
        str(top),
        "--index",
        index_path,
        "--save-csv",
        out_path,
    ]
    if regex:
        argv += ["--regex", regex]
    elif contains:
        argv += ["--contains", contains]
    saved = list(sys.argv)
    sys.argv = argv
    try:
        main()
    finally:
        sys.argv = saved

if __name__ == "__main__":
    run("harry potter", "data/bot_store/harry_potter_hits.csv", contains="harry")
    # Anchor batman as a word to avoid unrelated "bats"/"bat" noise
    run("batman", "data/bot_store/batman_hits.csv", regex=r"\bBatman\b|\bBruce Wayne\b|\bThe Dark Knight\b")
    # More precise star wars filtering
    run("star wars", "data/bot_store/star_wars_hits.csv", regex=r"\bStar Wars\b|\bLuke Skywalker\b|\bDarth Vader\b|\bHan Solo\b|\bLeia Organa\b|\bObi-Wan\b|\bAnakin Skywalker\b|\bThe Mandalorian\b")
    print("Exported harry_potter_hits.csv, batman_hits.csv, star_wars_hits.csv")
