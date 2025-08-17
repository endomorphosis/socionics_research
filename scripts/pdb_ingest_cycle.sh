#!/usr/bin/env bash
set -euo pipefail

# Configurable defaults (override via env or flags)
: "${PDB_BASE:=https://api.personality-database.com/api/v2}"
: "${PDB_HEADERS:=}"
: "${PDB_RPM:=120}"
: "${PDB_CONCURRENCY:=6}"
: "${PDB_TIMEOUT_S:=30}"
: "${MAX_KEYS:=10}"
: "${PAGES:=2}"
: "${LIMIT:=20}"
: "${INDEX_OUT:=data/bot_store/pdb_faiss.index}"
: "${ONLY_PROFILES:=}"
: "${LISTS:=}"
: "${EXPAND_SUBCATEGORIES:=}"
: "${FILTER_CHARACTERS:=}"
: "${EXPAND_MAX:=}"
: "${DRY_RUN:=}"
: "${UNTIL_EMPTY:=}"

usage() {
  cat <<EOF
Usage: PDB_HEADERS='{"User-Agent":"...","Referer":"...","Origin":"...","Cookie":"..."}' \\
  PDB_BASE=... PDB_RPM=... PDB_CONCURRENCY=... PDB_TIMEOUT_S=... \\
  MAX_KEYS=... PAGES=... LIMIT=... INDEX_OUT=... \\
  ./scripts/pdb_ingest_cycle.sh

Runs: hot-queries → follow-hot → export → summarize → ingest-report → index
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage; exit 0
fi

# Ensure venv active if present
if [[ -d .venv ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

export PYTHONPATH=bot/src
export PDB_CACHE=1
export PDB_API_BASE_URL="$PDB_BASE"
if [[ -n "$PDB_HEADERS" ]]; then
  export PDB_API_HEADERS="$PDB_HEADERS"
fi
export PDB_RPM PDB_CONCURRENCY PDB_TIMEOUT_S

set -x
python -m bot.pdb_cli hot-queries || true
EXTRA_FLAGS=()
if [[ -n "$ONLY_PROFILES" ]]; then EXTRA_FLAGS+=(--only-profiles); fi
if [[ -n "$LISTS" ]]; then EXTRA_FLAGS+=(--lists "$LISTS"); fi
if [[ -n "$EXPAND_SUBCATEGORIES" ]]; then EXTRA_FLAGS+=(--expand-subcategories); fi
if [[ -n "$FILTER_CHARACTERS" ]]; then EXTRA_FLAGS+=(--filter-characters); fi
if [[ -n "$EXPAND_MAX" ]]; then EXTRA_FLAGS+=(--expand-max "$EXPAND_MAX"); fi
if [[ -n "$DRY_RUN" ]]; then EXTRA_FLAGS+=(--dry-run); fi
if [[ -n "$UNTIL_EMPTY" ]]; then EXTRA_FLAGS+=(--until-empty); fi

python -m bot.pdb_cli follow-hot --max-keys "$MAX_KEYS" --limit "$LIMIT" --pages "$PAGES" \
  --auto-embed --auto-index --index-out "$INDEX_OUT" "${EXTRA_FLAGS[@]}"
python -m bot.pdb_cli export --out data/bot_store/pdb_profiles_normalized.parquet
python -m bot.pdb_cli summarize --normalized data/bot_store/pdb_profiles_normalized.parquet || true
python -m bot.pdb_cli ingest-report --top-queries 10 || true
set +x

echo "Ingest cycle complete. Index at: $INDEX_OUT"
