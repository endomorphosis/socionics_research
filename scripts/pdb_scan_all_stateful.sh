#!/usr/bin/env bash
set -euo pipefail

# Stateful discovery-first scan for PDB v2 + optional v1 scraping.
# Uses cache, persistent state, and no-progress guards. Safe to interrupt; state persists.

RPM=${RPM:-90}
CONCURRENCY=${CONCURRENCY:-3}
TIMEOUT=${TIMEOUT:-30}
PAGES=${PAGES:-3}
SWEEP_PAGES=${SWEEP_PAGES:-20}
SWEEP_QUERIES=${SWEEP_QUERIES:-a,b,c,d,e,f,g,h}
MAX_NO_PROGRESS_PAGES=${MAX_NO_PROGRESS_PAGES:-3}
INITIAL_FRONTIER_SIZE=${INITIAL_FRONTIER_SIZE:-1000}

V2_BASE_URL=${PDB_API_BASE_URL:-https://api.personality-database.com/api/v2}
V1_BASE_URL=${PDB_V1_BASE_URL:-https://api.personality-database.com/api/v1}

HEADERS_FILE=${HEADERS_FILE:-.secrets/pdb_headers.json}

INDEX_OUT=${INDEX_OUT:-data/bot_store/pdb_faiss.index}

if [[ ! -f "$HEADERS_FILE" ]]; then
  echo "Missing headers file: $HEADERS_FILE" >&2
  echo "Create it with browser-like headers and cookies from an active session." >&2
  exit 1
fi

echo "Running stateful scan-all with: RPM=$RPM CONCURRENCY=$CONCURRENCY PAGES=$PAGES SWEEP_PAGES=$SWEEP_PAGES"

export PDB_CACHE=1

trap 'echo; echo "Interrupted. State persisted at data/bot_store/scan_state.json"' INT TERM

PYTHONPATH=bot/src python -u -m bot.pdb_cli \
  --rpm "$RPM" --concurrency "$CONCURRENCY" --timeout "$TIMEOUT" \
  --base-url "$V2_BASE_URL" \
  --headers "$(tr -d '\n' < "$HEADERS_FILE")" \
  scan-all --max-iterations 0 \
  --initial-frontier-size "$INITIAL_FRONTIER_SIZE" \
  --search-names --limit 20 --pages "$PAGES" --until-empty \
  --sweep-queries "$SWEEP_QUERIES" --sweep-pages "$SWEEP_PAGES" --sweep-until-empty --sweep-into-frontier \
  --max-no-progress-pages "$MAX_NO_PROGRESS_PAGES" \
  --auto-embed --auto-index --index-out "$INDEX_OUT" \
  --scrape-v1 --v1-base-url "$V1_BASE_URL" --v1-headers "$(tr -d '\n' < "$HEADERS_FILE")" \
  --use-state "$@"

echo
echo "Coverage snapshot:"
PYTHONPATH=bot/src python -m bot.pdb_cli coverage --sample 10 || true
