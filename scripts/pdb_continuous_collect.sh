#!/usr/bin/env bash
set -euo pipefail

# Continuous collection loop: discovery-first scan, sweeps, v1 backfill, and maintenance.
# Safe to run in background. Uses cache + persistent state. Requires headers in .secrets.

RPM=${RPM:-90}
CONCURRENCY=${CONCURRENCY:-3}
TIMEOUT=${TIMEOUT:-30}
PAGES=${PAGES:-3}
SWEEP_PAGES=${SWEEP_PAGES:-20}
MAX_NO_PROGRESS_PAGES=${MAX_NO_PROGRESS_PAGES:-3}
INITIAL_FRONTIER_SIZE=${INITIAL_FRONTIER_SIZE:-1000}
SLEEP_SECS=${SLEEP_SECS:-300} # 5 minutes between cycles by default
INDEX_OUT=${INDEX_OUT:-data/bot_store/pdb_faiss.index}
HEADERS_FILE=${HEADERS_FILE:-.secrets/pdb_headers.json}
V2_BASE_URL=${PDB_API_BASE_URL:-https://api.personality-database.com/api/v2}
V1_BASE_URL=${PDB_V1_BASE_URL:-https://api.personality-database.com/api/v1}

if [[ ! -f "$HEADERS_FILE" ]]; then
  echo "Missing headers file: $HEADERS_FILE" >&2
  exit 1
fi

export PYTHONPATH=bot/src
export PDB_CACHE=1
export PDB_API_BASE_URL="$V2_BASE_URL"
export PDB_API_HEADERS="$(tr -d '\n' < "$HEADERS_FILE")"
export PDB_RPM="$RPM" PDB_CONCURRENCY="$CONCURRENCY" PDB_TIMEOUT_S="$TIMEOUT"

bigram_tokens() {
  python - <<'PY'
import string
print(','.join(a+b for a in string.ascii_lowercase for b in string.ascii_lowercase))
PY
}

while true; do
  if [[ -f /tmp/pdb_continuous_collect.lock ]]; then
    echo "Lock present; another collector may be running. Sleeping..."
    sleep "$SLEEP_SECS"; continue
  fi
  trap 'rm -f /tmp/pdb_continuous_collect.lock' EXIT
  echo $$ > /tmp/pdb_continuous_collect.lock
  echo "$(date -Is) :: Starting continuous collection cycle"

  # 1) Trending queries → follow-hot
  python -m bot.pdb_cli hot-queries || true
  python -m bot.pdb_cli follow-hot --max-keys 50 --limit 20 --pages 2 \
    --auto-embed --auto-index --index-out "$INDEX_OUT" || true

  # 2) Discovery-first scan-all with sweeps and until-empty guards
  python -u -m bot.pdb_cli \
    --rpm "$RPM" --concurrency "$CONCURRENCY" --timeout "$TIMEOUT" \
    --base-url "$V2_BASE_URL" \
    --headers "$(tr -d '\n' < "$HEADERS_FILE")" \
    scan-all --max-iterations 0 \
    --initial-frontier-size "$INITIAL_FRONTIER_SIZE" \
    --search-names --limit 20 --pages "$PAGES" --until-empty \
    --sweep-queries "$(bigram_tokens)" --sweep-pages "$SWEEP_PAGES" --sweep-until-empty --sweep-into-frontier \
    --max-no-progress-pages "$MAX_NO_PROGRESS_PAGES" \
    --auto-embed --auto-index --index-out "$INDEX_OUT" \
    --scrape-v1 --v1-base-url "$V1_BASE_URL" --v1-headers "$(tr -d '\n' < "$HEADERS_FILE")" \
    --use-state || true

  # 3) Opportunistic v1 backfill for missing items
  python -m bot.pdb_cli --rpm "$RPM" --concurrency "$CONCURRENCY" --timeout "$TIMEOUT" \
    scrape-v1-missing --max 25 --shuffle \
    --v1-base-url "$V1_BASE_URL" --v1-headers "$(tr -d '\n' < "$HEADERS_FILE")" || true

  # 4) Export + quick summaries
  python -m bot.pdb_cli export --out data/bot_store/pdb_profiles_normalized.parquet || true
  python -m bot.pdb_cli edges-analyze --top 3 --per-component-top 5 || true
  python -m bot.pdb_cli coverage --sample 10 || true

  echo "$(date -Is) :: Cycle complete. Sleeping for $SLEEP_SECS seconds..."
  sleep "$SLEEP_SECS"
  rm -f /tmp/pdb_continuous_collect.lock
  trap - EXIT

done
