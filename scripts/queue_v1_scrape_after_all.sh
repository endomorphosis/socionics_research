#!/usr/bin/env bash
set -euo pipefail

# Wait until no running scan-all process remains, then run a v1 scrape pass.
# Usage: just run in repo root: scripts/queue_v1_scrape_after_all.sh

cd "$(dirname "$0")/.."

echo "[queue-v1] Waiting for all scan-all runs to finish..."
while pgrep -f "python.*-m bot\.pdb_cli .*scan-all" >/dev/null 2>&1; do
  sleep 30
done

echo "[queue-v1] All scan-all runs appear finished. Launching v1 scrape-only pass..."

export PYTHONPATH=bot/src
# v1 base URL and headers (reuse secrets headers file if present)
V1_BASE=${PDB_V1_BASE_URL:-https://api.personality-database.com/api/v1}
V1_HEADERS_JSON=""
if [[ -f .secrets/pdb_headers.json ]]; then
  V1_HEADERS_JSON="$(cat .secrets/pdb_headers.json)"
fi

exec python -m bot.pdb_cli scan-all \
  --scrape-v1 \
  --v1-base-url "$V1_BASE" \
  --v1-headers "$V1_HEADERS_JSON" \
  --pages 1 \
  --sweep-pages 1 \
  --max-no-progress-pages 2 \
  --max-iterations 0 \
  --auto-embed \
  --auto-index \
  --index-out data/bot_store/pdb_faiss.index
