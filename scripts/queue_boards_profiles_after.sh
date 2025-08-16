#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <prev_scan_pid>" >&2
  exit 1
fi

PID="$1"

# Change to repo root (script directory is scripts/)
cd "$(dirname "$0")/.."

echo "[queue] Waiting for PID $PID to exit before starting boards+profiles scan..."
while kill -0 "$PID" 2>/dev/null; do
  sleep 30
 done

echo "[queue] Previous scan finished. Launching boards+profiles scan-all..."

export PYTHONPATH=bot/src
export PDB_API_BASE_URL=${PDB_API_BASE_URL:-https://api.personality-database.com/api/v2}
if [[ -f .secrets/pdb_headers.json ]]; then
  export PDB_API_HEADERS="$(cat .secrets/pdb_headers.json)"
fi

exec python -m bot.pdb_cli scan-all \
  --lists boards,profiles \
  --sweep-into-frontier \
  --pages 1 \
  --sweep-pages 1 \
  --max-no-progress-pages 2 \
  --max-iterations 0 \
  --auto-embed \
  --auto-index \
  --index-out data/bot_store/pdb_faiss.index
