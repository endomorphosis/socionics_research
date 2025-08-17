#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

LOG="post_scan_report.log"
echo "[post-scan] Waiting for scan-all and v1 scrape to finish..." | tee "$LOG"

while pgrep -f "python.*-m bot\.pdb_cli .*scan-all" >/dev/null 2>&1; do
  sleep 20
done

echo "[post-scan] Scans finished. Running coverage, ingest-report, and export..." | tee -a "$LOG"
export PYTHONPATH=bot/src
{
  echo "--- coverage ---"
  python -m bot.pdb_cli coverage --sample 20
  echo "--- ingest-report ---"
  python -m bot.pdb_cli ingest-report --top-queries 20
  echo "--- export ---"
  python -m bot.pdb_cli export --out data/bot_store/pdb_profiles_normalized.parquet
  echo "--- summarize ---"
  python -m bot.pdb_cli summarize --normalized data/bot_store/pdb_profiles_normalized.parquet
} | tee -a "$LOG"

echo "[post-scan] Done." | tee -a "$LOG"
