#!/usr/bin/env bash
set -euo pipefail
latest=$(ls -t /tmp/pdb_scan_all_*.log 2>/dev/null | head -n 1 || true)
if [[ -z "${latest}" ]]; then
  echo "No scan logs found under /tmp/pdb_scan_all_*.log"
  exit 1
fi
echo "Tailing ${latest} (Ctrl-C to stop)"
tail -f "$latest"