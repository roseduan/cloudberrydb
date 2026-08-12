#!/bin/bash
# monitor/system_metrics.sh
# SOAK-LOOP: scope=global interval=60 interval_var=SOAK_METRICS_EVERY order=70
#
# One sample per invocation, appended to system_metrics.csv:
#
#   timestamp, disk_used_pct, disk_used_gb, free_gb,
#   bgw_workers, bgw_rss_total_kb, bgw_rss_max_kb
#
# - disk_*: parsed from `df` for the data directory
# - bgw_*:  walked via /proc/<pid>/status for every BGW worker pid
#           returned by soak_worker_rss_kb
#
# PURE COLLECTION: disk_pct is only sampled here.  The disk-full
# early-stop DECISION (disk_pct ≥ SOAK_DISK_KILL_PCT → panic.disk_full)
# moved to judge/verdicts/framework_health.sh on 2026-07-09.  Sampling
# at SOAK_METRICS_EVERY (20 s) keeps that decision responsive without a
# separate high-frequency loop.
#
# Usage (invoked by lib/loops.sh):
#   system_metrics.sh <results_dir>            one collection pass

set -uo pipefail

SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$SOAK_DIR/lib/helpers.sh"

# ── Collection mode ──────────────────────────────────────────────────
RESULTS=${1:?usage: system_metrics.sh <results_dir>}
CSV="$RESULTS/data/system_metrics.csv"

# Header (only if file is empty)
if [[ ! -s "$CSV" ]]; then
  echo "ts,disk_pct,disk_used_gb,free_gb,bgw_workers,bgw_rss_total_kb,bgw_rss_max_kb" > "$CSV"
fi

DATA_DIR="${SOAK_DATA_DIR:-/home/gpadmin/gpdata}"

# df: %Use Used Avail.  Skip if path missing.
if [[ -d "$DATA_DIR" ]]; then
  read -r DISK_PCT DISK_USED_GB DISK_FREE_GB < <(
    df -BG --output=pcent,used,avail "$DATA_DIR" \
      | tail -1 \
      | tr -d 'G%'
  )
else
  DISK_PCT=0; DISK_USED_GB=0; DISK_FREE_GB=0
fi

# BGW workers: count + sum + max RSS
RSS_LINES=$(soak_worker_rss_kb)
N=$(echo "$RSS_LINES" | grep -c .)
SUM=$(echo "$RSS_LINES" | awk '{s+=$1} END {print s+0}')
MAX=$(echo "$RSS_LINES" | awk 'BEGIN{m=0} {if($1>m)m=$1} END {print m+0}')

printf '%s,%s,%s,%s,%s,%s,%s\n' \
  "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
  "$DISK_PCT" "$DISK_USED_GB" "$DISK_FREE_GB" \
  "$N" "$SUM" "$MAX" >> "$CSV"
