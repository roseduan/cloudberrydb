#!/bin/bash
# judge/verdicts/scrape_liveness.sh
# Verdict: are the per-cycle scrapers still receiving fresh samples?
#
# Motivated by SOAK-20260723_170208: soak_test_b's watermark_lag scrape
# stopped appending after 19:11 UTC while soak.sh ran until 22:04 — the
# scrape's SQL query itself was hanging on that DB, so the framework
# lost its progress signal for 3 hours with no alarm.  Meanwhile the
# refresh-perf/watermark judge sections happily reported "✓ OK" against
# their stale caches.
#
# What this verdict does:  for each CSV that is APPENDED PER-CYCLE
# (i.e. every scrape tick produces one row per db, regardless of
# whether the DB produced new signal), look at the newest ts per db
# and compute its age.  If age >= WARN_SEC → ⚠ WARN; if >= FAIL_SEC →
# 🔴 CRITICAL + verdicts/fail.framework.scrape_stale + signals/
# panic.scrape_stale so soak.sh exits early (a scrape that's been
# dead for FAIL_SEC will not resurrect on its own — the run has no
# reason to keep burning wall-clock).
#
# CONDITIONAL-append CSVs (refresh_durations, compress_durations,
# perf_probe, view_correctness) are DELIBERATELY skipped: they only
# grow when a bgw job / probe cycle produces a new row, so "no new
# row for 30 min" can be legitimate quiescence.  Their own verdicts
# already own the "stopped producing" question in the domain-native
# way.
#
# CHAOS-AWARE: during a cluster_crash the DB is unqueryable and
# per-cycle scrapes miss samples by design.  A newest-sample ts that
# fell inside a chaos window is granted extra grace before it fails.
#
# Timestamps are compared LEXICOGRAPHICALLY (ISO-8601 UTC sorts ==
# chronologically), so we can pre-compute two threshold timestamps
# via GNU date once and skip per-row shellouts.  This matches
# judge/lib/chaos.sh's approach.

_judge_eval_scrape_liveness() {
  local RESULTS="$1"
  mkdir -p "$RESULTS/verdicts" "$RESULTS/signals"
  local VERDICT_FILE="$RESULTS/verdicts/fail.framework.scrape_stale"
  local PANIC_FILE="$RESULTS/signals/panic.scrape_stale"
  local NOW; NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

  local WARN_SEC="${SOAK_SCRAPE_STALE_WARN_SEC:-300}"
  local FAIL_SEC="${SOAK_SCRAPE_STALE_FAIL_SEC:-900}"
  # Extra grace when the newest sample fell inside a chaos window
  # (added on top of FAIL_SEC before firing CRITICAL).
  local CHAOS_GRACE="${SOAK_SCRAPE_STALE_CHAOS_GRACE_SEC:-$WARN_SEC}"

  # Only cluster_crash makes a per-cycle scrape LEGITIMATELY skip
  # samples across DBs; worker/scheduler kills don't stop the
  # scrape's own psql from returning.
  local CW; CW=$(chaos_windows_str "$RESULTS/data/chaos_log.csv" \
                 "$CHAOS_ALL_FAULTS" 2>/dev/null)

  # (csv_basename, group_col_index)   group_col=0 → single global group.
  local -a TARGETS=(
    "watermark_lag.csv|2"
    "invalidation_log.csv|2"
    "bgw_scheduler_health.csv|2"
    "system_metrics.csv|0"
  )

  # Reference "now" = max(newest ts across every monitored CSV).  Not
  # wall-clock: a post-mortem `report.sh <dir>` on a run that ended
  # hours ago would otherwise mark every scrape stale.  The healthy
  # scrapes track wall-clock live so their newest ts is always within
  # seconds of it; the sick scrape's newest ts sits WAY behind the
  # rest.  Comparing every group against max(newest_ts) puts them all
  # on the same reference frame — the fastest-ticking scrape defines
  # "now", and any group lagging that by > threshold is stale.  This
  # holds during the live streaming judge_run tick as well (max ≈ wall).
  local REF_NOW=""
  local t
  local target
  for target in "${TARGETS[@]}"; do
    IFS='|' read -r name _gcol <<< "$target"
    [[ -s "$RESULTS/data/$name" ]] || continue
    t=$(tail -1 "$RESULTS/data/$name" | cut -d, -f1)
    [[ -n "$t" ]] || continue
    if [[ -z "$REF_NOW" || "$t" > "$REF_NOW" ]]; then REF_NOW="$t"; fi
  done
  [[ -z "$REF_NOW" ]] && REF_NOW="$NOW"
  # GNU date turns REF_NOW back into epoch → subtract → back to ISO.
  # Verdicts already assume a GNU-date container env (chaos.sh, etc.).
  local REF_EPOCH; REF_EPOCH=$(date -u -d "$REF_NOW" +%s 2>/dev/null || date -u +%s)
  local WARN_TS FAIL_TS FAIL_CHAOS_TS
  WARN_TS=$(date -u -d "@$(( REF_EPOCH - WARN_SEC ))" '+%Y-%m-%dT%H:%M:%SZ')
  FAIL_TS=$(date -u -d "@$(( REF_EPOCH - FAIL_SEC ))" '+%Y-%m-%dT%H:%M:%SZ')
  FAIL_CHAOS_TS=$(date -u -d "@$(( REF_EPOCH - FAIL_SEC - CHAOS_GRACE ))" '+%Y-%m-%dT%H:%M:%SZ')

  local any_fail=0
  local out=""
  for entry in "${TARGETS[@]}"; do
    IFS='|' read -r name gcol <<< "$entry"
    local csv="$RESULTS/data/$name"
    local label_name="${name%.csv}"
    if [[ ! -s "$csv" ]]; then
      out+=$(printf "  %-24s %s\n" "$label_name" "(no csv yet)")
      out+=$'\n'
      continue
    fi
    # Extract newest ts per group + whether that ts falls inside a
    # chaos window.  Awk output: "group|newest_ts|in_chaos".
    local extracted
    extracted=$(awk -F, \
        -v gcol="$gcol" \
        -v windows="$CW" '
      function in_chaos(t,  i,a,m,se){
        if (windows == "") return 0
        m = split(windows, a, " ")
        for (i=1;i<=m;i++) {
          split(a[i], se, "|")
          if (t >= se[1] && t <= se[2]) return 1
        }
        return 0
      }
      NR == 1 { next }
      {
        key = (gcol+0 > 0) ? $(gcol+0) : "-"
        if (!(key in newest) || $1 > newest[key]) newest[key] = $1
      }
      END {
        for (k in newest) printf "%s|%s|%d\n", k, newest[k], in_chaos(newest[k])
      }
    ' "$csv")

    while IFS='|' read -r group newest in_ch; do
      [[ -z "$group" ]] && continue
      local disp_group="$group"
      [[ "$group" == "-" ]] && disp_group="global"
      local hdr; hdr=$(printf "  %-14s %-24s" "$disp_group" "$label_name")
      local eff_fail_ts="$FAIL_TS"
      [[ "$in_ch" == "1" ]] && eff_fail_ts="$FAIL_CHAOS_TS"

      # Lex-compare newest against thresholds.  newest < FAIL_TS means
      # newest is OLDER than (NOW - FAIL_SEC) → stale beyond FAIL.
      if [[ "$newest" < "$eff_fail_ts" ]]; then
        out+="$hdr newest=${newest} 🔴 CRITICAL (>= ${FAIL_SEC}s stale$([[ "$in_ch" == "1" ]] && echo ", chaos-grace applied"))"$'\n'
        any_fail=1
        if [[ "$in_ch" != "1" ]]; then
          echo "$NOW group=$disp_group csv=${name} newest=$newest threshold=${FAIL_SEC}s" >> "$VERDICT_FILE"
        fi
      elif [[ "$newest" < "$WARN_TS" ]]; then
        out+="$hdr newest=${newest} ⚠ WARN (>= ${WARN_SEC}s stale)"$'\n'
      else
        out+="$hdr newest=${newest} ✓ OK"$'\n'
      fi
    done <<< "$extracted"
  done

  # Report in stable sorted order.
  printf "%s" "$out" | sort

  if [[ "$any_fail" -eq 1 && ! -f "$PANIC_FILE" ]]; then
    echo "one or more per-cycle scrapes >= ${FAIL_SEC}s stale at $NOW" > "$PANIC_FILE"
  fi
}

judge_run_scrape_liveness()    { _judge_eval_scrape_liveness "$1" >/dev/null 2>&1; }
judge_report_scrape_liveness() {
  echo "── scrape liveness (per-cycle sample staleness) ──"
  _judge_eval_scrape_liveness "$1"
}
