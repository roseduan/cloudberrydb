#!/bin/bash
# monitor/view_correctness_scrape.sh
# SOAK-LOOP: scope=per-db interval=1800 interval_var=SOAK_VIEW_CORRECTNESS_EVERY order=10
#
# THE correctness signal: runs monitor/view_correctness.sql (one
# REPEATABLE READ transaction, 3 live + 3 mat windows) against one DB
# and parses the verdict rows.
#
# This monitor is PURE COLLECTION — it detects + records + captures
# evidence, but makes NO fail/early-stop DECISION.  All judgment lives in
# judge/verdicts/: view_correctness.sh raises fail.view_mismatch (via
# cross-cycle persistence, below); framework_health.sh raises the early
# stops (consecutive-incomplete → view_check_dead, OID change → env_lost),
# chaos-aware.  The one thing this file must do in-process is snapshot the
# ephemeral diagnostic state on a MISMATCH (watermark + L1/L2 + differing
# rows are gone seconds later) — capturing evidence is still collection.
#
# What it emits:
#   MISMATCH row        → record the DISTINCT differing buckets to
#                         view_mismatch_buckets.csv + capture an evidence
#                         snapshot.  Does NOT raise fail.view_mismatch.
#                         judge keys on (db,cagg,window,bucket) across
#                         cycles: a bucket in >= 2 cycles = real divergence
#                         → fail (exit +2); a single-cycle bucket is a
#                         watermark-boundary transient (a late write's row
#                         is visible a few seconds before its L1/L2 entry,
#                         so the decidable check briefly sees a below-wm
#                         bucket as clean) → recorded, not failed.
#   incomplete output   → view_incomplete.csv + db_health.csv(status=
#                         incomplete).  judge counts the consecutive
#                         streak → panic.view_check_dead.
#   normal cycle        → db_health.csv(status=ok).
#   DB identity changed → db_health.csv(status=oid_changed).  setup
#                         records each DB's OID; judge raises
#                         panic.env_lost when it differs.
#
# Usage (invoked by lib/loops.sh):
#   view_correctness_scrape.sh <db> <results_dir>          one check cycle

set -uo pipefail

SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$SOAK_DIR/lib/helpers.sh"

VIEW_EXPECTED_LINES=6   # 3 LIVE + 3 MAT — keep in sync with view_correctness.sql

# ── Collection mode ──────────────────────────────────────────────────
DB="${1:?usage: view_correctness_scrape.sh <db> <results_dir>}"
RESULTS="${2:?usage: view_correctness_scrape.sh <db> <results_dir>}"
HOST="${SOAK_HOST:-localhost}"
PORT="${SOAK_PORT:-7000}"
USER="${SOAK_USER:-gpadmin}"
FLAGS="$RESULTS/flags"
mkdir -p "$FLAGS"

VIEW_CSV="$RESULTS/data/view_correctness.csv"
INCOMPLETE_CSV="$RESULTS/data/view_incomplete.csv"
DB_HEALTH_CSV="$RESULTS/data/db_health.csv"
BUCKETS_CSV="$RESULTS/data/view_mismatch_buckets.csv"
OID_FILE="$RESULTS/state/cursors/db_oid_${DB}"
EVERY="${SOAK_VIEW_CORRECTNESS_EVERY:-1800}"

[[ -f "$VIEW_CSV" ]]       || echo "ts,db,cagg,window,total_rows,mismatch_count,excluded_buckets,verdict" > "$VIEW_CSV"
[[ -f "$INCOMPLETE_CSV" ]] || echo "ts,db,rc,actual_lines,expected_lines,duration_s,reason" > "$INCOMPLETE_CSV"
# db_health.csv — one row PER CYCLE recording what happened this cycle:
# status ∈ {ok, incomplete, oid_changed}.  PURE COLLECTION: the early-stop
# DECISION (consecutive-incomplete streak → view_check_dead, oid change →
# env_lost) is made by judge/verdicts/framework_health.sh, chaos-aware.
[[ -f "$DB_HEALTH_CSV" ]]  || echo "ts,db,status,detail" > "$DB_HEALTH_CSV"
# view_mismatch_buckets.csv — one row per DISTINCT differing bucket per
# MISMATCH cycle (pure collection).  judge decides fail via cross-cycle
# persistence: same (cagg,window,bucket) in >= 2 cycles = real divergence;
# seen in only 1 cycle = watermark-boundary transient.
[[ -f "$BUCKETS_CSV" ]]  || echo "ts,db,cagg,window,bucket" > "$BUCKETS_CSV"

TS=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

# 1. Environment-integrity data — record this DB's current OID vs the one
# setup pinned.  We only RECORD it; judge decides env_lost.  A recreated
# DB gives garbage comparisons, so we still skip this cycle's check.
CUR_OID=$(PGOPTIONS='--client-min-messages=warning' \
  psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -X -At \
       -c "SELECT oid FROM pg_database WHERE datname = current_database();" 2>/dev/null) || CUR_OID=""
if [[ -f "$OID_FILE" ]]; then
  EXPECTED_OID=$(cat "$OID_FILE")
  if [[ -n "$CUR_OID" && "$CUR_OID" != "$EXPECTED_OID" ]]; then
    soak_log "VIEW_CHECK: $DB oid changed ($EXPECTED_OID → $CUR_OID) — recording oid_changed"
    printf '%s,%s,%s,%s\n' "$TS" "$DB" "oid_changed" "expected=$EXPECTED_OID current=$CUR_OID" >> "$DB_HEALTH_CSV"
    exit 0
  fi
fi

# 2. The check itself.  statement_timeout comes from soak_params.sh
# (the SQL file deliberately carries no hardcoded tunables).
T0=$(date +%s)
VIEW_ERR="$(mktemp)"
OUT=$(PGOPTIONS="--client-min-messages=warning -c statement_timeout=${SOAK_VIEW_STATEMENT_TIMEOUT:-3min}" \
  psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
       -X -v ON_ERROR_STOP=1 \
       -f "$SOAK_DIR/monitor/view_correctness.sql" \
       2>"$VIEW_ERR")
PSQL_RC=$?
T1=$(date +%s)
ELAPSED=$(( T1 - T0 ))
cat "$VIEW_ERR" >> "$RESULTS/errors/view_correctness.err"

ACTUAL_LINES=$(echo -n "$OUT" | grep -cE '^[0-9]{4}-[0-9]{2}-[0-9]{2}' || true)
if [[ $PSQL_RC -ne 0 ]] || [[ "$ACTUAL_LINES" -ne "$VIEW_EXPECTED_LINES" ]]; then
  # Classify the incomplete.  A DB that is crash-recovering / restarting /
  # dropped the connection is INFRASTRUCTURE DOWN, not a wedged correctness
  # check: a chaos SIGKILL of a BGW triggers a full cluster crash-reinit
  # (postmaster "reinitializing"), during which this check simply cannot
  # connect.  That is expected disruption — NOT proof the check itself is
  # broken.  Tag it db_unavailable so judge does NOT count it toward
  # fail.view_incomplete or panic.view_check_dead (both mean "the check is
  # wedged", a different signal; a real crash-loop is caught by the
  # server log, not here).  A genuine wedge/timeout/
  # unexpected-rowcount stays incomplete_output and still counts.
  REASON=incomplete_output; HSTATUS=incomplete
  if grep -qiE 'in recovery mode|system is (starting up|in recovery|shutting down)|closed the connection unexpectedly|terminating connection due to|could not connect|connection to server .* failed|no connection to the server|server .*(was )?terminated' "$VIEW_ERR"; then
    REASON=db_unavailable; HSTATUS=db_unavailable
  fi
  rm -f "$VIEW_ERR"
  soak_log "VIEW_CHECK INCOMPLETE ($REASON): $DB rc=$PSQL_RC lines=$ACTUAL_LINES/$VIEW_EXPECTED_LINES duration=${ELAPSED}s — skipping cycle"
  printf '%s,%s,%s,%s,%s,%s,%s\n' \
    "$TS" "$DB" "$PSQL_RC" "$ACTUAL_LINES" "$VIEW_EXPECTED_LINES" "$ELAPSED" "$REASON" \
    >> "$INCOMPLETE_CSV"
  printf '%s,%s,%s,%s\n' "$TS" "$DB" "$HSTATUS" "rc=$PSQL_RC lines=$ACTUAL_LINES/$VIEW_EXPECTED_LINES" >> "$DB_HEALTH_CSV"
  exit 0
fi
rm -f "$VIEW_ERR"
printf '%s,%s,%s,%s\n' "$TS" "$DB" "ok" "" >> "$DB_HEALTH_CSV"

if [[ "$ELAPSED" -gt $(( EVERY / ${SOAK_VIEW_SLOW_DIV:-4} )) ]]; then
  soak_log "VIEW_CHECK SLOW: $DB took ${ELAPSED}s (budget ${EVERY}s) — investigate"
fi

# 3. Record verdicts; on MISMATCH record the differing buckets + capture
# ephemeral evidence.  PURE COLLECTION: this monitor does NOT decide
# real-vs-transient and does NOT raise fail.view_mismatch.  That decision
# is judge/verdicts/view_correctness.sh's — it flags a bucket only if the
# same (cagg,window,bucket) mismatches across >= 2 check cycles, because a
# single-cycle mismatch is a watermark-boundary eventual-consistency
# transient (a late write's row is visible a few seconds before its L1/L2
# invalidation entry, so the decidable check briefly sees a below-wm
# bucket as clean).  Confirming that in-collector needs a live re-check
# whose timing is unreliable (2026-07-09: a fixed re-check flapped across
# a rolling batch of cold-start buckets); cross-cycle persistence in judge
# is both correct and keeps this file a pure collector.
# SQL emits schema v2: ts,db,cagg,window,total_rows,mismatch,excluded,verdict
# Plus optional _DIFF_-prefixed rows interleaved (one per differing bucket
# per mismatched cagg), captured at the SAME REPEATABLE READ snapshot.
# Filter them out of the summary parser.
SUMMARY_OUT=$(printf '%s\n' "$OUT" | grep -v '^_DIFF_,')
while IFS=',' read -r _ts_csv _db_csv cagg_csv window_csv total mismatch excluded verdict; do
  [[ -z "$cagg_csv" ]] && continue
  # The query emits its own now()::text; we record the driver-side TS
  # so reports correlate with the other CSVs.
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$TS" "$DB" "$cagg_csv" "$window_csv" "$total" "$mismatch" "$excluded" "$verdict" \
    >> "$VIEW_CSV"

  if [[ "$verdict" == "MISMATCH" ]]; then
    soak_log "CAGG VIEW MISMATCH: $DB.$cagg_csv window=$window_csv mismatch_count=$mismatch (of $total) — recording buckets + evidence (judge decides real/transient)"
    SNAP="$RESULTS/plan_snapshots/view_mismatch_${DB}_${cagg_csv}_$(date -u +%H%M%SZ).snapshot.txt"
    # Map cagg_csv (window-tagged name from view_correctness.sql) back
    # to the actual CAGG view name -- the leading underscore-suffixed
    # variant (_mat) probes the materialised branch but the same view.
    base_cagg="${cagg_csv%_mat}"
    # Re-run the same per-cagg comparison standalone and dump the
    # actually-differing rows.  Diagnostic only -- this is a fresh
    # snapshot, NOT the one that fired the MISMATCH, so transient
    # races may show clean (still informative: shows what the row
    # shape looks like right after the failure).  Per-cagg aggregate
    # column lists kept here are the same as in view_correctness.sql.
    {
      echo "# ── MISMATCH DETAIL (from view_correctness.sql output) ──"
      echo "$OUT"
      echo
      echo "# ── CAGG CATALOG + WATERMARK STATE ──"
      psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -X -c "
        SELECT cagg_id, watermark FROM time_series.cagg_watermark ORDER BY 1;
        SELECT * FROM time_series.cagg_invalidation_log ORDER BY 2 DESC LIMIT 30;
        SELECT * FROM time_series.cagg_materialization_log ORDER BY 2 DESC LIMIT 20;
      " 2>/dev/null || true
      echo
      echo "# ── DIFFERING ROWS (src LEFT JOIN view; UP TO 50)  ──"
      echo "# Side  bucket  tags_id   src_cnt  v_cnt   src_aggs   v_aggs"
      # Use the view-correctness windows that match the spec: live
      # windows = last $window for plain caggs; mat windows already
      # encoded in cagg_csv via the 30min<wm/1h<wm/... suffix.
      case "$cagg_csv" in
        cv_1min|cv_1min_mat)        bucket_w="1 minute" ;;
        cv_5min|cv_5min_mat)        bucket_w="5 minutes" ;;
        cv_1hour|cv_1hour_mat)      bucket_w="1 hour" ;;
        *)                          bucket_w="1 hour" ;;
      esac
      # Live windows -> [w_start, w_end) = [now-window, now)
      # Mat windows -> bracket bucket >= wm-low AND < wm-high (the
      # exact offsets are spec-aligned in view_correctness.sql; we use
      # 'last 6h' here as a generous superset for diagnosis).
      case "$window_csv" in
        30min)      probe="time >= now() - INTERVAL '30 min' AND time < now()" ;;
        1h)         probe="time >= now() - INTERVAL '1 hour'  AND time < now()" ;;
        6h)         probe="time >= now() - INTERVAL '6 hours' AND time < now()" ;;
        2d)         probe="time >= now() - INTERVAL '2 days'  AND time < now()" ;;
        *)          probe="time >= now() - INTERVAL '6 hours' AND time < now()" ;;
      esac
      # Per-cagg aggregate column expressions kept in lockstep with
      # view_correctness.sql 'src'/'v' CTEs.
      case "$base_cagg" in
        cv_1min)
            src_agg="count(*) AS cnt, avg(usage_user)::numeric(20,6) AS avg_user, max(usage_system) AS max_system"
            v_cols="cnt, avg_user::numeric(20,6) AS avg_user, max_system" ;;
        cv_5min)
            src_agg="count(*) AS cnt, sum(usage_user) AS sum_user, min(usage_idle) AS min_idle, max(usage_system) AS max_system"
            v_cols="cnt, sum_user, min_idle, max_system" ;;
        cv_1hour)
            src_agg="count(*) AS cnt, avg(usage_user)::numeric(20,6) AS avg_user, max(usage_system) AS max_system, min(usage_idle) AS min_idle"
            v_cols="cnt, avg_user::numeric(20,6) AS avg_user, max_system, min_idle" ;;
        *)
            src_agg="count(*) AS cnt"; v_cols="cnt" ;;
      esac
      psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -X -c "
        SET optimizer = off;
        SET search_path TO public, time_series;
        BEGIN ISOLATION LEVEL REPEATABLE READ;
        WITH src AS (
          SELECT time_bucket('$bucket_w'::interval, time) AS bucket,
                 tags_id, $src_agg
            FROM cpu WHERE $probe GROUP BY 1, 2
        ), v AS (
          SELECT bucket, tags_id, $v_cols FROM $base_cagg
           WHERE bucket >= now() - INTERVAL '6 hours' AND bucket < now()
        )
        SELECT
          CASE WHEN s.bucket IS NULL THEN 'view-only'
               WHEN v.bucket IS NULL THEN 'src-only'
               ELSE 'differ' END AS side,
          COALESCE(s.bucket, v.bucket) AS bucket,
          COALESCE(s.tags_id, v.tags_id) AS tags_id,
          row_to_json(s.*) AS src_row,
          row_to_json(v.*) AS view_row
          FROM src s FULL OUTER JOIN v
            ON s.bucket = v.bucket AND s.tags_id = v.tags_id
         WHERE (s.bucket IS NULL) OR (v.bucket IS NULL)
            OR (row_to_json(s.*)::text IS DISTINCT FROM row_to_json(v.*)::text
                AND s.bucket IS NOT NULL AND v.bucket IS NOT NULL)
         ORDER BY 2, 3
         LIMIT 50;
        COMMIT;
      " 2>/dev/null || true
    } > "$SNAP" 2>&1
    # Record the DISTINCT differing buckets for this (cagg,window) with
    # this cycle's TS.  judge/verdicts/view_correctness.sh keys on
    # (db,cagg,window,bucket) across cycles: a bucket seen in >= 2 cycles
    # is a real divergence → fail.view_mismatch; a single-cycle bucket is
    # a watermark-boundary transient → recorded, not failed.  (bucket is a
    # timestamp with no comma, so it is a clean CSV field.)
    printf '%s\n' "$OUT" | awk -F, -v ts="$TS" -v db="$DB" -v cg="$cagg_csv" -v win="$window_csv" '
      $1=="_DIFF_" && $2==cg && $3==win { if (!seen[$4]++) printf "%s,%s,%s,%s,%s\n", ts, db, cg, win, $4 }
    ' >> "$BUCKETS_CSV"
  fi
done <<< "$SUMMARY_OUT"

# ── 4. Targeted delayed re-check (§4) ────────────────────────────────
# PURE COLLECTION: re-sample specific MISMATCH buckets after a settle
# delay and RECORD the outcome; judge/verdicts/view_correctness.sh decides
# fail from it.  Why not cross-cycle persistence: the mat check window
# slides with the watermark, so at a coarse SOAK_VIEW_CORRECTNESS_EVERY a
# persistent divergence may be observed in only ONE cycle (the window then
# slides past the bucket) — "recurs across >= N cycles" then never trips.
# A per-bucket re-query is decoupled from window re-coverage: it re-checks
# THAT exact bucket wherever the window now sits.  A late-write transient
# resolves within the settle delay (its L1/L2 entry drains → the bucket
# becomes decidable-and-matching).  Comparison + decidability are the SAME
# as view_correctness.sql (same tolerances), so no new verdict is invented.
RECHECK_CSV="$RESULTS/data/view_mismatch_recheck.csv"
[[ -f "$RECHECK_CSV" ]] || echo "ts,db,cagg,window,bucket,outcome" > "$RECHECK_CSV"
SETTLE="${SOAK_VIEW_RECHECK_SETTLE_SEC:-180}"
CUTOFF=$(date -u -d "@$(( $(date -u +%s) - SETTLE ))" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || true)
if [[ -n "$CUTOFF" ]]; then
  while IFS='|' read -r r_cagg r_window r_bucket; do
    [[ -z "$r_cagg" ]] && continue
    base_cagg="${r_cagg%_mat}"
    case "$base_cagg" in
      cv_1min)  bw='1 minute'
                src='count(*) cnt, avg(usage_user) avg_user, max(usage_system) max_system'
                vc='cnt, avg_user, max_system'
                pred='src.cnt!=v.cnt OR abs(src.avg_user-v.avg_user)>1e-4 OR src.max_system!=v.max_system' ;;
      cv_5min)  bw='5 minutes'
                src='count(*) cnt, sum(usage_user) sum_user, min(usage_idle) min_idle, max(usage_system) max_system'
                vc='cnt, sum_user, min_idle, max_system'
                pred='src.cnt!=v.cnt OR abs(src.sum_user-v.sum_user)>1e-4 OR src.min_idle!=v.min_idle OR src.max_system!=v.max_system' ;;
      cv_1hour) bw='1 hour'
                src='count(*) cnt, avg(usage_user) avg_user, max(usage_system) max_system, min(usage_idle) min_idle'
                vc='cnt, avg_user, max_system, min_idle'
                pred='src.cnt!=v.cnt OR abs(src.avg_user-v.avg_user)>1e-4 OR src.max_system!=v.max_system OR src.min_idle!=v.min_idle' ;;
      *)        continue ;;   # unknown cagg — no faithful predicate, skip
    esac
    RES=$(PGOPTIONS="--client-min-messages=warning -c statement_timeout=${SOAK_VIEW_STATEMENT_TIMEOUT:-10min}" \
      psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -X -At -F'|' -c "
        SET optimizer=off; SET search_path TO public, time_series;
        WITH ids AS (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name='$base_cagg'),
        wm AS (SELECT NULLIF(MIN(watermark),'-infinity'::timestamptz) AS watermark
                 FROM time_series.cagg_watermark WHERE cagg_id=(SELECT cagg_id FROM ids)),
        dirty AS (
          SELECT il.lowest_modified AS lo, il.greatest_modified AS hi
            FROM time_series.cagg_invalidation_log il WHERE il.source_table_oid='public.cpu'::regclass
          UNION ALL
          SELECT ml.lowest_modified, ml.greatest_modified
            FROM time_series.cagg_materialization_log ml WHERE ml.cagg_id=(SELECT cagg_id FROM ids)),
        dec AS (SELECT ('$r_bucket'::timestamptz >= COALESCE((SELECT watermark FROM wm),'-infinity'::timestamptz)
                  OR NOT EXISTS (SELECT 1 FROM dirty d WHERE d.hi >= '$r_bucket'::timestamptz
                                                    AND d.lo <  '$r_bucket'::timestamptz + INTERVAL '$bw')) AS decidable),
        src AS (SELECT tags_id, $src FROM cpu
                 WHERE time >= '$r_bucket'::timestamptz AND time < '$r_bucket'::timestamptz + INTERVAL '$bw' GROUP BY tags_id),
        v AS (SELECT tags_id, $vc FROM $base_cagg WHERE bucket = '$r_bucket'::timestamptz)
        SELECT (SELECT decidable FROM dec),
               count(*) FILTER (WHERE src.tags_id IS NULL OR v.tags_id IS NULL OR $pred)
          FROM src FULL OUTER JOIN v ON src.tags_id=v.tags_id;
      " 2>/dev/null || true)
    dec="${RES%%|*}"; mm="${RES##*|}"
    if   [[ "$dec" == "t" && "$mm" =~ ^[0-9]+$ && "$mm" -gt 0 ]]; then out="still_mismatch"
    elif [[ "$dec" == "t" && "$mm" == "0" ]];                    then out="resolved"
    else                                                              out="pending"; fi
    printf '%s,%s,%s,%s,%s,%s\n' "$TS" "$DB" "$r_cagg" "$r_window" "$r_bucket" "$out" >> "$RECHECK_CSV"
    [[ "$out" == "still_mismatch" ]] && \
      soak_log "VIEW_RECHECK: $DB.$r_cagg $r_window bucket=$r_bucket STILL diverges after settle=${SETTLE}s — real divergence (judge will fail)"
  done < <(awk -F, -v db="$DB" -v cutoff="$CUTOFF" '
    # file1 = recheck.csv: collect keys with a TERMINAL outcome (done).
    FNR==NR { if (FNR>1 && $2==db && ($6=="still_mismatch"||$6=="resolved")) term[$3"|"$4"|"$5]=1; next }
    # file2 = buckets.csv: candidate = this DB, first-seen <= cutoff, not terminal.
    FNR>1 && $2==db { k=$3"|"$4"|"$5; if (!(k in mn) || $1<mn[k]) mn[k]=$1; seen[k]=1 }
    END { for (k in seen) if (!(k in term) && mn[k] <= cutoff) print k }
  ' "$RECHECK_CSV" "$BUCKETS_CSV")
fi
