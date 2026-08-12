#!/bin/bash
# tools/selftest.sh — TRUE-POSITIVE proof for the correctness comparator.
#
# A check that has never fired is indistinguishable from a check that
# cannot fire.  Every MISMATCH the soak ever produced was a framework
# bug or a timing artifact — the comparator's ability to catch a REAL
# materialization error was, until this script, unproven.
#
# This is the "press the test button" for the smoke detector: it
# deliberately writes a wrong value straight into a CAGG mat table
# (bypassing the normal refresh path), runs monitor/view_correctness.sql,
# and asserts the comparator REPORTS MISMATCH on exactly that bucket —
# then restores the value and asserts the comparator returns to match.
#
# Run this once on a prepared DB BEFORE trusting a long GA soak.  It is
# NOT a SOAK-LOOP (no header): it mutates data on purpose and must never
# auto-run during a real soak.
#
# Safety:
#   - operates on ONE row in a bucket inside cv_1hour's mat window,
#     well below the watermark (decidable, not racing the BGW)
#   - original value saved before corruption; an EXIT trap restores it
#     on ANY exit path (success, assertion failure, Ctrl-C)
#   - restoration is verified (re-read == original) before declaring OK
#
# Usage:
#   bash tools/selftest.sh [db]        # db defaults to first SOAK_DBS
#
# Exit: 0 = comparator proven (caught the corruption AND cleared after
#           restore); 1 = comparator FAILED to catch a known-bad value
#           (a serious problem — the soak's core signal is blind); 2 =
#           setup/precondition error (couldn't run the proof).

set -uo pipefail

SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$SOAK_DIR/conf/soak_params.sh"

DB="${1:-${SOAK_DBS%% *}}"
HOST="$SOAK_HOST"; PORT="$SOAK_PORT"; USER="$SOAK_USER"
CAGG=cv_1hour                       # 1-hour buckets: mat window is wide + stable
BUMP=888888                         # an unmistakable wrong cnt

psql_at() { PGOPTIONS='-c optimizer=off --client-min-messages=warning' \
  psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -X -At "$@"; }

say()  { printf '  %s\n' "$*"; }
die2() { printf '✗ SETUP: %s\n' "$*" >&2; exit 2; }

echo "=== soak comparator self-test (true-positive proof) ==="
echo "  db=$DB cagg=$CAGG"

# ── 1. Resolve the mat table + a decidable target bucket ─────────────
MAT=$(psql_at -c "SELECT mat_table_name FROM time_series.continuous_agg
                   WHERE user_view_name='$CAGG'") || die2 "cannot query continuous_agg"
[[ -n "$MAT" ]] || die2 "no mat table for $CAGG (is the DB prepared?)"

# Pick a row inside the cv_1hour mat window [wm-8h, wm-2h): below the
# watermark (so the view serves it from mat, not the live branch) and
# old enough that no pending invalidation overlaps it.
# Pipe-separated: the bucket timestamp contains a space, so a
# space-delimited read would split it across fields.
IFS='|' read -r BUCKET TAGS ORIG < <(psql_at -F'|' -c "
  WITH wm AS (
    SELECT MIN(watermark) w FROM time_series.cagg_watermark cw
      JOIN time_series.continuous_agg ca ON cw.cagg_id=ca.cagg_id
     WHERE ca.user_view_name='$CAGG')
  SELECT m.bucket, m.tags_id, m.cnt
    FROM time_series.\"$MAT\" m, wm
   WHERE m.bucket >= date_trunc('hour',wm.w) - INTERVAL '8 hours'
     AND m.bucket <  date_trunc('hour',wm.w) - INTERVAL '2 hours'
   ORDER BY m.bucket LIMIT 1")
[[ -n "${ORIG:-}" ]] || die2 "no decidable bucket in $CAGG mat window — DB needs >8h of seed below the watermark"
say "target: bucket='$BUCKET' tags_id=$TAGS original_cnt=$ORIG"

# ── 2. EXIT trap: restore no matter how we leave ─────────────────────
restored=0
restore() {
  [[ "$restored" -eq 1 ]] && return
  psql_at -c "UPDATE time_series.\"$MAT\" SET cnt=$ORIG
               WHERE bucket='$BUCKET' AND tags_id=$TAGS" >/dev/null 2>&1
  local now
  now=$(psql_at -c "SELECT cnt FROM time_series.\"$MAT\"
                     WHERE bucket='$BUCKET' AND tags_id=$TAGS")
  if [[ "$now" == "$ORIG" ]]; then
    say "restored: cnt back to $ORIG ✓"; restored=1
  else
    printf '✗ RESTORE FAILED: cnt is now %s, expected %s — FIX MANUALLY:\n' "$now" "$ORIG" >&2
    printf '    UPDATE time_series."%s" SET cnt=%s WHERE bucket=%s AND tags_id=%s;\n' \
      "$MAT" "$ORIG" "'$BUCKET'" "$TAGS" >&2
  fi
}
trap restore EXIT

verdict_of() {  # $1=label → prints that row's verdict column
  psql_at -f "$SOAK_DIR/monitor/view_correctness.sql" 2>/dev/null \
    | awk -F, -v l="$1" '$3==l {print $NF}'
}

# ── 3. Corrupt → expect MISMATCH ─────────────────────────────────────
echo "── phase 1: inject wrong value, expect MISMATCH ──"
psql_at -c "UPDATE time_series.\"$MAT\" SET cnt=$BUMP
             WHERE bucket='$BUCKET' AND tags_id=$TAGS" >/dev/null || die2 "corrupt UPDATE failed"
V=$(verdict_of "${CAGG}_mat")
say "${CAGG}_mat verdict = ${V:-<none>}"
if [[ "$V" != "MISMATCH" ]]; then
  echo "✗ FAILED — comparator did NOT catch a known-bad mat value (verdict=${V:-none})."
  echo "  The soak's core correctness signal is BLIND.  Do not trust a green run."
  exit 1
fi
say "comparator caught the corruption ✓"

# ── 4. Restore → expect match ────────────────────────────────────────
echo "── phase 2: restore, expect match ──"
restore
V=$(verdict_of "${CAGG}_mat")
say "${CAGG}_mat verdict = ${V:-<none>}"
if [[ "$V" != "match" ]]; then
  echo "✗ FAILED — comparator still MISMATCH after restore (verdict=${V:-none})."
  echo "  Either restore didn't take, or the comparator is non-deterministic."
  exit 1
fi

echo "=== PASSED — comparator catches a real mat error and clears after fix ==="
exit 0
