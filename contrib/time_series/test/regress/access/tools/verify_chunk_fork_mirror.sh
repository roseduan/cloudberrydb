#!/bin/bash
# verify_chunk_fork_mirror.sh <port> <dbname> <relid>
#
# Sister to verify_pax_mirror.sh, but for the *heap* chunk-fork files
# (base/<dbid>/<relfilenode>_ts_<N>) rather than the PAX sidecar
# directory (base/<dbid>/ts_compressed/<relid>/).
#
# Motivation: three sites (ts_tableam.c:718, ts_compress.c:2361,
# ts_compress.c:2434) call smgrtruncate on a chunk fork.  Because
# chunk fork numbers exceed MAX_FORKNUM they live outside standard
# PG WAL; the fix wraps each site in a ts_wal_fork_truncate +
# delayChkptEnd bracket so the ts_rmgr TRUNCATE record reaches the
# mirror.  This helper diffs the resulting (name, size) tuples so a
# regression that drops the WAL emit again surfaces as "differ".
#
# For every content segment in gp_segment_configuration, list every
# matching `<relfilenode>_ts_*` file on primary and mirror datadirs
# and compare the (name, size) tuples.  Prints one line per content:
#
#   content <N>: match     — file sets and sizes match (or both empty)
#   content <N>: differ    — file sets or sizes disagree
#   content <N>: absent    — neither side has any chunk fork file
#
# Output is stable across runs; gpdemo assigns content ids
# 0/1/2 deterministically.

set -u
PORT="${1:-7000}"
DBNAME="${2:-contrib_regression}"
RELID="${3:?relid arg required}"

DBOID=$(psql -p "$PORT" -d "$DBNAME" -tAc \
    "SELECT oid FROM pg_database WHERE datname='$DBNAME';")

# (content, primary_datadir, mirror_datadir, segment-local relfilenode).
# Cloudberry assigns relfilenode independently on QD vs QEs, so we must
# query it per segment via gp_dist_random('pg_class') rather than
# trusting the QD-side pg_class.relfilenode.
ROWS=$(psql -p "$PORT" -d "$DBNAME" -tA -F '|' <<EOF
WITH segs AS (
    SELECT content,
           MAX(CASE WHEN role = 'p' THEN datadir END) AS pdir,
           MAX(CASE WHEN role = 'm' THEN datadir END) AS mdir
      FROM gp_segment_configuration
     WHERE content >= 0
     GROUP BY content
),
relnodes AS (
    SELECT gp_execution_segment() AS content, relfilenode
      FROM gp_dist_random('pg_class')
     WHERE oid = $RELID
)
SELECT s.content, s.pdir, s.mdir, r.relfilenode
  FROM segs s LEFT JOIN relnodes r USING (content)
 ORDER BY s.content;
EOF
)

echo "$ROWS" | while IFS='|' read -r content pdir mdir relnode; do
    [ -z "$content" ] && continue
    PPATH="$pdir/base/$DBOID"
    MPATH="$mdir/base/$DBOID"

    # List each chunk-fork file on this segment (both sides), sorted by
    # name.  stat's `%n %s` prints relative name then size.
    plist=$(cd "$PPATH" 2>/dev/null && \
            stat -c '%n %s' ${relnode}_ts_* 2>/dev/null | sort || true)
    mlist=$(cd "$MPATH" 2>/dev/null && \
            stat -c '%n %s' ${relnode}_ts_* 2>/dev/null | sort || true)

    if [ -z "$plist" ] && [ -z "$mlist" ]; then
        echo "content $content: absent"
    elif [ "$plist" = "$mlist" ]; then
        echo "content $content: match"
    else
        echo "content $content: differ"
    fi
done
