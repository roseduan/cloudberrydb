#!/bin/bash
# verify_pax_mirror.sh <port> <dbname> <relid>
#
# For every content segment in gp_segment_configuration, compare the
# primary's ts_compressed/<relid>/ against the mirror's copy.  Prints
# one line per content:
#
#   content <N>: match     — dirs byte-identical (or both absent)
#   content <N>: absent    — neither has the dir (also OK; treated as match)
#   content <N>: differ    — either the dirs differ or one is missing
#
# Used by ts_pax_mirror_repl.sql via psql \!.  Output must be stable
# across runs (start_matchsubs is not needed — content ids are 0/1/2
# on gpdemo, deterministic).

set -u
PORT="${1:-7000}"
DBNAME="${2:-contrib_regression}"
RELID="${3:?relid arg required}"

# One-off SQL: (content, primary_datadir, mirror_datadir) triples.
ROWS=$(psql -p "$PORT" -d "$DBNAME" -tA -F '|' <<EOF
SELECT c.content,
       MAX(CASE WHEN c.role = 'p' THEN c.datadir END),
       MAX(CASE WHEN c.role = 'm' THEN c.datadir END)
  FROM gp_segment_configuration c
 WHERE c.content >= 0
 GROUP BY c.content
 ORDER BY c.content;
EOF
)

DBOID=$(psql -p "$PORT" -d "$DBNAME" -tAc \
    "SELECT oid FROM pg_database WHERE datname='$DBNAME';")

# Parse each row, diff.
echo "$ROWS" | while IFS='|' read -r content pdir mdir; do
    [ -z "$content" ] && continue
    PPATH="$pdir/base/$DBOID/ts_compressed/$RELID"
    MPATH="$mdir/base/$DBOID/ts_compressed/$RELID"

    if [ ! -e "$PPATH" ] && [ ! -e "$MPATH" ]; then
        echo "content $content: absent"
    elif [ ! -e "$PPATH" ] || [ ! -e "$MPATH" ]; then
        echo "content $content: differ"
    else
        if diff -r --brief "$PPATH" "$MPATH" >/dev/null 2>&1; then
            echo "content $content: match"
        else
            echo "content $content: differ"
        fi
    fi
done
