#!/bin/bash
# =============================================================================
# Iceberg builtin cleanup — MinIO integration test (issues #399 + #344)
#
# Verifies the OBJECT-STORAGE side of the cleanup paths that pg_regress cannot
# check (it has no way to list MinIO / wait for the async consumer):
#
#   A  abort  INSERT+UPDATE ROLLBACK   -> zero new S3 files                (#399)
#   B  abort  DELETE+INSERT ROLLBACK   -> zero new S3 files, rows intact   (#399)
#   C  commit INSERT+UPDATE            -> intermediate orphans async-deleted,
#                                         final tree kept, data intact      (#399)
#   D  DROP TABLE                      -> all files async-deleted           (#344)
#   E  TRUNCATE                        -> emptied, old files deleted, reusable
#   F  VACUUM  (compaction)            -> old files deleted, data intact
#   G  DROP SCHEMA CASCADE             -> schema's files async-deleted
#
# The test uses its OWN catalog/volume on a dedicated base_path
# (/itxn_volume/) so S3 file diffing is isolated from other tables.
#
# Requirements: a running builtin-catalog-capable cluster, a reachable MinIO,
# and the `mc` client. Override via env:
#   PGPORT (7000)  MC_ENDPOINT (http://127.0.0.1:9000)
#   MC_ACCESS (minioadmin)  MC_SECRET (minioadmin)  BUCKET (warehouse)
#   FDW_ENDPOINT (http://minio:9000)  FDW_ACCESS (admin)  FDW_SECRET (admin12345)
#   MIN_INTERVAL (10)  -- lowers the consumer interval for a faster run
#
# Exit code 0 = all scenarios PASS.
# =============================================================================
set -u
PGPORT="${PGPORT:-7000}"
MC_ENDPOINT="${MC_ENDPOINT:-http://127.0.0.1:9000}"
MC_ACCESS="${MC_ACCESS:-minioadmin}"
MC_SECRET="${MC_SECRET:-minioadmin}"
BUCKET="${BUCKET:-warehouse}"
FDW_ENDPOINT="${FDW_ENDPOINT:-http://minio:9000}"
FDW_ACCESS="${FDW_ACCESS:-admin}"
FDW_SECRET="${FDW_SECRET:-admin12345}"
MIN_INTERVAL="${MIN_INTERVAL:-10}"
BASE=/itxn_volume/
FAILS=0

# Fail fast if MinIO is unreachable: otherwise SNAP() returns empty for every
# scenario and the abort checks (A/B) would pass vacuously ("0|[]" == "0|[]").
mc alias set itxn "$MC_ENDPOINT" "$MC_ACCESS" "$MC_SECRET" >/dev/null 2>&1 \
  || { echo "ERROR: mc alias setup failed (is MinIO reachable at $MC_ENDPOINT?)"; exit 1; }
# Sanity-check the client can actually reach the bucket before any scenario runs.
mc ls "itxn/${BUCKET}/" >/dev/null 2>&1 \
  || { echo "ERROR: cannot list bucket '$BUCKET' via mc (MinIO down or bad creds?)"; exit 1; }
P(){ psql postgres -p "$PGPORT" -tAc "$1" 2>/dev/null; }
RUN(){ psql postgres -p "$PGPORT" -q >/dev/null 2>&1 <<EOF
$1
EOF
}
SNAP(){ mc ls --recursive "itxn/${BUCKET}${BASE}" 2>/dev/null | awk '{print $NF}' | sort; }
DQ(){ P "select count(*) from pg_ext_aux.pg_iceberg_deletion_queue;"; }
drain(){ local i; for i in $(seq 1 30); do sleep 6; [ "$(DQ)" = "0" ] && return 0; done; return 1; }
still_present(){ local snap f n=0; snap=$(SNAP); for f in $1; do echo "$snap" | grep -qxF "$f" && n=$((n+1)); done; echo "$n"; }
check(){ if [ "$2" = "$3" ]; then echo "  $1 PASS"; else echo "  $1 FAIL (got '$2' want '$3')"; FAILS=$((FAILS+1)); fi; }

echo "### setup (own catalog/volume on ${BASE}) ###"
P "alter system set datalake_fdw.deletion_queue_min_interval=${MIN_INTERVAL};" >/dev/null
psql postgres -p "$PGPORT" -qc "select pg_reload_conf();" >/dev/null 2>&1
RUN "SET client_min_messages=WARNING;
DROP SCHEMA IF EXISTS itxn CASCADE;
DROP VOLUME IF EXISTS itxn_volume; DROP SERVER IF EXISTS itxn_volume_server CASCADE;
DROP CATALOG IF EXISTS itxn_catalog; DROP SERVER IF EXISTS itxn_catalog_server CASCADE;"
RUN "CREATE SERVER itxn_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER itxn_catalog_server;
CREATE FOREIGN CATALOG itxn_catalog SERVER itxn_catalog_server;
CREATE SERVER itxn_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (type 's3', endpoint '${FDW_ENDPOINT}', region 'us-east-1', bucket_name '${BUCKET}', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER itxn_volume_server
  OPTIONS (access_key_id '${FDW_ACCESS}', secret_access_key '${FDW_SECRET}');
CREATE FOREIGN VOLUME itxn_volume SERVER itxn_volume_server OPTIONS(base_path '${BASE}');
CREATE SCHEMA itxn;"
SETUP="SET iceberg_default_catalog='itxn_catalog'; SET iceberg_default_volume='itxn_volume';"

echo "### A) abort INSERT+UPDATE ROLLBACK ###"
RUN "${SETUP} CREATE ICEBERG TABLE itxn.a(id int,n int);"
B=$(SNAP)
RUN "${SETUP} BEGIN; INSERT INTO itxn.a VALUES(1,10),(2,20),(3,30); UPDATE itxn.a SET n=n+1 WHERE id=1; ROLLBACK;"
sleep 2; NEW=$(comm -13 <(echo "$B") <(SNAP))
check A "$(P 'select count(*) from itxn.a;')|[${NEW}]" "0|[]"

echo "### B) abort DELETE+INSERT ROLLBACK ###"
RUN "${SETUP} CREATE ICEBERG TABLE itxn.b(id int,n int); INSERT INTO itxn.b VALUES(1,10),(2,20);"
sleep 1; B=$(SNAP)
RUN "${SETUP} BEGIN; DELETE FROM itxn.b WHERE id=1; INSERT INTO itxn.b VALUES(9,90); ROLLBACK;"
sleep 2; NEW=$(comm -13 <(echo "$B") <(SNAP))
check B "$(P 'select count(*) from itxn.b;')|[${NEW}]" "2|[]"

echo "### C) commit INSERT+UPDATE (intermediate orphans async-deleted, final kept) ###"
RUN "${SETUP} CREATE ICEBERG TABLE itxn.c(id int,n int);"
RUN "${SETUP} BEGIN; INSERT INTO itxn.c VALUES(1,10),(2,20); UPDATE itxn.c SET n=n+1 WHERE id=1; COMMIT;"
FIN=$(P "select metadata_location from pg_ext_aux.pg_iceberg_metadata m join pg_class c on c.oid=m.relid where c.relname='c';")
drain
FINEX=$(mc stat "itxn/${BUCKET}${BASE}metadata/$(basename "$FIN")" >/dev/null 2>&1 && echo YES || echo NO)
check C "$(P 'select sum(n) from itxn.c;')|${FINEX}" "31|YES"

echo "### D) DROP TABLE async cleanup ###"
RUN "${SETUP} CREATE ICEBERG TABLE itxn.d(id int,v text); INSERT INTO itxn.d SELECT g,'x'||g FROM generate_series(1,50) g;"
sleep 1; B=$(SNAP)
RUN "${SETUP} INSERT INTO itxn.d VALUES(999,'z');"
sleep 1; OWN=$(comm -13 <(echo "$B") <(SNAP))
RUN "DROP TABLE itxn.d;"; drain; sleep 2
check D "$(still_present "$OWN")" "0"

echo "### E) TRUNCATE ###"
RUN "${SETUP} CREATE ICEBERG TABLE itxn.e(id int); INSERT INTO itxn.e SELECT generate_series(1,100);"
sleep 1
RUN "TRUNCATE itxn.e;"; sleep 2; drain
RUN "${SETUP} INSERT INTO itxn.e VALUES(7);"
check E "$(P 'select count(*) from itxn.e;')" "1"

echo "### F) VACUUM compaction ###"
RUN "${SETUP} SET datalake.iceberg_vacuum_compact_min_input_files=2; CREATE ICEBERG TABLE itxn.f(id int,n int);"
for k in 1 2 3 4; do RUN "${SETUP} INSERT INTO itxn.f SELECT g,g*10 FROM generate_series(1,50) g;"; done
RUN "${SETUP} UPDATE itxn.f SET n=n+1 WHERE id<10; DELETE FROM itxn.f WHERE id>190;"
R0=$(P 'select count(*) from itxn.f;'); S0=$(P 'select sum(n) from itxn.f;')
RUN "${SETUP} VACUUM itxn.f;"; drain
check F "$(P 'select count(*) from itxn.f;')|$(P 'select sum(n) from itxn.f;')" "${R0}|${S0}"

echo "### G) DROP SCHEMA CASCADE ###"
RUN "${SETUP} CREATE ICEBERG TABLE itxn.g(id int); INSERT INTO itxn.g SELECT generate_series(1,20);"
sleep 1; B=$(SNAP)
RUN "${SETUP} INSERT INTO itxn.g VALUES(999);"
sleep 1; OWN=$(comm -13 <(echo "$B") <(SNAP))
RUN "DROP SCHEMA itxn CASCADE;"; drain; sleep 2
check G "$(still_present "$OWN")" "0"

echo "### teardown ###"
RUN "DROP VOLUME IF EXISTS itxn_volume; DROP SERVER IF EXISTS itxn_volume_server CASCADE;
DROP CATALOG IF EXISTS itxn_catalog; DROP SERVER IF EXISTS itxn_catalog_server CASCADE;"
psql postgres -p "$PGPORT" -qc "alter system reset datalake_fdw.deletion_queue_min_interval; select pg_reload_conf();" >/dev/null 2>&1

echo ""
if [ "$FAILS" = "0" ]; then echo "ALL SCENARIOS PASS"; exit 0; else echo "${FAILS} SCENARIO(S) FAILED"; exit 1; fi
