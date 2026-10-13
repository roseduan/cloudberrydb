#!/usr/bin/env bash
# End-to-end acceptance for issue #457 (gpbackup/gprestore metadata-only
# iceberg).  Runs against a live cluster with datalake_fdw and the lakehouse
# MinIO; needs gpbackup/gprestore built from the matching gpbackup branch on
# PATH.  Exits non-zero on the first mismatch.
set -euo pipefail
export PGPORT=${PGPORT:-7000}
PORT=$PGPORT
DB=${DB:-iceberg457}
BACKUP_DIR=${BACKUP_DIR:-/tmp/iceberg457_backup}
LOG_DIR=${LOG_DIR:-/tmp}
# External-catalog table name is unique per run: Polaris outlives the
# databases this script drops, and an old table would be adopted with old rows.
EXT="ext457_$(date +%s)"
psql_q() { psql -p "$PORT" -d "$1" -XAtqc "$2"; }
fail() { echo "iceberg #457 e2e: FAIL: $*" >&2; exit 1; }
# gprestore names its report file after its start second; back-to-back runs
# in the same second collide ("Unable to open restore report file", exit 1).
next_restore() { sleep 1; }

dropdb -p "$PORT" --if-exists "$DB"
dropdb -p "$PORT" --if-exists "${DB}_r"
createdb -p "$PORT" "$DB"
rm -rf "$BACKUP_DIR"; mkdir -p "$BACKUP_DIR"
psql -p "$PORT" -d "$DB" -Xq -v ON_ERROR_STOP=1 <<'SQL'
CREATE EXTENSION datalake_fdw;
CREATE SERVER e2e_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER e2e_cat_srv;
CREATE FOREIGN CATALOG e2e_cat SERVER e2e_cat_srv;
CREATE SERVER e2e_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER e2e_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME e2e_vol SERVER e2e_vol_srv OPTIONS (base_path '/iceberg457_e2e/');
CREATE ICEBERG TABLE kept (id int, region text) CATALOG e2e_cat VOLUME e2e_vol PARTITION BY (region);
CREATE ICEBERG TABLE dropped_later (id int) CATALOG e2e_cat VOLUME e2e_vol;
INSERT INTO kept VALUES (1, 'a'), (2, 'b');
INSERT INTO dropped_later VALUES (1);
COMMENT ON TABLE kept IS 'restored by #457';
CREATE SCHEMA rs_src;
CREATE ICEBERG TABLE rs_src.t (id int) CATALOG e2e_cat VOLUME e2e_vol;
INSERT INTO rs_src.t VALUES (10), (20);
-- External catalog (Polaris): the catalog owns the metadata, so only DDL is
-- backed up and a restore must read the catalog's latest version.
CREATE SERVER e2e_pvol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER e2e_pvol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME e2e_pvol SERVER e2e_pvol_srv
    OPTIONS (base_path '/iceberg457_polaris/', allow_writes 'true');
CREATE SERVER e2e_pcat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'polaris', url 'http://polaris:8181/api/catalog');
CREATE USER MAPPING FOR current_user SERVER e2e_pcat_srv
    OPTIONS (client_id 'root', client_secret 's3cr3t', scope 'PRINCIPAL_ROLE:ALL');
CREATE FOREIGN CATALOG e2e_pcat SERVER e2e_pcat_srv
    OPTIONS (catalog_name 'polaris_default_catalog', default_namespace 'public');
SQL
psql_q "$DB" "CREATE ICEBERG TABLE $EXT (id int) CATALOG e2e_pcat VOLUME e2e_pvol"
psql_q "$DB" "INSERT INTO $EXT VALUES (1), (2)"

gpbackup --dbname "$DB" --backup-dir "$BACKUP_DIR" --metadata-only 2>&1 | tee "$LOG_DIR/iceberg457_backup.log"
TS=$(grep -oE 'Backup Timestamp = [0-9]+' "$LOG_DIR/iceberg457_backup.log" | awk '{print $4}')
[ -n "$TS" ] || fail "no backup timestamp"
META=$(ls "$BACKUP_DIR"/*/backups/*/"$TS"/gpbackup_"$TS"_metadata.sql)
grep -A6 "CREATE ICEBERG TABLE public.$EXT " "$META" > "$LOG_DIR/iceberg457_ext_ddl.sql" || fail "no DDL for $EXT"
grep -q "location 's3://" "$LOG_DIR/iceberg457_ext_ddl.sql" || fail "external table lost its location option"
if grep -q "attach_metadata('public.$EXT'" "$META"; then
    fail "an attach call was emitted for the external-catalog table"
fi

# After the backup: one more commit (must NOT come back), and a DROP whose
# files the deletion queue cleans (must be skipped with a warning).
psql_q "$DB" "INSERT INTO kept VALUES (3, 'c')"
psql_q "$DB" "INSERT INTO $EXT VALUES (3)"
psql_q "$DB" "DROP TABLE dropped_later"
# The deletion-queue consumer is autovacuum-driven: poll until it has
# actually removed dropped_later's files instead of guessing a sleep.
left=1
for i in $(seq 1 120); do
    left=$(psql_q "$DB" "SELECT count(*) FROM pg_ext_aux.pg_iceberg_deletion_queue WHERE table_qname = 'public.dropped_later'")
    [ "$left" = "0" ] && break
    psql_q "$DB" "VACUUM" >/dev/null || true
    sleep 10
done
[ "$left" = "0" ] || fail "deletion queue never drained for dropped_later"

# Scenario 1: disaster -> restore into a fresh database.  dropped_later's
# metadata is gone: restore_table() warns and skips it, the restore goes on.
gprestore --timestamp "$TS" --backup-dir "$BACKUP_DIR" --redirect-db "${DB}_r" --create-db --exclude-schema rs_src 2>&1 | tee "$LOG_DIR/iceberg457_restore.log"
[ "$(psql_q "${DB}_r" "SELECT string_agg(id::text, ',' ORDER BY id) FROM kept")" = "1,2" ] || fail "kept should hold the backup-time rows"
[ "$(psql_q "${DB}_r" "SELECT to_regclass('dropped_later') IS NULL")" = "t" ] || fail "dropped_later should be skipped"
[ "$(psql_q "${DB}_r" "SELECT obj_description('kept'::regclass)")" = "restored by #457" ] || fail "comment lost"
psql_q "${DB}_r" "INSERT INTO kept VALUES (9, 'z')"
[ "$(psql_q "${DB}_r" "SELECT count(*) FROM kept")" = "3" ] || fail "restored table not writable"
# External catalog: DDL only, adopted from Polaris -> latest data incl. row 3.
[ "$(psql_q "${DB}_r" "SELECT string_agg(id::text, ',' ORDER BY id) FROM $EXT")" = "1,2,3" ] || fail "external-catalog table should read the catalog's latest rows"

next_restore
# Scenario 2: table exists -> skipped, untouched (restore into the source db).
before=$(psql_q "$DB" "SELECT metadata_location FROM pg_ext_aux.pg_iceberg_metadata WHERE relid='kept'::regclass")
gprestore --timestamp "$TS" --backup-dir "$BACKUP_DIR" --on-error-continue 2>&1 | tee "$LOG_DIR/iceberg457_restore2.log" || true
[ "$(psql_q "$DB" "SELECT string_agg(id::text, ',' ORDER BY id) FROM kept")" = "1,2,3" ] || fail "existing kept lost rows"
[ "$(psql_q "$DB" "SELECT metadata_location FROM pg_ext_aux.pg_iceberg_metadata WHERE relid='kept'::regclass")" = "$before" ] || fail "existing kept was touched"

next_restore
# Scenario 3: --include-table naming an existing iceberg table is refused
# by gprestore's pre-restore relation check, as for any existing table.
set +e
gprestore --timestamp "$TS" --backup-dir "$BACKUP_DIR" --include-table public.kept 2>&1 | tee "$LOG_DIR/iceberg457_restore3.log"
rc=${PIPESTATUS[0]}
set -e
[ "$rc" != "0" ] || fail "--include-table of an existing table should be refused"
grep -q "Relation public.kept already exists" "$LOG_DIR/iceberg457_restore3.log" || fail "no existing-relation error"
[ "$(psql_q "$DB" "SELECT metadata_location FROM pg_ext_aux.pg_iceberg_metadata WHERE relid='kept'::regclass")" = "$before" ] || fail "existing kept was touched by --include-table"

next_restore
# Scenario 4: --redirect-schema into the SOURCE database.  rs_src.t is still
# live on the same metadata, so the attach must be refused (fatal) and no
# rs_dst.t may be left behind.
psql_q "$DB" "CREATE SCHEMA rs_dst"
set +e
gprestore --timestamp "$TS" --backup-dir "$BACKUP_DIR" --include-schema rs_src --redirect-schema rs_dst 2>&1 | tee "$LOG_DIR/iceberg457_restore4.log"
rc=${PIPESTATUS[0]}
set -e
[ "$rc" != "0" ] || fail "redirect-schema next to the live source must not succeed"
grep -q "iceberg metadata is already used by table rs_src.t" "$LOG_DIR/iceberg457_restore4.log" || fail "no shared-metadata refusal"
[ "$(psql_q "$DB" "SELECT to_regclass('rs_dst.t') IS NULL")" = "t" ] || fail "refused attach left rs_dst.t behind"
[ "$(psql_q "$DB" "SELECT string_agg(id::text, ',' ORDER BY id) FROM rs_src.t")" = "10,20" ] || fail "source rs_src.t was disturbed"

next_restore
# Scenario 5: --redirect-schema into the rebuilt database (no live source
# there) -> rs_dst.t holds the backup-time rows.
psql_q "${DB}_r" "CREATE SCHEMA rs_dst"
gprestore --timestamp "$TS" --backup-dir "$BACKUP_DIR" --redirect-db "${DB}_r" --include-schema rs_src --redirect-schema rs_dst 2>&1 | tee "$LOG_DIR/iceberg457_restore5.log"
[ "$(psql_q "${DB}_r" "SELECT string_agg(id::text, ',' ORDER BY id) FROM rs_dst.t")" = "10,20" ] || fail "redirect-schema restore should hold the backup-time rows"
[ "$(psql_q "${DB}_r" "SELECT to_regclass('rs_src.t') IS NULL")" = "t" ] || fail "redirect-schema should not create rs_src.t"

# Drop the Polaris table so runs do not accumulate them in the catalog.
psql_q "${DB}_r" "DROP TABLE $EXT" || true

echo "iceberg #457 e2e: PASS"
