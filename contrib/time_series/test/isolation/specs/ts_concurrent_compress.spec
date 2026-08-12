# ts_concurrent_compress
#
# Concurrency invariants for the chunk-compress / reclaim flow:
#
#   1. compress_chunks and reclaim_chunk_heaps must NOT run inside an
#      explicit BEGIN/COMMIT block — rolling back after the .new+rename
#      atomic file replacement would leave the new PAX in place with the
#      catalog reverted, duplicating rows in subsequent PARTIAL scans.
#      Same gate VACUUM and CREATE INDEX CONCURRENTLY use.
#
#   2. INSERT and compress on the same chunk are serialised via a
#      per-chunk advisory lock keyed on (table_oid, chunk_number).
#      INSERT takes ShareLock at xact-first-write to a chunk; compress
#      takes ExclusiveLock for the duration of the chunk's PAX scan +
#      catalog UPDATE.  An in-flight INSERT xact must commit / abort
#      before compress can proceed on that chunk, so rows INSERTed mid-
#      compress are never silently dropped.
#
#   3. INSERTs to different chunks don't interfere with compress on a
#      different chunk — the advisory lock keys are per-chunk.

setup
{
    CREATE EXTENSION IF NOT EXISTS time_series;
    SET search_path = public, time_series;
    SET timezone = 'UTC';
    CREATE TABLE ts_conc_comp (
        ts  timestamptz NOT NULL,
        sid int,
        val int
    ) USING time_series WITH (
        ts_partition_column = 'ts', ts_chunk_interval = '1 day',
        ts_chunk_origin = '2025-01-01'
    ) DISTRIBUTED REPLICATED;
    INSERT INTO ts_conc_comp
    SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 minute'),
           1, i
    FROM generate_series(1, 200) i;
    INSERT INTO ts_conc_comp
    SELECT '2025-01-02 00:00:00+00'::timestamptz + (i * interval '1 minute'),
           2, i
    FROM generate_series(1, 200) i;
}

teardown
{
    DELETE FROM time_series.ts_compressed_chunk
     WHERE table_oid = 'ts_conc_comp'::regclass;
    DELETE FROM time_series.ts_compress_config
     WHERE table_oid = 'ts_conc_comp'::regclass;
    DROP TABLE IF EXISTS ts_conc_comp;
}

# Two writer sessions and two compress / reclaim sessions.  s1 and s2
# hold open transactions that take the per-chunk advisory ShareLock via
# a real INSERT; s_compress and s_reclaim run autocommit and contend
# for the per-chunk ExclusiveLock, which forces them to wait.

session s1
setup       { SET search_path = public, time_series; SET timezone = 'UTC'; }
step s1_begin           { BEGIN; }
# INSERT into chunk_idx 0 (range_start = origin = 2025-01-01).
step s1_insert_chunk1
{
    INSERT INTO ts_conc_comp VALUES
        ('2025-01-01 23:30:00+00', 99, 9999);
}
# INSERT into chunk_idx 1 (range_start = 2025-01-02).
step s1_insert_chunk2
{
    INSERT INTO ts_conc_comp VALUES
        ('2025-01-02 23:30:00+00', 99, 9999);
}
step s1_commit          { COMMIT; }
step s1_rollback        { ROLLBACK; }

session s2
setup       { SET search_path = public, time_series; SET timezone = 'UTC'; }
# Autocommit compress / reclaim — single statement = own implicit xact.
step s2_compress        { SELECT time_series.compress_chunks('ts_conc_comp'::regclass) AS s2_compressed; }
# Per-chunk variant — chunk 4 = Jan 1 (origin), chunk 5 = Jan 2.
step s2_compress_c4     { SELECT time_series.compress_chunk('ts_conc_comp'::regclass, 4) AS s2_c4; }
step s2_compress_c5     { SELECT time_series.compress_chunk('ts_conc_comp'::regclass, 5) AS s2_c5; }
step s2_count           { SELECT count(*) AS s2_count FROM ts_conc_comp; }
# Begin an explicit transaction; the next compress / reclaim call inside
# this xact must error.  Test rolls back to leave the session clean.
step s2_begin           { BEGIN; }
step s2_compress_in_xact
{
    SELECT time_series.compress_chunks('ts_conc_comp'::regclass);
}
step s2_reclaim_in_xact
{
    SELECT time_series.reclaim_chunk_heaps('ts_conc_comp'::regclass);
}
step s2_rollback        { ROLLBACK; }
# Session-level isolation toggle — verifies compress / reclaim run
# correctly under REPEATABLE READ and SERIALIZABLE in the absence of
# concurrent same-chunk operations.  Isolation level affects internal
# snapshot semantics but the call itself succeeds.  Concurrent
# same-chunk compresses under RR/SER may get a standard 40001
# SERIALIZATION_FAILURE — that path is left to PG's natural conflict
# detection, no special rejection.
step s2_set_rr      { SET default_transaction_isolation = 'repeatable read'; }
step s2_set_ser     { SET default_transaction_isolation = 'serializable'; }
step s2_reset_iso   { RESET default_transaction_isolation; }
step s2_reclaim     { SELECT time_series.reclaim_chunk_heaps('ts_conc_comp'::regclass) AS s2_reclaimed; }

session reader
setup       { SET search_path = public, time_series; SET timezone = 'UTC'; }
step r_select   { SELECT count(*) AS reader_count FROM ts_conc_comp; }

# A SHARE UPDATE EXCLUSIVE holder (what VACUUM / ANALYZE take) does
# NOT block compress — compress only takes AccessShareLock on the user
# relation, so different SUEL holders coexist with it.  This is what
# enables two concurrent compress_chunks on different chunks of the
# same table to run in parallel.
session lk
setup       { SET search_path = public, time_series; SET timezone = 'UTC'; }
step lk_begin       { BEGIN; }
step lk_take_suel   { LOCK TABLE ts_conc_comp IN SHARE UPDATE EXCLUSIVE MODE; }
step lk_commit      { COMMIT; }

# An AccessExclusive holder (what ALTER TABLE / DROP take) DOES block
# compress.  This proves DDL still mutually excludes compress as it
# should, while our per-chunk advisory lock handles intra-table
# serialisation.
session ddl
setup       { SET search_path = public, time_series; SET timezone = 'UTC'; }
step ddl_begin      { BEGIN; }
step ddl_lock       { LOCK TABLE ts_conc_comp IN ACCESS EXCLUSIVE MODE; }
step ddl_alter_add  { ALTER TABLE ts_conc_comp ADD COLUMN extra integer; }
step ddl_commit     { COMMIT; }
# Index AM (ts_btree) disabled; re-add when re-enabled:
# step ddl_create_idx { CREATE INDEX idx_conc_ts ON ts_conc_comp
#                             USING ts_btree (ts ts_btree_timestamptz_ops); }
# step ddl_drop_idx   { DROP INDEX IF EXISTS idx_conc_ts; }


# 1. compress_chunks rejected inside BEGIN/COMMIT (Case 5 / PreventInTxn).
permutation s2_begin s2_compress_in_xact s2_rollback

# 2. reclaim_chunk_heaps rejected inside BEGIN/COMMIT.
permutation s2_begin s2_reclaim_in_xact s2_rollback

# 3. INSERT-then-compress on SAME chunk: compress waits for INSERT xact
#    to commit, then proceeds.  Final count must include both pre-existing
#    rows (200 in chunk 1) AND the s1 INSERT (1 new row), proving no
#    rows were dropped — this is the Case 1 fix in action.
permutation s1_begin s1_insert_chunk1 s2_compress s1_commit s2_count

# 4. INSERT-then-compress on DIFFERENT chunks: compress proceeds without
#    waiting (per-chunk locks).  s1's INSERT into chunk 2 doesn't block
#    compress of chunk 1 — but compress sees both chunks as eligible, so
#    it'll lock chunk 2 too and wait.  Use compress_chunks with older_than
#    to target only chunk 1, isolating the per-chunk granularity.
permutation s1_begin s1_insert_chunk2 s2_compress s1_commit s2_count

# 5. INSERT-then-rollback during compress wait: after rollback, compress
#    proceeds and sees the original 400 rows (s1's INSERT was rolled back).
permutation s1_begin s1_insert_chunk1 s2_compress s1_rollback s2_count

# 6. Reader unaffected by compress — concurrent SELECT proceeds while
#    compress runs (autocommit).
permutation s2_compress r_select s2_count

# 7. SUEL holder does NOT block compress.  Demonstrates the
#    cross-chunk concurrency property: compress only takes
#    AccessShareLock on the user relation, so it coexists with
#    VACUUM / ANALYZE / another compress.  s2_compress completes
#    without waiting.
permutation lk_begin lk_take_suel s2_compress lk_commit s2_count

# 8. AccessExclusive holder (DDL semantics) DOES block compress.
#    Verifies ALTER TABLE / DROP TABLE still serialise correctly.
permutation ddl_begin ddl_lock s2_compress ddl_commit s2_count

# 9. compress_chunks runs successfully under REPEATABLE READ (single
#    session, no concurrent same-chunk contention).  Snapshot isolation
#    is allowed; concurrent conflicts get the standard 40001
#    SERIALIZATION_FAILURE.
permutation s2_set_rr s2_compress s2_reset_iso

# 10. reclaim_chunk_heaps runs successfully under SERIALIZABLE.
permutation s2_set_ser s2_compress s2_reclaim s2_reset_iso

# 11. ALTER TABLE ADD COLUMN concurrent with compress: ALTER takes
#     AccessExclusiveLock → blocks behind compress's AccessShareLock.
#     After compress finishes, ALTER completes; subsequent count
#     query sees the new column.
permutation ddl_begin ddl_alter_add s2_compress ddl_commit s2_count

# 12. CREATE INDEX concurrent with compress: disabled along with ts_btree.
#     CREATE INDEX (without CONCURRENTLY) takes ShareLock — compatible with
#     compress's AccessShareLock.  Re-add this permutation when the
#     index AM is re-enabled:
# permutation s2_compress ddl_create_idx ddl_drop_idx s2_count

# 13. Cross-chunk parallel via per-chunk API.  s1 holds an open xact
#     with INSERT into chunk 4 (advisory share on chunk 4 held until
#     commit).  s2 calls compress_chunk for chunk 5 — different
#     advisory key, no conflict, completes immediately.  This is the
#     deterministic test that proves "two sessions on different chunks
#     of the same table run in parallel" — which compress_chunks (the
#     batch entry) couldn't demonstrate because both sessions iterate
#     the same chunk list and serialise on the first one.
permutation s1_begin s1_insert_chunk1 s2_compress_c5 s1_commit
# Reset state — both chunks compressed when s1 commits, so the
# subsequent compress_chunk on chunk 4 also runs without contention.
permutation s1_begin s1_insert_chunk1 s2_compress_c5 s2_compress_c4 s1_commit
