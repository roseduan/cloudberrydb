-- ============================================================
-- bgw_teardown.sql  (MUST be the LAST test in the schedule)
--
-- Stops the per-database time_series scheduler so the NEXT
-- `make installcheck` can recreate the test database.
--
-- Why this is needed:
--   The launcher spawns a resident scheduler in every database that
--   has the time_series extension installed, and that scheduler keeps
--   an open connection to contrib_regression.  pg_regress drops and
--   recreates the test database at the START of every run with a plain
--   `DROP DATABASE` (it does NOT pass WITH FORCE), which fails with
--   "database is being accessed by other users" whenever the scheduler
--   is still connected.
--
--   stop_background_workers() sends a BGW_MSG_STOP to the launcher,
--   which terminates the running scheduler and transitions its per-DB
--   entry to DISABLED.  The launcher's automatic state trans loop
--   ignores DISABLED entries, so no respawn happens and the next run's
--   DROP DATABASE succeeds deterministically.  Mirrors TimescaleDB
--   upstream's bgw teardown pattern.
--
--   Guarded with IF EXISTS since earlier tests in the schedule may
--   have DROP EXTENSION-ed time_series from contrib_regression -- in
--   that case the launcher has already reaped the per-DB entry so
--   there's nothing to stop.
-- ============================================================
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'time_series') THEN
        PERFORM time_series.stop_background_workers();
    END IF;
END$$;
