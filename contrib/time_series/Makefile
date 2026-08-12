# contrib/time_series/Makefile
#
# Build and test configuration for the time_series extension.
#
# Test layout (CBDB-style subdir layout, every suite delegated to its own
# Makefile and invoked by the umbrella installcheck target below):
#   test/regress/access/      — hypertable / tableam / compression / chunk_append
#   test/regress/             — CAGG / BGW regression tests
#   test/isolation/           — TimescaleDB-style isolation tests (.spec)
#   test/isolation2/          — CAGG concurrency tests (pg_isolation2_regress)

MODULE_big = time_series

OBJS = \
	src/time_series.o \
	src/time_bucket.o \
	src/gapfill/gapfill.o \
	src/gapfill/plan.o \
	src/gapfill/exec.o \
	src/access/ts_config.o \
	src/access/ts_tableam.o \
	src/access/ts_scan.o \
	src/access/ts_chunk_append.o \
	src/access/ts_func_cache.o \
	src/access/ts_catalog.o \
	src/access/ts_ddl.o \
	src/access/ts_wal.o \
	src/compress/ts_compress.o \
	src/compress/ts_compress_pax.o \
	src/compress/ts_pax_wal.o \
	src/storage/ts_smgr.o \
	src/storage/ts_fork_name.o \
	src/cagg/create.o \
	src/cagg/insert.o \
	src/cagg/refresh.o \
	src/cagg/watermark_constify.o \
	src/cagg/time_bucket_pushdown.o \
	src/cagg/planner_hook.o \
	src/bgw/bgw_init.o \
	src/bgw/launcher.o \
	src/bgw/scheduler.o \
	src/bgw/bgw_counter.o \
	src/bgw/bgw_message_queue.o \
	src/bgw/bgw_interface.o \
	src/bgw/job.o \
	src/bgw/job_stat.o \
	src/bgw/job_stat_history.o \
	src/bgw/timer.o \
	src/bgw/utils.o \
	src/bgw_policy/cagg_refresh_policy.o \
	test/mock_time/params.o \
	test/mock_time/timer_mock.o \
	test/mock_time/scheduler_mock.o \
	test/mock_time/log.o

EXTENSION = time_series
DATA = time_series--1.0.sql
PGFILEDESC = "time_series - time bucket and gapfill functions"

# PAX storage integration (same approach as storage_am)
PG_CXXFLAGS += -std=c++17
PRIVATE_CPP_FLAGS = -I$(top_builddir)/contrib/pax_storage/src/cpp \
	-I$(top_builddir)/contrib/pax_storage/build/src/cpp

# libpq headers + link — needed for CdbDispatchCommand result parsing
# (PQresultStatus, PQgetvalue, etc.) in ts_scan.c.  Matches the pattern
# used by other backend cdb code, e.g. cdbrelsize.c.
PG_CPPFLAGS += -I$(libpq_srcdir)
SHLIB_LINK += -L$(libpq_builddir) -lpq
# PAX C++ symbols (pax::LocalFileSystem, pax::PaxFilter, pax::pax_enable_*, ...)
# resolve at runtime from pax.so, which CBDB auto-preloads via the built-in
# process_shared_preload_libraries_array (USE_PAX_STORAGE).  RTLD_GLOBAL on
# shared_preload makes pax.so's symbols visible when time_series.so is loaded.
#
# Do NOT link libpaxformat.so here: pax.so already contains libpaxformat's
# object code statically (both are add_library(SHARED ${pax_storage_src} ...)
# in contrib/pax_storage/src/cpp/cmake/), so pulling libpaxformat.so into the
# same process creates 28K duplicate C++ globals (protobuf descriptor tables,
# ShutdownData singletons, ...) and corrupts ld-linux's relocation state,
# making later dlopen()s SIGSEGV.
SHLIB_LINK += -Wl,--unresolved-symbols=ignore-in-object-files

# ---------------------------------------------------------------------------
# Regression tests now live under test/regress/access/ (hypertable/compress)
# and test/regress/ (CAGG/BGW), both invoked from the installcheck target
# below.  The PGXS REGRESS/REGRESS_OPTS variables are intentionally empty;
# pg_regress is driven per-subdir instead.
# ---------------------------------------------------------------------------
# Isolation tests (test/isolation/specs/ + test/isolation/expected/)
# ---------------------------------------------------------------------------
ISOLATION = ts_concurrent_insert ts_insert_select ts_insert_rollback \
	ts_multi_writer ts_multi_reader_writer ts_repeatable_read \
	ts_copy_insert ts_concurrent_compress ts_concurrent_chunk_create
# ts_chunk_creation_race retained on disk but disabled — written for the
# old SUEL-based chunk creation; superseded by ts_concurrent_chunk_create
# under the duplicate-tolerant model.
ISOLATION_OPTS = --inputdir=$(srcdir)/test/isolation --outputdir=$(srcdir)/test/isolation

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
override FAULTINJECTOR_OPTS =
else
subdir = contrib/time_series
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
override FAULTINJECTOR_OPTS =
endif

# C++ custom compile: pass PRIVATE_CPP_FLAGS (PAX headers) to .cc files only
ifeq ($(autodepend)+$(GCC), yes+yes)
%.o: %.cc
	@if test ! -d $(DEPDIR); then mkdir -p $(DEPDIR); fi
	$(CXX) $(PRIVATE_CPP_FLAGS) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@ -MMD -MP -MF $(DEPDIR)/$(*F).Po
else
%.o: %.cc
	$(CXX) $(PRIVATE_CPP_FLAGS) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@
endif

# Install libpaxformat.so alongside time_series.so (runtime dependency).
# Uses installdirs as a hook point that runs during PGXS install.
installdirs: install-paxformat-dep
.PHONY: install-paxformat-dep
install-paxformat-dep:
	@paxlib=$(top_builddir)/contrib/pax_storage/build/src/cpp/libpaxformat.so; \
	if [ ! -f "$$paxlib" ]; then \
		echo "ERROR: libpaxformat.so not found at $$paxlib — rebuild PAX storage before installing time_series" >&2; \
		exit 1; \
	fi; \
	echo "Installing libpaxformat.so to $(DESTDIR)$(libdir)"; \
	$(MKDIR_P) '$(DESTDIR)$(libdir)'; \
	$(INSTALL_SHLIB) "$$paxlib" '$(DESTDIR)$(libdir)/libpaxformat.so'

# Ensure shared_preload_libraries includes time_series before running tests.
# time_series requires preloading for Custom Scan deserialization on segments.
# This prerequisite makes "make installcheck" work in any environment (ICW, CI,
# manual) by auto-configuring the cluster when needed.
#
# installcheck fully overrides the PGXS default rule (which would use
# the top-level REGRESS/ISOLATION variables — REGRESS is empty and
# ISOLATION is only used by the manual `isolationcheck` target below).
# The suites are dispatched to their own subdir Makefiles:
#   1. _ts_ensure_preload    — make sure shared_preload_libraries contains time_series
#   2. _ts_ensure_isolation_deps — copy gpdiff.pl etc. into test/isolation/
#   3. _ts_drop_stale_dbs    — clean schedulers + drop test DBs left behind
#                              by the previous run, so the next pg_regress's
#                              DROP DATABASE IF EXISTS is unblocked
#   4. _ts_regress           — subdir test/regress/ (CAGG regression tests)
#   5. _ts_access            — subdir test/regress/access/ (hypertable/compress
#                              + the 9 pg_isolation_regress specs listed
#                              in ISOLATION below — access/Makefile drives
#                              pg_isolation_regress itself; those specs
#                              are NOT double-run at this level)
#   6. _ts_isolation2        — subdir test/isolation2/ (CAGG concurrency tests)
# Umbrella-driven CI (which invokes `make installcheck` per contrib) and
# `make installcheck-world` thus pick up all suites automatically.
installcheck: _ts_ensure_preload _ts_ensure_isolation_deps _ts_drop_stale_dbs _ts_regress _ts_access _ts_isolation2

# Make installcheck idempotent across consecutive runs.  Each pg_regress
# phase ends with a scheduler still attached to its test DB (extension
# stays installed, launcher keeps the worker alive).  On the *next*
# `make installcheck`, pg_regress's one-shot `DROP DATABASE IF EXISTS`
# then fails with "is being accessed by other users".
#
# This target runs before any pg_regress invocation and:
#   * Calls time_series.stop_background_workers() — sets the launcher's
#     entry to DISABLED with stopped_by_message=true (TimescaleDB-style
#     persistent stop), preventing respawn on the next launcher poll.
#     This is a no-op if the DB doesn't exist or doesn't have the
#     extension installed (psql exits non-zero, suppressed).
#   * Drops the DB.  The persistent-stop above means no scheduler holds
#     an idle session, so DROP DATABASE IF EXISTS succeeds.
#
# Touches only state our own code created; same DBs pg_regress would
# drop+recreate anyway.  No changes to user-written test SQL.
.PHONY: _ts_drop_stale_dbs
_ts_drop_stale_dbs:
	@for db in contrib_regression isolation_regression isolation2test; do \
		psql -X -d "$$db" -tAc "SELECT time_series.stop_background_workers()" \
			>/dev/null 2>&1 || true; \
		for i in 1 2 3 4 5; do \
			psql -X -d postgres -tAc \
				"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$$db'" \
				>/dev/null 2>&1 || true; \
			out=$$(psql -X -d postgres -c "DROP DATABASE IF EXISTS $$db" 2>&1); \
			echo "$$out" | grep -qE 'DROP DATABASE|does not exist' && break; \
			sleep 1; \
		done; \
		echo "$$out" | grep -qE 'DROP DATABASE|does not exist' || \
			echo "WARNING: could not drop database $$db after 5 retries -- subsequent pg_regress may fail with 'database is being accessed by other users'" >&2; \
	done

# Hypertable / tableam / compression / chunk_append tests under
# test/regress/access/.
.PHONY: _ts_access
_ts_access:
	$(MAKE) -C test/regress/access installcheck

# Regression tests under test/regress/ (CAGG tests, subdir layout).
.PHONY: _ts_regress
_ts_regress:
	$(MAKE) -C test/regress installcheck

# Isolation2 tests under test/isolation2/ — CAGG concurrency tests in
# pg_isolation2_regress format.
.PHONY: _ts_isolation2
_ts_isolation2:
	$(MAKE) -C test/isolation2 installcheck

# Backward-compatible alias for callers that target the suite explicitly.
.PHONY: installcheck-isolation2
installcheck-isolation2: _ts_ensure_preload _ts_isolation2

# _ts_ensure_preload depends on install: make installcheck must observe the
# CURRENTLY-EDITED source, not a stale .so left over from a prior build.
# Without this prerequisite a developer who runs only `make installcheck`
# after editing source will silently test against outdated installed code.
# `make install` is idempotent when nothing changed, so this only costs a
# few milliseconds on a clean tree.
.PHONY: _ts_ensure_preload
_ts_ensure_preload: install
	@current=$$(psql -X -t -A -c "SHOW shared_preload_libraries" template1 2>/dev/null) || \
		{ echo "ERROR: cannot query the cluster (SHOW shared_preload_libraries failed) — is it running?  Refusing to auto-restart because an unreachable cluster may be intentionally stopped for debugging / core-dump analysis." >&2; exit 1; }; \
	if echo "$$current" | grep -qw 'time_series'; then \
		echo "time_series already in shared_preload_libraries"; \
	elif [ -z "$$current" ]; then \
		gpconfig -c shared_preload_libraries -v 'time_series' || exit 1; \
		gpstop -ra || exit 1; \
	else \
		gpconfig -c shared_preload_libraries -v "$$current,time_series" || exit 1; \
		gpstop -ra || exit 1; \
	fi; \
	if [ "$$TS_AUTO_TUNE_CLUSTER" = "0" ]; then \
		mwp=$$(psql -X -t -A -c "SHOW max_worker_processes" template1 2>/dev/null); \
		if [ -n "$$mwp" ] && [ "$$mwp" -lt 32 ]; then \
			echo "WARNING: max_worker_processes=$$mwp is below 32; BGW-heavy tests may starve.  TS_AUTO_TUNE_CLUSTER=0 suppresses the automatic raise (persistent cluster restart) -- unset it to let installcheck fix this for you." >&2; \
		fi; \
	else \
		mwp=$$(psql -X -t -A -c "SHOW max_worker_processes" template1 2>/dev/null); \
		if [ -n "$$mwp" ] && [ "$$mwp" -lt 32 ]; then \
			echo "max_worker_processes=$$mwp is below 32: raising to 32 (BGW scheduler/job-worker tests need the slots).  Set TS_AUTO_TUNE_CLUSTER=0 to skip this and only get a warning instead."; \
			gpconfig -c max_worker_processes -v 32 --skipvalidation || exit 1; \
			gpstop -ra || exit 1; \
		fi; \
	fi

# Run only regression tests (skip isolation2).  Delegates to the two
# subdir Makefiles that actually own the tests; the top-level REGRESS
# variable is intentionally empty (tests live in subdirs), so the
# previous `$(pg_regress_installcheck) $(REGRESS)` recipe ran zero
# tests and always exited 0 — meaningless.
.PHONY: regresscheck
regresscheck: _ts_ensure_preload _ts_drop_stale_dbs _ts_regress _ts_access

# Run only isolation tests.
.PHONY: isolationcheck
isolationcheck: _ts_ensure_preload _ts_ensure_isolation_deps
	$(pg_isolation_regress_installcheck) $(ISOLATION_OPTS) $(ISOLATION)

# Ensure gpdiff.pl and related perl modules are in the isolation directory.
.PHONY: _ts_ensure_isolation_deps
_ts_ensure_isolation_deps:
	@iso_dir="$(top_srcdir)/src/test/isolation"; \
	reg_dir="$(top_srcdir)/src/test/regress"; \
	for f in gpdiff.pl gpstringsubs.pl atmsort.pm explain.pm GPTest.pm; do \
		if [ ! -f "$$iso_dir/$$f" ] && [ -f "$$reg_dir/$$f" ]; then \
			cp "$$reg_dir/$$f" "$$iso_dir/$$f"; \
		fi; \
	done

# ---------------------------------------------------------------------------
# TAP tests (test/tap/*.pl) — crash recovery / WAL replay for the storage AM.
#
# Cluster was configured WITHOUT --enable-tap-tests, so PGXS' built-in
# `make check` won't invoke prove.  This target packages the environment
# that PostgresNode.pm needs into a single command:
#
#   make tapcheck                   # all test/tap/*.pl
#   make tapcheck PROVE_FLAGS=-v    # verbose
#
# The TAP test spins up its own single-node cluster under tmp_check/, so
# unlike installcheck it does NOT touch the live CB cluster.
# ---------------------------------------------------------------------------
PROVE_FLAGS ?=

.PHONY: tapcheck
tapcheck:
	@rm -rf test/tmp_check
	@mkdir -p test
	TESTDIR="$(srcdir)/test" \
	PG_REGRESS="$(top_srcdir)/src/test/regress/pg_regress" \
	PERL5LIB="$(top_srcdir)/src/test/perl" \
	PATH="$(bindir):$$PATH" \
	LD_LIBRARY_PATH="$(libdir):$$LD_LIBRARY_PATH" \
	prove $(PROVE_FLAGS) test/tap/*.pl

# CI convenience target.
installcheck-ci:
	$(MAKE) installcheck
