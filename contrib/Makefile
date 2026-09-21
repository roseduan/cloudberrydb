# contrib/Makefile

subdir = contrib
top_builddir = ..
include $(top_builddir)/src/Makefile.global

# Apache Cloudberry specific changes to the targetlist:
# tsearch2 is currently disabled due to test failures.
# cube and the related earthdistance are disabled since GPDB define CUBE
# as a keyword. lo is disabled since large objects aren't supported.
# adminpack is disabled since the functionality has been imported into
# GPDB.

# FIXME: installcheck for extension pageinspect, pg_visibility, postgres_fdw,
# seg, tsm_system_rows, tsm_system_time, xml2 is failed, which is also failed
# in gpdb. We'll fix it if we need these plugins to work properly.

# FIXME: the following are left out of SUBDIRS because they don't build yet
# after the PG16 merge -- each needs real porting work, tracked separately
# from the merge-conflict cleanup that restored the rest of this list:
#   cloudberry_fdw     - postgres_fdw.c uses many removed/renamed PG16 APIs
#                         (Value, RangeTblEntry.checkAsUser/updatedCols, etc.)
#   pg_stat_statements  - unresolved merge conflict in pg_stat_statements.c
#   pg_prewarm          - autoprewarm.c uses pre-PG16 BlockInfoRecord fields
#   pg_surgery          - heap_surgery.c: implicit pg_class_ownercheck decl
#   pgstattuple         - pgstatapprox.c has an unresolved merge conflict
#   pg_visibility       - pg_visibility.c has an unresolved merge conflict
#   unionstore_ext,
#   unionstore_walredo  - use pre-PG16 BufferTag/SMgrRelationData fields
#                         (rnode/smgr_rnode), need a RelFileLocator port
#   time_series         - src/gapfill_plan.c uses the removed PG16 `Value` node

SUBDIRS = \
		amcheck		\
		auth_delay	\
		auto_explain	\
		basic_archive	\
		basebackup_to_shell	\
		bloom		\
		btree_gin	\
		btree_gist	\
		citext		\
		dblink		\
		dict_int	\
		dict_xsyn	\
		file_fdw	\
		fixeddecimal	\
		fuzzystrmatch	\
		hstore		\
		intagg		\
		intarray	\
		interconnect	\
		isn		\
		ltree		\
		oid2name	\
		old_snapshot	\
		pageinspect	\
		passwordcheck	\
		postgres_fdw \
		pg_buffercache	\
		pg_freespacemap \
		pg_trgm		\
		pgrowlocks	\
		pg_walinspect	\
		seg		\
		spi		\
		tablefunc	\
		tcn		\
		test_decoding	\
		tsm_system_rows \
		tsm_system_time \
		unaccent	\
		vacuumlo

# Cloudberry-specific additions (to ease merge pain).
SUBDIRS += \
		formatter \
		formatter_fixedwidth \
		extprotocol \
		indexscan  \
		file_content \
		pg_tde

ifeq ($(with_ssl),openssl)
SUBDIRS += pgcrypto sslinfo
else
ALWAYS_SUBDIRS += pgcrypto sslinfo
endif

ifneq ($(with_uuid),no)
SUBDIRS += uuid-ossp
else
ALWAYS_SUBDIRS += uuid-ossp
endif

ifeq ($(with_libxml),yes)
SUBDIRS += xml2
else
ALWAYS_SUBDIRS += xml2
endif

ifeq ($(with_selinux),yes)
SUBDIRS += sepgsql
else
ALWAYS_SUBDIRS += sepgsql
endif

ifeq ($(with_perl),yes)
SUBDIRS += bool_plperl hstore_plperl jsonb_plperl
else
ALWAYS_SUBDIRS += bool_plperl hstore_plperl jsonb_plperl
endif

ifeq ($(with_python),yes)
SUBDIRS += hstore_plpython jsonb_plpython ltree_plpython
else
ALWAYS_SUBDIRS += hstore_plpython jsonb_plpython ltree_plpython
endif

ifeq ($(enable_vectorization),yes)
SUBDIRS += vectorization
else
ALWAYS_SUBDIRS += vectorization
endif

ifeq ($(enable_pax),yes)
SUBDIRS += pax_storage
else
ALWAYS_SUBDIRS += pax_storage
endif

ifeq ($(enable_gophermeta),yes)
SUBDIRS += pg_gophermeta
else
ALWAYS_SUBDIRS += pg_gophermeta
endif

ifeq ($(enable_dfs_tablespace),yes)
SUBDIRS += dfs-tablespace-ext
else
ALWAYS_SUBDIRS += dfs-tablespace-ext
endif

ifeq ($(enable_datalake),yes)
SUBDIRS += datalake_fdw hive_connector datalake_proxy datalake_agent datalake_apiary
else
ALWAYS_SUBDIRS += datalake_fdw hive_connector datalake_proxy datalake_agent datalake_apiary
endif

ifeq ($(enable_datalake_rest_catalog),yes)
SUBDIRS += datalake_rest_catalog
else
ALWAYS_SUBDIRS += datalake_rest_catalog
endif

ifeq ($(enable_perfmon), yes)
SUBDIRS += perfmon
else
ALWAYS_SUBDIRS += perfmon
endif

ifeq ($(enable_ic_udp2),yes)
SUBDIRS += udp2
else
ALWAYS_SUBDIRS += udp2
endif

ifeq ($(enable_vbf),yes)
SUBDIRS += vbf
else
ALWAYS_SUBDIRS += vbf
endif

# time_series is temporarily left out of SUBDIRS/ALWAYS_SUBDIRS -- see FIXME
# above; it doesn't build yet after the PG16 merge (removed `Value` node).

# Missing:
#		start-scripts	\ (does not have a makefile)


$(recurse)
$(recurse_always)

# time_series links against pax_storage's libpaxformat.so; order the
# recursion under `make -j` so it's built first (same pattern as
# src/interfaces/Makefile's ecpg/libpq dependency).
ifeq ($(enable_pax)+$(enable_time_series),yes+yes)
all-time_series-recurse: all-pax_storage-recurse
install-time_series-recurse: install-pax_storage-recurse
endif
