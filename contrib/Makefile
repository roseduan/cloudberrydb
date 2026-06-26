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

SUBDIRS = \
		amcheck		\
		auth_delay	\
		auto_explain	\
		bloom		\
		btree_gin	\
		btree_gist	\
		citext		\
		cloudberry_fdw \
		dblink		\
		dict_int	\
		dict_xsyn	\
		file_fdw	\
		fixeddecimal	\
		fuzzystrmatch	\
		hstore		\
		intagg		\
		intarray	\
		interconnect \
		isn		\
		ltree		\
		oid2name	\
		old_snapshot	\
		pageinspect	\
		passwordcheck	\
		postgres_fdw \
		pg_buffercache	\
		pg_freespacemap \
		pg_prewarm	\
		pg_stat_statements \
		pg_surgery	\
		pg_trgm		\
		pgcrypto	\
		pgrowlocks	\
		pgstattuple	\
		pg_visibility	\
		seg		\
		spi		\
		tablefunc	\
		tcn		\
		test_decoding	\
		tsm_system_rows \
		tsm_system_time \
		unaccent	\
		unionstore_ext  \
		unionstore_walredo \
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
SUBDIRS += sslinfo
else
ALWAYS_SUBDIRS += sslinfo
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

ifeq ($(enable_time_series),yes)
SUBDIRS += time_series
else
ALWAYS_SUBDIRS += time_series
endif

# Missing:
#		start-scripts	\ (does not have a makefile)


$(recurse)
$(recurse_always)
