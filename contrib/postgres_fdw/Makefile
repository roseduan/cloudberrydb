# contrib/postgres_fdw/Makefile

MODULE_big = postgres_fdw
OBJS_FRONTEND = \
	link-canary.o

OBJS = \
	$(WIN32RES) \
	$(OBJS_FRONTEND) \
	connection.o \
	deparse.o \
	option.o \
	postgres_fdw.o \
	shippable.o

PGFILEDESC = "postgres_fdw - foreign data wrapper for PostgreSQL"

PG_CPPFLAGS = -I$(libpq_srcdir)
# postgres_fdw is a backend module but must use the *frontend* libpq (see the
# comment in postgres_fdw.h).  We therefore statically link libpq and bind its
# symbols internally with -Bsymbolic.  libpq itself depends on src/common
# helpers such as pg_saslprep() (used by SCRAM authentication); if we only pin
# libpq.a, those helpers get resolved at load time against the backend copies,
# which allocate with palloc().  libpq then free()s that memory when closing
# the connection and the backend aborts with heap corruption.  Pull in the
# frontend libpgcommon/libpgport (the _shlib, PIC variants) too so libpq stays
# allocator-consistent with itself; --start-group resolves the mutual
# references between libpq.a and libpgcommon.
SHLIB_LINK_INTERNAL = -Wl,-Bsymbolic -Wl,-Bstatic -Wl,--start-group $(libpq_pgport_shlib) -Wl,--end-group -Wl,-Bdynamic

EXTENSION = postgres_fdw
DATA = postgres_fdw--1.0.sql postgres_fdw--1.0--1.1.sql

REGRESS = gp2pg_postgres_fdw gp_postgres_fdw mpp_postgres_fdw #postgres_fdw
REGRESS_OPTS += --init-file=$(top_srcdir)/src/test/regress/init_file

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq submake-libpgport
subdir = contrib/postgres_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

link-canary.c : % : $(top_srcdir)/src/common/%
	rm -f $@ && $(LN_S) $< .

link-canary.o: link-canary.c
	$(CC) $(CFLAGS) -DFRONTEND $(CPPFLAGS) -c $< -o $@

clean: clean-symlinks

clean-symlinks:
	rm -f link-canary.c

# For postgres_fdw test
export PG_PORT=5432
installcheck: install prep_postgres
clean: clean_postgres
prep_postgres:
	./postgres_setup.bash
clean_postgres:
	./postgres_clean.bash
.PHONY: prep_postgres clean_postgres
