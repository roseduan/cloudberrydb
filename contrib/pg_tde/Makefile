# contrib/pg_tde/Makefile

MODULE_big = pg_tde
OBJS = \
	$(WIN32RES) \
	pg_tde.o

EXTENSION = pg_tde
DATA = pg_tde--1.0.sql

PGFILEDESC = "pg_tde - tablespace transparent data encryption management"

REGRESS = pg_tde

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_tde
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
