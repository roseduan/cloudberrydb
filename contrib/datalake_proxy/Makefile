# gpcontrib/datalake_proxy/Makefile

MODULE_big = datalake_proxy
OBJS = \
	$(WIN32RES) \
	datalake_proxy.o

# Avoid building LLVM Bitcode for datalake_proxy module.
with_llvm = no

EXTENSION = datalake_proxy

PGFILEDESC = "datalake_proxy - Datalake Proxy"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/datalake_proxy
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Match datalake_fdw's storage-backend selection.  The open-source build
# (configure --without-gopher => with_gopher=no) links no libgopher, so the
# dlagent this proxy launches must not select GopherFileIO.  Define USE_GOPHER
# only for the commercial (gopher) build; the proxy uses it to decide whether
# to disable gopher on the agent side.
ifneq ($(with_gopher),no)
override CPPFLAGS += -DUSE_GOPHER
endif