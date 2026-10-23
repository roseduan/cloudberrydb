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

#
# The readiness probe.  Compiled only where grpc++ is found, by the same
# grpcxx.mk as datalake_fdw so the two libraries agree, and after the PGXS
# include for the same reason datalake_fdw's block is.
#
# The probe object is built here rather than shared with datalake_fdw.so
# because the two are separate shared libraries; the source is shared, which
# is what keeps the wire format in one place.
#
include ../datalake_agent/grpcxx.mk

ifeq ($(have_grpcxx),yes)
GRPC_SRC_DIR = ../datalake_fdw/src/components/agent_cli/grpc

#
# Compiled into THIS directory, under names of our own, even though the source
# is datalake_fdw's.  Writing the object next to the source would have two
# makefiles producing the same file: contrib's subdirectories are independent
# prerequisites of the top-level target, so "make -j" runs datalake_fdw and
# datalake_proxy at the same time and both would compile
# .../grpc/grpc_transport.o over each other -- with different CPPFLAGS, so
# whichever finished last decided what both libraries linked.
#
GRPC_PROBE_OBJS = grpc_health_probe.o grpc_transport_probe.o
OBJS += $(GRPC_PROBE_OBJS)
override CPPFLAGS += -I$(GRPC_SRC_DIR) $(GRPCXX_CFLAGS)
SHLIB_LINK += $(GRPCXX_LIBS) -lstdc++

#
# Hidden: datalake_proxy.so is preloaded with RTLD_GLOBAL, so symbols it
# exports would satisfy datalake_fdw.so's references to the same names, and
# datalake_fdw would run this copy of the transport instead of its own.
#
PROBE_VISIBILITY = -fvisibility=hidden -fvisibility-inlines-hidden

grpc_health_probe.o: $(GRPC_SRC_DIR)/grpc_health.cpp
	$(COMPILE.cc) $(PROBE_VISIBILITY) -o $@ $<

grpc_transport_probe.o: $(GRPC_SRC_DIR)/grpc_transport.cpp
	$(COMPILE.cc) $(PROBE_VISIBILITY) -o $@ $<

datalake_proxy.so: $(GRPC_PROBE_OBJS)
all: $(GRPC_PROBE_OBJS)
endif
