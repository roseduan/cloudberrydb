#
# grpc++ for the datalake agent's gRPC transport.
#
# Found here, by the datalake modules, rather than by the server's configure:
# nothing in the server core uses gRPC, so neither configure, pg_config.h nor
# Makefile.global should know about it.  datalake_fdw and datalake_proxy both
# include this file, so the two libraries always agree on whether the
# transport is built.
#
# Optional on purpose: without grpc++ >= 1.50 the modules still build and
# speak REST only, and datalake.agent_transport refuses the value 'grpc' with
# an error that says so.  The product build images carry grpc++ 1.59.2 already
# (Arrow's OpenTelemetry exporter links it), so this adds no dependency there.
#
# "make have_grpcxx=no" builds the REST-only variant on a machine that has
# grpc++.
#
# Include after contrib-global.mk: the flags are appended to CPPFLAGS, which
# PGXS assembles at include time.
#
# Only the pkg-config queries that read grpc++.pc alone.  --exists, --cflags
# and --libs walk grpc++'s whole Requires graph, about 180 abseil packages
# with heavy diamond sharing, and pkg-config 0.27 (CentOS 7) walks it without
# merging shared nodes: each of those calls ran for minutes, three times per
# make in each of the two modules.  --atleast-version and --variable return
# at once.  grpc++, grpc and gpr are shared libraries that carry abseil
# inside, and the transport only includes grpcpp/, so the three -l are all
# the link needs.
#
PKG_CONFIG ?= pkg-config
GRPCXX_MIN_VERSION = 1.50

ifndef have_grpcxx
have_grpcxx := $(shell $(PKG_CONFIG) --atleast-version=$(GRPCXX_MIN_VERSION) grpc++ && echo yes || echo no)
endif

ifeq ($(have_grpcxx),yes)
GRPCXX_CFLAGS := -I$(shell $(PKG_CONFIG) --variable=includedir grpc++)
GRPCXX_LIBS := -L$(shell $(PKG_CONFIG) --variable=libdir grpc++) -lgrpc++ -lgrpc -lgpr
override CPPFLAGS += -DHAVE_GRPCXX
endif
