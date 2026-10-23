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
PKG_CONFIG ?= pkg-config
GRPCXX_MODULE = grpc++ >= 1.50

ifndef have_grpcxx
have_grpcxx := $(shell $(PKG_CONFIG) --exists '$(GRPCXX_MODULE)' && echo yes || echo no)
endif

ifeq ($(have_grpcxx),yes)
GRPCXX_CFLAGS := $(shell $(PKG_CONFIG) --cflags '$(GRPCXX_MODULE)')
GRPCXX_LIBS := $(shell $(PKG_CONFIG) --libs '$(GRPCXX_MODULE)')
override CPPFLAGS += -DHAVE_GRPCXX
endif
