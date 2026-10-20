MODULE_big = vectorization

top_builddir = ../..

REGRESS_DIR = src/test/regress
VECHEADERS = src/include
BACKENDS = src/backend
PAX_DIR = ${top_builddir}/contrib/pax_storage

ARROW = $(BACKENDS)/utils/arrow
PLAN = $(BACKENDS)/optimizer
MISC = $(BACKENDS)/utils/misc
VHEAP = $(BACKENDS)/utils/vheap
HOOK = $(BACKENDS)/hook
EXECUTOR = $(BACKENDS)/vecexecutor
AM = $(BACKENDS)/utils/am
ACCESS = $(BACKENDS)/access
AOCS = $(ACCESS)/vecaocs
COMMON = $(ACCESS)/common
CDB = $(BACKENDS)/cdb
MOTION = $(CDB)/vecmotion
SORT = $(BACKENDS)/utils/sort

SUBDIRS = $(VHEAP) $(ARROW) $(AOCS) $(COMMON) $(AM) $(HOOK) $(PLAN) $(MISC) $(EXECUTOR) $(MOTION) $(SORT) $(CDB)
SRC := $(foreach dir,$(SUBDIRS),$(wildcard $(dir)/*.c))
OBJS = main.o $(patsubst %.c,%.o,$(SRC))

# Avoid building LLVM Bitcode for vectorization module.
with_llvm = no

arrow_dist_dir = $(abspath $(top_builddir)/contrib/apache-arrow/dist)

PG_CFLAGS += $(shell pkg-config --cflags glib-2.0 gobject-2.0) -I$(arrow_dist_dir)/include
PG_LDFLAGS += $(shell pkg-config --libs glib-2.0 gobject-2.0) -L$(arrow_dist_dir)/lib
PG_LDFLAGS += -lgandiva-glib -lgandiva -larrow-glib -larrow -larrow-dataset-glib

PG_CFLAGS += -I$(PAX_DIR)/src/cpp/
PG_CPPFLAGS = -I$(libpq_srcdir) -Iarrow/include  -Wno-unused-result

PG_REGRESS = $(top_builddir)/src/test/regress/pg_regress

PG_CFLAGS += -I$(VECHEADERS)
$(shell python3 build_vec_table.py)
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/vectorization
include $(top_builddir)/src/Makefile.global
include $(top_builddir)/contrib/contrib-global.mk
endif

.PHONY: install-data
install-data:
	$(INSTALL_DATA) vectorization--1.0.sql '$(DESTDIR)$(datadir)/cdb_init.d/vectorization--1.0.sql'

install: install-data

installcheck: vec_test icw_test icw_aocs

.PHONY: uninstall-data
uninstall-data:
	$(RM) '$(DESTDIR)$(datadir)/cdb_init.d/vectorization--1.0.sql'

uninstall: uninstall-data

.PHONY: vec_test
vec_test:
	mkdir -p data/tpch
	wget https://artifactory.hashdata.xyz/artifactory/list/utility/tpch_data.tar
	tar xf tpch_data.tar -C data/tpch
	rm -f tpch_data.tar
	$(PG_REGRESS) --init-file=init_file --schedule=vectorization_schedule

.PHONY: icw_test
icw_test:
	make -C $(REGRESS_DIR) icw_test

.PHONY: icw_aocs
icw_aocs:
	make -C $(REGRESS_DIR) icw_aocs
