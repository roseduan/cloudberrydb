# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# contrib/pax_storage/Makefile

MODULE_big = pax
OBJS = \
	$(WIN32RES)
PG_CPPFLAGS = -I/usr/local/include
PG_CXXFLAGS = -std=c++17

PGFILEDESC = "pax - PAX table access method"
SHLIB_LINK += -luuid

PAX_REGRESS_DIR = src/test/regress
PAX_VEC_REGRESS_DIR = src/test/vec
PAX_ISOLATION2_DIR = src/test/isolation2

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
PGXS_DIR := $(dir $(PGXS))
PG_REGRESS = $(PGXS_DIR)/../../src/test/regress/pg_regress
else
subdir = contrib/pax_storage
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
PG_REGRESS = $(top_builddir)/src/test/regress/pg_regress
endif

.PHONY: all
all: build

DIRS = $(shell find src/cpp -maxdepth 2 -type d)
SRC = $(foreach dir, $(DIRS), $(wildcard $(dir)/*.cc))
CSRC = $(foreach dir, $(DIRS), $(wildcard $(dir)/*.c))

# read options from ENV
USE_MANIFEST_API := $(shell echo $$USE_MANIFEST_API)
USE_PAX_CATALOG := $(shell echo $$USE_PAX_CATALOG)

# if the ENVs are not set, set default value
ifeq ($(USE_MANIFEST_API),)
    USE_MANIFEST_API := ON
endif

ifeq ($(USE_PAX_CATALOG),)
    USE_PAX_CATALOG := OFF
endif

arrow_dist_dir = $(abspath $(top_builddir)/contrib/apache-arrow/dist)

.PHONY: install-data build
build: $(SRC) $(CSRC)
	@echo "build pax, USE_MANIFEST_API=$(USE_MANIFEST_API) USE_PAX_CATALOG=$(USE_PAX_CATALOG)"
	@if [ ! -f build/Makefile ]; then \
		mkdir -p build && \
		cd build && \
		cmake -DCMAKE_INSTALL_PREFIX=$(DESTDIR)$(prefix) \
			-DCMAKE_PREFIX_PATH="$(with_thirdparty);$(arrow_dist_dir)" \
			-DUSE_MANIFEST_API=$(USE_MANIFEST_API)   \
			-DOPENSSL_ROOT_DIR=/usr/local            \
			-DOPENSSL_USE_STATIC_LIBS=ON             \
			-DSNAPPY_HOME=/usr/local                 \
			-DZSTD_HOME=/usr/local                   \
			-DZLIB_HOME=/usr/local                   \
			-DLZ4_HOME=/usr/local                    \
			-DUSE_PAX_CATALOG=$(USE_PAX_CATALOG) .. ;\
	fi
	cd build && $(MAKE) VERBOSE=1
	@cp -f build/src/cpp/libpax.so pax.so

install-data: build
	@echo "install data"
	$(INSTALL_DATA) pax-cdbinit--1.0.sql '$(DESTDIR)$(datadir)/cdb_init.d/pax-cdbinit--1.0.sql'

.PHONY: install
install: install-data
	$(INSTALL_SHLIB) pax.so '$(DESTDIR)$(pkglibdir)/pax.so'
	@if [ -f build/src/cpp/pax_dump ]; then $(INSTALL_PROGRAM) build/src/cpp/pax_dump '$(DESTDIR)$(bindir)/pax_dump'; fi;
	make -j10 -C build install

.PHONY: pax-test
#
# The pax_test database is deliberately left behind: toast.sql ends with a
# relation whose payload sits in an external toast file, and pg_upgrade's
# test (src/bin/pg_upgrade/test_gpdb.sh) upgrades whatever databases the ICW
# left in the cluster.  Dropping the database here would hide the only PAX
# external-toast relation from the upgrade.  pg_regress drops and recreates
# the database at the start of each run, so nothing accumulates.
pax-test: 
	@echo "Begin running pax tests"
	@echo "Run pax-test with vector.enable_vectorization off"
	PGOPTIONS="$(PGOPTIONS) -c pax.enable_prefetch=on -c vector.enable_vectorization=off" \
	$(PG_REGRESS) \
		--init-file=init_file --schedule=pax_schedule \
		--load-extension=gp_inject_fault --dbname=pax_test
ifeq ($(enable_vectorization),yes)
	@echo "Run pax-test with vector.enable_vectorization on"
	PGOPTIONS="$(PGOPTIONS) -c pax.enable_prefetch=on -c vector.enable_vectorization=on" \
	$(PG_REGRESS) \
		--init-file=init_file --schedule=pax_schedule \
		--load-extension=gp_inject_fault --dbname=pax_test
endif

.PHONY: icw_test
icw_test: pax-test pax_vec_test regress_test isolation2_test

.PHONY: pax_vec_test
pax_vec_test:
ifeq ($(enable_vectorization),yes)
	@echo "Begin running pax+vector regression tests"
	@echo "Current PGOPTIONS=$(PGOPTIONS) -c vector.enable_vectorization=on"
	PGOPTIONS="$(PGOPTIONS) -c pax.enable_prefetch=on -c vector.enable_vectorization=on" make -C $(PAX_VEC_REGRESS_DIR) icw_test
	@echo "Begin running pax+vec format+vector regression tests"
	@echo "Current PGOPTIONS=$(PGOPTIONS) -c vector.enable_vectorization=on -c pax.default_storage_format=porc_vec"
	PGOPTIONS="$(PGOPTIONS) -c pax.enable_prefetch=on -c vector.enable_vectorization=on -c pax.default_storage_format=porc_vec" make -C $(PAX_VEC_REGRESS_DIR) icw_test
endif

.PHONY: regress_test isolation2_test
regress_test:
	@echo "Begin running pax regression tests"
	PGOPTIONS="$(PGOPTIONS) -c pax.enable_prefetch=on" make -C $(PAX_REGRESS_DIR) regress_link
	@echo "Current PGOPTIONS=$(PGOPTIONS)"
	PGOPTIONS="$(PGOPTIONS) -c pax.enable_prefetch=on" make -C $(PAX_REGRESS_DIR) icw_test

isolation2_test:
	@echo "Begin running pax isolation2 tests"
	PGOPTIONS="$(PGOPTIONS) -c pax.enable_prefetch=on" make -C $(PAX_REGRESS_DIR) regress_link
	@echo "Current PGOPTIONS=$(PGOPTIONS)"
	PGOPTIONS="$(PGOPTIONS) -c pax.enable_prefetch=on" make -C $(PAX_ISOLATION2_DIR) installcheck

.PHONY: uninstall-data
uninstall-data:
	$(RM) '$(DESTDIR)$(datadir)/cdb_init.d/pax-cdbinit--1.0.sql'

uninstall: uninstall-data
clean-data:
	$(RM) pax-cdbinit--1.0.sql
	$(RM) -r build

clean_isolation2_test:
	make -C $(PAX_ISOLATION2_DIR) clean

clean: clean-data clean_isolation2_test
