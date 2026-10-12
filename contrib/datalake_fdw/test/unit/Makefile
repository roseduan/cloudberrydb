# Standalone tests for the pieces of the site-config reader that carry no
# PostgreSQL dependency.  No cluster, no extension build, no configure --
# just a compiler -- so the equivalence layer stays checkable anywhere.
#
#     make check

SRCDIR = ../../src/common

CFLAGS ?= -Wall -Wextra -Werror -O1 -g
CPPFLAGS += -I$(SRCDIR)

# The column-statistics collector reads a parquet footer, so its test needs
# the parquet library and the base64 encoder -- but still no PostgreSQL.
# THIRDPARTY defaults to the umbrella's prebuilt dependency tree; override it
# for a tree laid out differently.
THIRDPARTY ?= ../../../../../dist/thirdparty
STATSDIR = ../../src/provider/parquet/write
PGCOMMON = ../../../../src/common
CXXFLAGS ?= -Wall -Wextra -Werror -O1 -g -std=c++17

TESTS = iceberg_site_scalar_test
PARQUET_TESTS = iceberg_column_stats_test

all: $(TESTS)

iceberg_site_scalar_test: iceberg_site_scalar_test.c $(SRCDIR)/iceberg_site_scalar.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ $^

# base64.c is PostgreSQL's, shared with the frontend; -DFRONTEND is what lets
# it build without the backend's Assert machinery.  It must be compiled as C
# so the collector's extern "C" declaration finds it.
base64_test.o: $(PGCOMMON)/base64.c
	$(CC) $(CFLAGS) -DFRONTEND -I../../../../src/include -c -o $@ $<

iceberg_column_stats_test: iceberg_column_stats_test.cpp \
		$(STATSDIR)/parquetColumnStats.cpp base64_test.o
	$(CXX) $(CXXFLAGS) -I$(STATSDIR) -I../../../../src/include \
		-I$(THIRDPARTY)/include -o $@ $^ \
		-L$(THIRDPARTY)/lib -lparquet -Wl,-rpath,$(abspath $(THIRDPARTY)/lib)

check: $(TESTS)
	@for t in $(TESTS); do ./$$t || exit 1; done

# Separate because it needs the parquet library present; run it wherever the
# thirdparty tree is built.
check-parquet: $(PARQUET_TESTS)
	@for t in $(PARQUET_TESTS); do ./$$t || exit 1; done

clean:
	rm -f $(TESTS) $(PARQUET_TESTS) base64_test.o

.PHONY: all check check-parquet clean
