# Standalone tests for the pieces of the site-config reader that carry no
# PostgreSQL dependency.  No cluster, no extension build, no configure --
# just a compiler -- so the equivalence layer stays checkable anywhere.
#
#     make check

SRCDIR = ../../src/common

CFLAGS ?= -Wall -Wextra -Werror -O1 -g
CPPFLAGS += -I$(SRCDIR)

TESTS = iceberg_site_scalar_test

all: $(TESTS)

iceberg_site_scalar_test: iceberg_site_scalar_test.c $(SRCDIR)/iceberg_site_scalar.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ $^

check: $(TESTS)
	@for t in $(TESTS); do ./$$t || exit 1; done

clean:
	rm -f $(TESTS)

.PHONY: all check clean
