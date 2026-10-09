top_builddir = ../..
include $(top_builddir)/src/Makefile.global

MVN_EXISTS := $(shell command -v mvn 2> /dev/null)

# Extra maven flags, e.g. `make test MVN_FLAGS="-o -nsu"` to build against
# whatever snapshots the local repository already holds. Useful while the pom
# points at a hand-published SNAPSHOT, since resolving one afresh reaches the
# internal Nexus.
MVN_FLAGS ?=

all:
	mvn $(MVN_FLAGS) package -DskipTests

# Everything that needs no infrastructure: the unit tests plus the controller
# slice tests.  The end-to-end classes are in the same run but assume their way
# out (see AgentAvailability) when no agent or object store is up, so this is
# safe to wire into CI and safe to run on a laptop.
#
# Deliberately not guarded on MVN_EXISTS the way clean is: a test target that
# quietly succeeds because the toolchain is missing is worse than one that
# fails, and this suite already spent months green without asserting anything.
test:
	mvn $(MVN_FLAGS) test

# The end-to-end pass: brings up an agent with a mock parent and drives it over
# HTTP.  Still needs the compose stack (hive metastore, object store, gopher
# store) that the request fixtures point at; without it the guarded tests skip
# and this reports success having run almost nothing.
test-e2e:
	cd test && ./make-test.sh

clean:
ifdef MVN_EXISTS
	mvn clean
endif

install: all
	$(MKDIR_P) $(pkglibdir)/java/
	$(INSTALL_DATA) 'target/dlagent-1.0.0.jar' $(pkglibdir)/java/

uninstall:
	rm -f $(pkglibdir)/java/dlagent-1.0.0.jar

distclean: clean

.PHONY: all test test-e2e clean install uninstall distclean
