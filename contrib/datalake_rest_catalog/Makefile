top_builddir = ../..
include $(top_builddir)/src/Makefile.global

MVN_EXISTS := $(shell command -v mvn 2> /dev/null)

# HikariCP 5.1.0 (a runtime dependency) is compiled for Java 11 (class file version 55.0) and
# the gateway sources use Java 11 APIs, so mvn must run under a JDK >= 11. Build images commonly
# default `java` to JDK 8. JAVA_HOME is resolved at build time in this order:
#   1. DRC_BUILD_JAVA_HOME, if exported, is used verbatim (escape hatch).
#   2. an existing JDK >= 11 auto-detected under common locations (preference order below).
#   3. otherwise, on yum-based images, java-11-openjdk-devel is installed on the fly. The CI
#      devel image ships only JDK 8, but its internal mirror carries java-11-openjdk-devel.
# Candidate JDK homes, most-preferred first (globs expanded by the shell at build time; literal
# non-matches simply fail the javac test and are skipped).
DRC_JDK_GLOBS = /opt/jdk17 /opt/jdk11 /usr/local/jdk17 /usr/local/jdk11 /usr/lib/jvm/java-17-openjdk* /usr/lib/jvm/java-11-openjdk* /usr/lib/jvm/*17* /usr/lib/jvm/*11*

# Shell snippet (single logical line via backslashes): leaves a usable JDK>=11 home in $$jh, or
# empty if none could be found or installed. Kept side-effect-tolerant: a failed yum install just
# warns and lets the caller fall back to the default java.
DRC_RESOLVE_JH = jh="$(DRC_BUILD_JAVA_HOME)"; \
	if [ -z "$$jh" ]; then for d in $(DRC_JDK_GLOBS); do [ -x "$$d/bin/javac" ] && jh="$$d" && break; done; fi; \
	if [ -z "$$jh" ] && command -v yum >/dev/null 2>&1; then \
		echo "datalake_rest_catalog: no JDK>=11 found; installing java-11-openjdk-devel via yum"; \
		{ sudo -n yum install -y java-11-openjdk-devel || yum install -y java-11-openjdk-devel; } || \
			echo "datalake_rest_catalog: WARNING java-11 install failed; will try default java"; \
		for d in $(DRC_JDK_GLOBS); do [ -x "$$d/bin/javac" ] && jh="$$d" && break; done; \
	fi

all:
ifdef MVN_EXISTS
	@$(DRC_RESOLVE_JH); \
	if [ -n "$$jh" ]; then \
		echo "datalake_rest_catalog: building with JAVA_HOME=$$jh"; \
		export JAVA_HOME="$$jh"; export PATH="$$jh/bin:$$PATH"; \
	else \
		echo "datalake_rest_catalog: WARNING building with default java ($$(java -version 2>&1 | head -n1))"; \
	fi; \
	mvn package -DskipTests
else
	@echo "WARNING: mvn not found — skipping datalake_rest_catalog build (install JDK/mvn or use --enable-datalake-rest-catalog=yes explicitly)"
endif

test:
	@$(DRC_RESOLVE_JH); \
	if [ -n "$$jh" ]; then export JAVA_HOME="$$jh"; export PATH="$$jh/bin:$$PATH"; fi; \
	mvn test

clean:
ifdef MVN_EXISTS
	mvn clean
endif

install: all
ifdef MVN_EXISTS
	$(MKDIR_P) '$(DESTDIR)$(pkglibdir)/java/'
	$(INSTALL_DATA) 'target/datalake-rest-catalog-1.0.0.jar' '$(DESTDIR)$(pkglibdir)/java/'
endif
	$(MKDIR_P) '$(DESTDIR)$(bindir)'
	$(INSTALL_SCRIPT) 'bin/datalake_rest_catalog_ctl.sh' '$(DESTDIR)$(bindir)/'

uninstall:
	rm -f '$(DESTDIR)$(pkglibdir)/java/datalake-rest-catalog-1.0.0.jar'
	rm -f '$(DESTDIR)$(bindir)/datalake_rest_catalog_ctl.sh'

distclean: clean
