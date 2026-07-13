top_builddir = ../..
include $(top_builddir)/src/Makefile.global

MVN_EXISTS := $(shell command -v mvn 2> /dev/null)

all:
ifdef MVN_EXISTS
	mvn package -DskipTests
else
	@echo "WARNING: mvn not found — skipping datalake_rest_catalog build (install JDK/mvn or use --enable-datalake-rest-catalog=yes explicitly)"
endif

test:
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
