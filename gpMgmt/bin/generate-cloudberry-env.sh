#!/usr/bin/env bash

cat <<"EOF"
if test -n "${ZSH_VERSION:-}"; then
    # zsh
    SCRIPT_PATH="${(%):-%x}"
elif test -n "${BASH_VERSION:-}"; then
    # bash
    SCRIPT_PATH="${BASH_SOURCE[0]}"
else
    # Unknown shell, hope below works.
    # Tested with dash
    result=$(lsof -p $$ -Fn | tail --lines=1 | xargs --max-args=2 | cut --delimiter=' ' --fields=2)
    SCRIPT_PATH=${result#n}
fi

if test -z "$SCRIPT_PATH"; then
    echo "The shell cannot be identified. \$GPHOME may not be set correctly." >&2
fi

# downstream PATH / PYTHONPATH / LD_LIBRARY_PATH derivations stay valid
# even when GPHOME is reached via a symlink whose target is a relative
# path (e.g. /opt/database -> database-2.1.0).  `pwd -P` resolves every
# symlink component, returning the physical absolute path.  This replaces
# an earlier branch that ran bare `readlink` on the script's directory,
# which would return the symlink target verbatim and break with relative
# targets (PYTHONPATH became relative, gppylib import failed).
GPHOME="$(cd "$(dirname "${SCRIPT_PATH}")" >/dev/null 2>&1 && pwd -P)"
EOF

cat <<"EOF"
PYTHONPATH="${GPHOME}/lib/python"
PATH="${GPHOME}/bin:${PATH}"
LD_LIBRARY_PATH="${GPHOME}/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

if [ -e "${GPHOME}/etc/openssl.cnf" ]; then
	OPENSSL_CONF="${GPHOME}/etc/openssl.cnf"
fi
if [ -d "${GPHOME}/lib/ossl-modules/" ]; then
	export OPENSSL_MODULES=${GPHOME}/lib/ossl-modules/
fi

#setup JAVA_HOME
if [ -x "${GPHOME}/ext/jdk/bin/java" ]; then
    JAVA_HOME="${GPHOME}/ext/jdk"
    PATH="${JAVA_HOME}/bin:${PATH}"
    CLASSPATH=${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tool.jar
    LD_LIBRARY_PATH=${JAVA_HOME}/jre/lib/amd64/server/:${JAVA_HOME}/jre/lib/aarch64/server/:${LD_LIBRARY_PATH}
fi

#setup perl
if [ -x "${GPHOME}/ext/perl/bin/perl" ]; then
    PATH="${GPHOME}/ext/perl/bin:${PATH}"
fi

#setup python
if [ -x "${GPHOME}/ext/python/bin/python3" ]; then
    PATH="${GPHOME}/ext/python/bin:${PATH}"
    LD_LIBRARY_PATH="${GPHOME}/ext/python/lib:${LD_LIBRARY_PATH}"
fi

#setup R
if [ -x "${GPHOME}/ext/R/bin/R" ]; then
    export R_HOME="${GPHOME}/ext/R"
    LD_LIBRARY_PATH="${GPHOME}/ext/R/lib:${LD_LIBRARY_PATH}"
fi


export GPHOME
export PATH
export PYTHONPATH
export LD_LIBRARY_PATH
export OPENSSL_CONF
export JAVA_HOME
export CLASSPATH

# Load the external environment variable files
if [ -d "${GPHOME}/etc/environment.d" ]; then
	LOGGER=$(which logger 2> /dev/null || which true)
	set -o allexport
	for env in $(find "${GPHOME}/etc/environment.d" -regextype sed -regex '.*\/[0-9][0-9]-.*\.conf$' -type f | sort -n); do
		$LOGGER -t "greenplum-path.sh" "loading environment from ${env}"
		source "${env}"
	done
	set +o allexport
fi
EOF
