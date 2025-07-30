#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

source "$SCRIPT_DIR/cloudberry_env.sh"

if [ -z "${DATADIRS}" ]; then
if [ -z "${COORDINATOR_DATADIR}" ]; then
	DATADIRS="${SCRIPT_DIR}/cbdb_test_data/datadirs"
else
	DATADIRS="${COORDINATOR_DATADIR}/datadirs"
fi
fi

if [ ! -d "$DATADIRS" ]; then
echo "Creating DATADIRS at $DATADIRS"
mkdir -p "$DATADIRS" || {
	echo "Failed to create directory $DATADIRS"
	exit 1
}
fi

export PGPORT=$PORT_BASE

echo "Using PORT_BASE=$PORT_BASE"
echo "NUM_PRIMARY_MIRROR_PAIRS=$NUM_PRIMARY_MIRROR_PAIRS"
echo "WITH_MIRRORS=$WITH_MIRRORS"
echo "DATADIRS=$DATADIRS"

cd ../../gpAux/gpdemo || {
echo "Failed to cd into ../gpAux/gpdemo"
exit 1
}

export DEMO_PORT_BASE="$PORT_BASE"
export NUM_PRIMARY_MIRROR_PAIRS="$NUM_PRIMARY_MIRROR_PAIRS"
export WITH_MIRRORS="$WITH_MIRRORS"
export DATADIRS="$DATADIRS"

./demo_cluster.sh -d && \
./demo_cluster.sh -c && \
./demo_cluster.sh

if [ $? -ne 0 ]; then
echo "Cluster creation failed."
exit 1
else
echo "Cluster created successfully."
fi

echo "Creating database remotedb..."
psql -h localhost -p 15432 -U gpadmin -d postgres -c "CREATE DATABASE remotedb;" || {
	echo "Failed to create remotedb"
	exit 1
}

echo "Running remote initialization SQL..."
psql -q -h localhost -p 15432 -U gpadmin -d remotedb -f ../../contrib/postgres_fdw/sql/postgres_sql/cloudberry_fdw_insert_init.sql || {
	echo "Failed to run cloudberry_fdw_insert_init.sql"
	exit 1
}
