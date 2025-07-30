export PORT_BASE=${PORT_BASE:-15432}
export NUM_PRIMARY_MIRROR_PAIRS=${NUM_PRIMARY_MIRROR_PAIRS:-3}
export WITH_MIRRORS=${WITH_MIRRORS:-true}
export DATADIRS=${DATADIRS:-$(pwd)/../../contrib/postgres_fdw/cbdb_test_data/datadirs}
