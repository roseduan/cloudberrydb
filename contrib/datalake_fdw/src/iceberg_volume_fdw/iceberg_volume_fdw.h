/*
 * iceberg_volume_fdw.h
 *      Header file for Iceberg volume foreign data wrapper
 */

#ifndef ICEBERG_VOLUME_FDW_H
#define ICEBERG_VOLUME_FDW_H

/* Iceberg table metadata information */
typedef struct icebergTableInfo {
    const char* icebergNamespace;    /* database schema name */
    const char* tableName;           /* Iceberg table name */
    const char* basePath;            /* Catalog fdw Base path */
    const char* agentCliRespond;     /* Agent CLI response data */
    const char* volumn_name;         /* Volume name for data access */
    const char* volumn_server_name;  /* Volume server name */
    const char* location;            /* pre-formatted data path from AM layer, must be non-NULL */
    /*
     * Storage configuration JSON, NULL when a FOREIGN VOLUME supplies it.
     * Two generations share this field and the segment tells them apart by
     * the "envelope-version" key: the legacy Polaris payload (config +
     * storage-credentials + table-location), and the versioned REST envelope,
     * which a volume-less REST table always carries.
     */
    const char* catalog_properties;
    const char* compression;         /* Parquet write compression codec; NULL on non-AM volume path */
    int         compression_level;   /* Parquet write compression level; -1 = codec default */
    const char* partition_by;        /* Iceberg partition columns (comma-separated); NULL/empty when unpartitioned */
} icebergTableInfo;

/* Scan state for Iceberg volume foreign data wrapper */
typedef struct icebergVolumeScanState {
    icebergTableInfo iceTable;      /* Iceberg table information */
    void* volumeState;              /* Volume-specific scan state */

    /*
     * Time travel: snapshot schema field-ids for the scan tupdesc, copied
     * into dataLakeOptions by iceberg_volume_get_options.  NULL/0 (the
     * palloc0 default) for ordinary scans.  Set only by
     * pg_iceberg_snapshot_beginscan.
     */
    int* snapshot_field_ids;
    int  n_snapshot_field_ids;
} icebergVolumeScanState;

#endif /* ICEBERG_VOLUME_FDW_H */
