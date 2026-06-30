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
    const char* catalog_properties;  /* Catalog config JSON (config + storage-credentials + table-location), NULL if volume specified */
    const char* compression;         /* Parquet write compression codec; NULL on non-AM volume path */
    int         compression_level;   /* Parquet write compression level; -1 = codec default */
} icebergTableInfo;

/* Scan state for Iceberg volume foreign data wrapper */
typedef struct icebergVolumeScanState {
    icebergTableInfo iceTable;      /* Iceberg table information */
    void* volumeState;              /* Volume-specific scan state */
} icebergVolumeScanState;

#endif /* ICEBERG_VOLUME_FDW_H */
