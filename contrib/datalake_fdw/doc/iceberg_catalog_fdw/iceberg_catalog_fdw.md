# Iceberg Catalog FDW Documentation

## Overview

The Iceberg Catalog FDW (Foreign Data Wrapper) provides integration with Apache Iceberg table format through catalog and volume management. It enables creating, managing, and accessing Iceberg tables using PostgreSQL/Greenplum foreign data wrapper infrastructure.

## Architecture

The Iceberg Catalog FDW consists of two main components:

1. **Catalog Server**: Manages table metadata through various catalog implementations (Hive Metastore, etc.)
2. **Volume Server**: Handles data storage operations on different storage systems (S3, HDFS, etc.)

## Installation

```sql
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
```

## Schema and Functions

### Iceberg Toolkit Schema

The extension creates an `iceberg_toolkit` schema containing utility functions:

```sql
CREATE SCHEMA iceberg_toolkit;
```

### Main Function: catalog_fdw

```sql
CREATE FUNCTION iceberg_toolkit.catalog_fdw(
    operation text,           -- Operation type: 'create_table', 'append', 'load'
    name_space text,         -- Target namespace/database
    table_name text,         -- Table name
    schema_json text,        -- Iceberg schema definition in JSON format
    catalog_server_name text, -- Foreign catalog server name
    catalog_table_name text,  -- Catalog table identifier
    volume_server_name text,  -- Foreign volume server name
    volume_table_name text    -- Volume table identifier
)
RETURNS text;
```

## Configuration

### 1. Catalog Server Setup

Create a foreign server for catalog operations:

```sql
CREATE SERVER example_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (
    server_type 'hive',
    hive_metastore_uri 'thrift://hive-metastore:9083'
);
```

A catalog that speaks the Iceberg REST protocol is configured on the same
server object. Its tables carry no FOREIGN VOLUME — the catalog assigns each
table a location and vends credentials for it — so the storage endpoint that
a volume would otherwise supply is given here instead:

```sql
CREATE SERVER xsky_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (
    type 'rest',
    url 'http://s3tables.example.com:8060/iceberg',
    endpoint 'http://s3tables.example.com:8060',
    region 'us-east-1',
    signing_name 's3tables',
    path_style_access 'true'
);
```

#### Catalog Server Options

| Option | Description | Example |
|--------|-------------|---------|
| `type` | Type of catalog server: `hive`, `rest`, `polaris` or `hadoop`. Also spelled `server_type`; when both are present `type` wins. | `'rest'` |
| `hive_metastore_uri` | Hive Metastore connection URI (`hive` only) | `'thrift://hive-metastore:9083'` |
| `url` | Base URL of the REST catalog, including its `/iceberg`-style path prefix (`rest`, `polaris`) | `'http://host:8060/iceberg'` |
| `endpoint` | Object-storage endpoint the segments read and write through. Required for a REST catalog, which has no FOREIGN VOLUME to take it from. | `'http://host:8060'` |
| `region` | Region for the storage endpoint, and the SigV4 signing region | `'us-east-1'` |
| `signing_name` | SigV4 service name. Platform-specific and **not** `s3` for an S3-Tables-style catalog — signing with the wrong name is accepted by the catalog but yields credentials that do not work. | `'s3tables'` |
| `path_style_access` | Address buckets as a path rather than a subdomain | `'true'` |
| `vended_credentials` | Ask the catalog to vend per-table storage credentials. Unset means on. Set `off` to use the static keys on the user mapping instead — weaker isolation, so it is an explicit opt-out and never an automatic fallback. Settable here or on the user mapping; the user mapping wins. | `'off'` |
| `polaris_server_realm` | Value of the `Polaris-Realm` header, for a multi-tenant Polaris deployment. Lets `type 'rest'` reach a Polaris catalog. | `'TENANT-A'` |
| `maintenance_api` | Declares that the platform offers a maintenance API to which VACUUM can be delegated. The only accepted value is `s3tables`; without it, VACUUM on a REST table is refused rather than silently doing nothing. | `'s3tables'` |

### 2. User Mapping for Catalog

```sql
CREATE USER MAPPING FOR current_user
SERVER example_catalog_server
OPTIONS (
    username 'hive_user',
    auth_method 'simple'
);
```

For a REST catalog, the user mapping carries the credentials used to
**authenticate to the catalog itself**. These are not the credentials the
segments use to read data — those are vended per table by the catalog, unless
`vended_credentials 'off'` says otherwise, in which case `access_key_id` /
`secret_access_key` serve both purposes:

```sql
-- SigV4 (XSKY S3 Tables, AWS S3 Tables)
CREATE USER MAPPING FOR current_user
SERVER xsky_catalog_server
OPTIONS (
    auth_method 'sigv4',
    access_key_id 'AK...',
    secret_access_key 'SK...'
);

-- OAuth2 (Polaris)
CREATE USER MAPPING FOR current_user
SERVER polaris_catalog_server
OPTIONS (
    auth_method 'oauth2',
    client_id 'root',
    client_secret 's3cr3t',
    scope 'PRINCIPAL_ROLE:ALL'
);
```

#### Catalog User Mapping Options

| Option | Description | Example |
|--------|-------------|---------|
| `auth_method` | How to authenticate to the catalog: `sigv4`, `oauth2`, `none`, or `simple`/Kerberos for Hive | `'sigv4'` |
| `access_key_id`, `secret_access_key` | Static keys. Required by `sigv4`; also the data-plane credentials when `vended_credentials` is `off`. | `'AK...'` |
| `client_id`, `client_secret`, `scope` | OAuth2 client credentials (`oauth2`) | `'root'` |
| `vended_credentials` | Same switch as on the server, and takes precedence over it. `off` without static keys is rejected at resolution time rather than falling back silently. | `'off'` |
| `username` | Hive user name (`simple`) | `'hive_user'` |
| `krb_service_principal`, `krb_client_principal`, `krb_client_keytab` | Kerberos identity for a Hive metastore | - |

### 3. Foreign Catalog Creation

```sql
CREATE FOREIGN CATALOG hive_production
SERVER example_catalog_server
OPTIONS (
    catalog_name 'hive_location',
    default_namespace 'default',
    metadata_cache '5',
    warehouse_location_prefix 's3a://warehouse/hive_location/'
);
```

#### Foreign Catalog Options

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `catalog_name` | Catalog identifier | - | `'hive_location'` |
| `default_namespace` | Default namespace | `'default'` | `'default'` |
| `metadata_cache` | How long (in **minutes**) a resolved metadata head may be reused before the external catalog is asked again; the catalog-level default for the per-table option of the same name. `0` or unset = no caching, every read fetches the latest head. It only delays visibility of commits made by *other* engines; this cluster's own writes are always visible immediately. It does not cache file contents (manifests, metadata.json) — those are cached by gopher, independently of this option. | `'0'` | `'5'` |
| `prefix` | Path segment the REST catalog expects between `/v1/` and `namespaces` (`rest`). Defaults to `catalog_name`. | `catalog_name` | `'ice-bucket'` |
| `warehouse_location_prefix` | Base warehouse path | - | `'s3a://warehouse/hive_location/'` |

> **Deprecated:** `enable_metadata_cache`, `metadata_cache_ttl` (seconds) and
> `auto_refresh_metadata` are no longer recognized (they had never taken effect
> in any released version). A DDL that still carries them succeeds with the
> options ignored. Migrate `metadata_cache_ttl '300'` to `metadata_cache '5'`
> (minutes).

### 4. Volume Server Setup

Create a foreign server for data storage operations:

```sql
CREATE SERVER s3_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    server_type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
```

#### Volume Server Options

| Option | Description | Example |
|--------|-------------|---------|
| `server_type` | Storage system type | `'s3'` |
| `endpoint` | Storage endpoint URL | `'http://minio:9000'` |
| `region` | AWS region | `'us-east-1'` |
| `bucket_name` | S3 bucket name | `'warehouse'` |
| `path_style_access` | Use path-style access | `'true'` |

### 5. User Mapping for Volume

```sql
CREATE USER MAPPING FOR current_user
SERVER s3_volume_server
OPTIONS (
    username 'gpadmin',
    aws_access_key_id 'admin',
    aws_secret_access_key 'password'
);
```

### 6. Foreign Volume Creation

```sql
CREATE FOREIGN VOLUME s3_warehouse
SERVER s3_volume_server
OPTIONS (
    base_path '/warehouse/',
    enable_caching 'true',
    allow_writes 'true'
);
```

#### Foreign Volume Options

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `base_path` | Base storage path | `'/'` | `'/warehouse/'` |
| `enable_caching` | Enable data caching | `'false'` | `'true'` |
| `allow_writes` | Allow write operations | `'false'` | `'true'` |

## Usage Examples

### Creating an Iceberg Table

```sql
SELECT iceberg_toolkit.catalog_fdw(
    'create_table',
    'default',
    'my_iceberg_table',
    '{
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {
                "id": 1,
                "name": "id",
                "type": "long",
                "required": true
            },
            {
                "id": 2,
                "name": "name",
                "type": "string",
                "required": true
            },
            {
                "id": 3,
                "name": "age",
                "type": "int",
                "required": false
            }
        ]
    }',
    'example_catalog_server',
    'hive_production',
    's3_volume_server',
    's3_warehouse'
);
```

On a REST catalog the last two arguments — the volume server and volume name
— are passed empty, because such a table has no FOREIGN VOLUME: the catalog
assigns its location and vends the credentials for it.

```sql
SELECT iceberg_toolkit.catalog_fdw(
    'create_table', 'default', 'my_rest_table', '{ ... }',
    'xsky_catalog_server', 'xsky_production',
    '', ''
);
```

The same asymmetry shows up in DDL: `CREATE ICEBERG TABLE ... VOLUME v` is
rejected on a REST catalog, and `iceberg_default_volume` is bypassed rather
than applied, so a statically configured bucket can never override the
per-table one the catalog handed out.

### Schema Definition Format

The `schema_json` parameter follows the Iceberg schema specification:

```json
{
    "type": "struct",
    "schema-id": 0,
    "fields": [
        {
            "id": 1,
            "name": "column_name",
            "type": "data_type",
            "required": true|false
        }
    ]
}
```

#### Supported Data Types

| Iceberg Type | Description |
|--------------|-------------|
| `boolean` | Boolean values |
| `int` | 32-bit signed integer |
| `long` | 64-bit signed integer |
| `float` | 32-bit IEEE 754 floating point |
| `double` | 64-bit IEEE 754 floating point |
| `string` | UTF-8 encoded character string |
| `binary` | Arbitrary-length byte array |
| `date` | Calendar date without timezone |
| `time` | Time of day without timezone |
| `timestamp` | Timestamp with timezone |
| `timestamptz` | Timestamp without timezone |

## Operations

### Supported Operations

| Operation | Description |
|-----------|-------------|
| `create_table` | Create a new Iceberg table |
| `append` | Append data to existing table |
| `load` | Load data from table |

## Error Handling

The function returns JSON responses indicating success or failure:

### Success Response
```json
{
    "status": "success",
    "operation": "create_table",
    "table": "my_table",
    "message": "Table created successfully"
}
```

### Error Response
```json
{
    "status": "error",
    "operation": "create_table",
    "table": "my_table",
    "error": "Table already exists"
}
```

## Permissions

Grant necessary permissions to users:

```sql
GRANT USAGE ON SCHEMA iceberg_toolkit TO your_user;
GRANT EXECUTE ON FUNCTION iceberg_toolkit.catalog_fdw(text, text, text, text, text, text, text, text) TO your_user;
```

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify catalog and volume server configurations
2. **Authentication Failures**: Check user mappings and credentials
3. **Schema Validation**: Ensure JSON schema follows Iceberg specification
4. **Permission Denied**: Verify user has necessary grants

### Debugging

Enable detailed logging:
```sql
SET log_min_messages = DEBUG1;
```

## Limitations

- Currently supports Hive Metastore catalog
- S3-compatible storage systems supported
- Schema evolution operations limited
- Transaction isolation depends on underlying storage

## See Also

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Datalake FDW Main Documentation](./datalake_fdw.md)
- [PostgreSQL Foreign Data Wrapper Documentation](https://www.postgresql.org/docs/current/fdwhandler.html)
