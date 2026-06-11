/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cloud.elastic.dlagent.constants;

/**
 * Configuration constants for Iceberg REST API
 * Following the OpenAPI schema naming convention
 */
public final class IcebergConfigConstants {

    private IcebergConfigConstants() {
        // Utility class
    }

    public static final String ICEBERG_CONFIG = "IcebergConfig";

    // META domain: how to reach Iceberg table metadata (catalog: hive metastore / polaris / hadoop warehouse).
    public static final class ICEBERG_CATALOG_CONFIG {
        public static final String ICEBERG_CATALOG_CONFIG_STRING = "IcebergCatalogConfig";
        public static final String SERVER_TYPE = "server_type";
        // Config-file section name; when set, meta config is read from gphive.conf[SERVER_NAME].
        public static final String SERVER_NAME = "server_name";
        public static final String HIVE_METASTORE_URI = "hive_metastore_uri";
        public static final String USERNAME = "username";
        public static final String AUTH_METHOD = "auth_method";
        public static final String KRB_SERVICE_PRINCIPAL = "krb_service_principal";
        public static final String KRB_CLIENT_PRINCIPAL = "krb_client_principal";
        public static final String KRB_CLIENT_KEYTAB = "krb_client_keytab";
        public static final String CATALOG_NAME = "catalog_name";
        public static final String ENABLE_METADATA_CACHE = "enable_metadata_cache";
        public static final String METADATA_CACHE_TTL = "metadata_cache_ttl";
        public static final String AUTO_REFRESH_METADATA = "auto_refresh_metadata";
        public static final String WAREHOUSE_LOCATION_PERFIX = "warehouse_location_prefix";
        public static final String POLARIS_SERVER_URL = "polaris_server_url";
        public static final String CLIENT_ID = "client_id";
        public static final String CLIENT_SECRET = "client_secret";
        public static final String SCOPE = "scope";
    }

    //builtin catalog rest api option
    public static final class BUILDIN_CATALOG_OPTION {
        public static final String BUILDIN_CATALOG_STRING = "buildInCatalog";
        public static final String TABLE_EXISTS_PROP = "table_exists";
        public static final String METADATA_LOCATION_PROP = "metadata_location";
    }

    public static final String CATALOG_TYPE_HIVE = "hive";
    public static final String CATALOG_TYPE_S3 = "s3";
    public static final String CATALOG_TYPE_HADOOP = "hadoop";
    public static final String CATALOG_TYPE_BUILDIN = "builtin";
    public static final String CATALOG_TYPE_POLARIS = "polaris";

    // DATA domain: how to read/write Iceberg data + manifest files (storage/FileIO: s3 / hdfs / abfss).
    public static final class ICEBERG_VOLUME_CONFIG {
        public static final String ICEBERG_VOLUME_CONFIG_STRING = "IcebergVolumeConfig";
        public static final String VOLUME_SERVER_TYPE = "volume_server_type";
        // Config-file section name; when set, data config is read from s3.conf/gphdfs.conf[SERVER_NAME].
        public static final String SERVER_NAME = "server_name";
        public static final String VOLUME_ENDPOINT = "volume_endpoint";
        public static final String VOLUME_REGION = "volume_region";
        public static final String BUCKET_NAME = "bucket_name";
        public static final String PATH_STYLE_ACCESS = "path_style_access";
        public static final String ACCESS_KEY_ID = "access_key_id";
        public static final String SECRET_ACCESS_KEY = "secret_access_key";
        public static final String BASE_PATH = "base_path";
        public static final String ENABLE_CACHING = "enable_caching";
        public static final String ALLOW_WRITES = "allow_writes";
        public static final String USERNAME = "username";
    }

    // Hadoop configuration keys
    public static final String FS_S3A_IMPL = "fs.s3a.impl";
    public static final String FS_S3A_AWS_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";
    public static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
    public static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
    public static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
    public static final String FS_S3A_ENDPOINT_REGION = "fs.s3a.endpoint.region";
    public static final String FS_S3A_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";

    // Hadoop implementation values
    public static final String S3A_FILESYSTEM_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem";
    public static final String S3A_CREDENTIALS_PROVIDER = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";

    // S3FileIO (iceberg-aws)
    public static final String S3FILEIO_ACCESS_KEY_ID = "s3.access-key-id";
    public static final String S3FILEIO_SECRET_ACCESS_KEY = "s3.secret-access-key";
    public static final String S3FILEIO_ENDPOINT = "s3.endpoint";
    public static final String S3FILEIO_REGION = "s3.region";
    public static final String S3FILEIO_PATH_STYLE_ACCESS = "s3.path-style-access";

    // S3 Default value
    public static final String DEFAULT_S3_REGION_VALUE = "us-east-1";

    // Volume server types — must match FDW's iceberg_volume_option.c definitions.
    // Note: the legacy s3a / s3av2 aliases are intentionally NOT defined here;
    // FDW rejects them at OPTION parse time with a hint to use s3 / s3v2.
    public static final String VOLUME_TYPE_S3 = "s3";
    public static final String VOLUME_TYPE_S3V2 = "s3v2";
    public static final String VOLUME_TYPE_HDFS = "hdfs";
    public static final String VOLUME_TYPE_ABFSS = "abfss";

    /**
     * Prefix for pass-through FileIO extension keys (non-gopher path only).
     * Carries arbitrary {@code FileIOConfig.properties.<key>} entries from the
     * request body verbatim to Hadoop Configuration via emitS3Inline.
     */
    public static final String FILE_IO_CONFIG_PROPERTIES_PREFIX = "FileIOConfig.properties";

    // Common field names used by IcebergRequestConfigParser when reading the
    // request body. The "impl_class" / SimpleFileIOConfig / GopherFileIOConfig
    // wrapper keys and FileIO implementation class names are intentionally
    // omitted — FileIO selection is driven by gopher.enabled, not impl_class.
    public static final String COMMON = "common";
    public static final String PROPERTIES = "properties";

    public static final class ICEBERG_ADDITIONAL_CONFIG {
        public static final String ICEBERG_ADDITIONAL_CONFIG_STRING = "IcebergAdditionalConfig";
        public static final String TOTAL_SEGMENT = "totalSegment";
        public static final String SPLIT_SIZE = "splitSize";
        public static final String FILTER_STRING = "filterString";
        public static final String FILE_IO_CONFIG = "fileIOConfig";
        public static final String TABLE_IDENTIFIER = "tableIdentifier";
    }

    /**
     * Legacy wrapper field name retained <b>only</b> for backward-compat parsing
     * in {@link cloud.elastic.dlagent.api.model.IcebergRequestConfigParser};
     * old C clients still emit {@code fileIOConfig.gopherFileIOConfig.gopherConfig.common.*}.
     * The current OpenAPI schema uses the flattened
     * {@code fileIOConfig.gopherConfig.common.*} shape directly. Remove once
     * all C clients have migrated.
     */
    public static final class FILE_IO_CONFIG {
        public static final String GOPHER_FILEIO_CONFIG = "gopherFileIOConfig";
    }

    /**
     * Field names of the {@code GopherCommonConfig} schema in iceberg-openapi.yaml.
     * <b>Only runtime parameters live here</b>: connection info (endpoint /
     * bucket / credentials / region / path_style_access / ufs_type) is
     * exclusively carried by {@code IcebergVolumeConfig} and translated to
     * gopher.* keys by {@code GopherPropertiesResolver}.
     */
    public static final class GOPHER_CONFIG {
        public static final String GOPHER_HEADER = "gopher";
        public static final String GOPHER_CONFIG_STRING = "gopherConfig";
        public static final String WORKER_PATH = "worker_path";
        public static final String CONNECT_PATH = "connect_path";
        public static final String CONNECT_PLASMA_PATH = "connect_plasma_path";
        public static final String CACHE_STRATEGY = "cache_strategy";
        public static final String LOG_LEVEL = "log_level";
        public static final String LIBOSS2_LOG_SEVERITY = "liboss2_log_severity";
    }
}
