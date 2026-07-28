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

package cloud.elastic.dlagent.plugins.iceberg.utilities;

import cloud.elastic.dlagent.api.configuration.GopherPropertiesResolver;
import cloud.elastic.dlagent.api.error.DlRuntimeException;
import cloud.elastic.dlagent.api.error.UnsupportedTypeException;
import cloud.elastic.dlagent.api.io.DataType;
import cloud.elastic.dlagent.api.model.Fragment;
import cloud.elastic.dlagent.api.model.Metadata;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.api.utilities.ColumnDescriptor;
import cloud.elastic.dlagent.api.utilities.EnumGpdbType;
import cloud.elastic.dlagent.api.utilities.GpdbFragmentMetadata;
import cloud.elastic.dlagent.api.utilities.Utilities;
import cloud.elastic.dlagent.service.rest.FileListRequest;
import cloud.elastic.dlagent.constants.IcebergConfigConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Class containing helper functions connecting
 * and interacting with Iceberg.
 */
@Component
public class IcebergUtilities {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergUtilities.class);

    /**
     * Resolver that owns the {@code gopher.enabled} master switch lookup.
     * Spring autowires it; callers continue to invoke {@link #isGopherEnabled(Configuration)}
     * unchanged so the 6 catalog implementations need no edits.
     */
    @Autowired
    private GopherPropertiesResolver gopherResolver;

    /**
     * Prefixes of runtime-configuration keys that must never appear in Iceberg
     * {@code TableMetadata.properties} (and therefore never in metadata.json).
     *
     * <p>These are the JSON sections that {@code IcebergRestController.extractProperties}
     * flattens into its mixed runtime-config map. Open-source Iceberg separates
     * {@code Catalog.initialize(catalogProps)} from
     * {@code Catalog.createTable(..., tableProps)}; we enforce the same by stripping
     * any key that belongs to a runtime-config section before it reaches
     * {@code TableMetadata}.
     *
     * <p>When a new section is added to the C&harr;Java JSON protocol, append its
     * prefix here.
     */
    private static final List<String> INTERNAL_PROPERTY_PREFIXES = Arrays.asList(
            "IcebergCatalogConfig.",
            "IcebergVolumeConfig.",
            "IcebergAdditionalConfig.",
            "FileIOConfig.",
            "gopherFileIOConfig.",
            "gopher.",
            "buildInCatalog."
    );

    /** Exact-match runtime-config keys (siblings of {@link #INTERNAL_PROPERTY_PREFIXES}). */
    private static final Set<String> INTERNAL_EXACT_KEYS;
    static {
        Set<String> s = new HashSet<>();
        s.add("config_files");
        INTERNAL_EXACT_KEYS = Collections.unmodifiableSet(s);
    }

    /**
     * True if the given key is one of our internal runtime-config keys and must never
     * land in Iceberg {@code TableMetadata.properties}.
     */
    public static boolean isInternalConfigKey(String key) {
        if (key == null || key.isEmpty()) {
            return false;
        }
        if (INTERNAL_EXACT_KEYS.contains(key)) {
            return true;
        }
        for (String prefix : INTERNAL_PROPERTY_PREFIXES) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return a new map containing only the user-facing table properties from
     * {@code properties} &mdash; any key recognised as internal runtime config
     * (see {@link #isInternalConfigKey}) is dropped.
     *
     * <p>Called at every {@code IcebergCatalog.createTable} boundary, at the HTTP
     * response assembler, and at commit-time self-heal to guarantee metadata.json
     * carries only open-source-Iceberg-legal TBLPROPERTIES.
     */
    public static Map<String, String> stripInternalProperties(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return new HashMap<>();
        }
        Map<String, String> filtered = new HashMap<>(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!isInternalConfigKey(entry.getKey())) {
                filtered.put(entry.getKey(), entry.getValue());
            }
        }
        return filtered;
    }

    /**
     * Checks if iceberg type is supported, and if so return its matching GPDB
     * type. Unsupported types will result in an exception. <br>
     * The supported mappings are:
     * <ul>
     * <li>{@code int -> int4}</li>
     * <li>{@code long -> int8}</li>
     * <li>{@code boolean -> bool}</li>
     * <li>{@code float -> float4}</li>
     * <li>{@code double -> float8}</li>
     * <li>{@code string -> text}</li>
     * <li>{@code uuid -> uuid}</li>
     * <li>{@code binary -> bytea}</li>
     * <li>{@code time -> time}</li>
     * <li>{@code timestamp -> timestamp}</li>
     * <li>{@code date -> date}</li>
     * <li>{@code decimal(precision, scale) -> numeric(precision, scale)}</li>
     * <li>{@code list<dataType> -> text}</li>
     * <li>{@code map<keyDataType, valueDataType> -> text}</li>
     * <li>{@code struct<field1:dataType,...,fieldN:dataType> -> text}</li>
     * </ul>
     *
     * @param icebergColumn iceberg column schema
     * @return field with mapped GPDB type and modifiers
     * @throws UnsupportedTypeException if the column type is not supported
     * @see EnumIcebergToGpdbType
     */
    public Metadata.Field mapIcebergType(Types.NestedField icebergColumn) throws UnsupportedTypeException {
        String fieldName = icebergColumn.name();
        String icebergType = EnumIcebergToGpdbType.getFullIcebergTypeName(icebergColumn); // Type name and modifiers if any
        String icebergTypeName; // Type name
        String[] modifiers = null; // Modifiers
        EnumIcebergToGpdbType icebergToGpdbType = EnumIcebergToGpdbType.getIcebergToGpdbType(icebergType);
        EnumGpdbType gpdbType = icebergToGpdbType.getGpdbType();

        if (icebergToGpdbType.getSplitExpression() != null) {
            String[] tokens = icebergType.split(icebergToGpdbType.getSplitExpression());
            icebergTypeName = tokens[0];
            if (gpdbType.getModifiersNum() > 0) {
                modifiers = Arrays.copyOfRange(tokens, 1, tokens.length);
                if (modifiers.length != gpdbType.getModifiersNum()) {
                    throw new UnsupportedTypeException(
                            "GPDB does not support type " + icebergType
                                    + " (Field " + fieldName + "), "
                                    + "expected number of modifiers: "
                                    + gpdbType.getModifiersNum()
                                    + ", actual number of modifiers: "
                                    + modifiers.length);
                }
                if (!Utilities.verifyIntegerModifiers(modifiers)) {
                    throw new UnsupportedTypeException("GPDB does not support type " + icebergType + " (Field " + fieldName + "), modifiers should be integers");
                }
            }
        } else
            icebergTypeName = icebergType;

        return new Metadata.Field(fieldName, gpdbType, icebergToGpdbType.isComplexType(), icebergTypeName, modifiers);
    }

    /**
     * Validates whether given GPDB and Iceberg data types are compatible.
     * If data type could have modifiers, GPDB data type is valid if it hasn't modifiers at all
     * or GPDB's modifiers are greater or equal to Iceberg's modifiers.
     * <p>
     * For example:
     * <p>
     * Iceberg type - numeric(20), GPDB type decimal(21) - valid.
     *
     * @param gpdbDataType   GPDB data type
     * @param gpdbTypeMods   GPDB type modifiers
     * @param icebergType    full Iceberg type, i.e. decimal(10,2)
     * @param gpdbColumnName Iceberg column name
     * @throws UnsupportedTypeException if types are incompatible
     */
    public void validateTypeCompatible(DataType gpdbDataType, Integer[] gpdbTypeMods, String icebergType, String gpdbColumnName) {
        EnumIcebergToGpdbType icebergToGpdbType = EnumIcebergToGpdbType.getIcebergToGpdbType(icebergType);
        EnumGpdbType expectedGpdbType = icebergToGpdbType.getGpdbType();

        if (!expectedGpdbType.getDataType().equals(gpdbDataType)) {
            throw new UnsupportedTypeException("Invalid definition for column " + gpdbColumnName
                    + ": expected GPDB type " + expectedGpdbType.getDataType() +
                    ", actual GPDB type " + gpdbDataType);
        }

        switch (gpdbDataType) {
            case NUMERIC:
                if (gpdbTypeMods != null && gpdbTypeMods.length > 0) {
                    Integer[] icebergTypeModifiers = EnumIcebergToGpdbType
                            .extractModifiers(icebergType);
                    for (int i = 0; i < icebergTypeModifiers.length; i++) {
                        if (gpdbTypeMods[i] < icebergTypeModifiers[i])
                            throw new UnsupportedTypeException(
                                    "Invalid definition for column " + gpdbColumnName
                                            + ": modifiers are not compatible, "
                                            + Arrays.toString(icebergTypeModifiers) + ", "
                                            + Arrays.toString(gpdbTypeMods));
                    }
                }
                break;
        }
    }

    public TableIdentifier getIcebergTableIdentifier(String tableName) {
        // If database not been specified in property, use default
        if (!tableName.contains(".")) {
            return TableIdentifier.of("default", tableName);
        }

        String[] tokens = tableName.split("\\.");
        if (tokens.length == 3) {
            tableName = tokens[1] + "." + tokens[2];
        }

        // Parse the (possibly modified) table name
        return TableIdentifier.parse(tableName);
    }

    /**
     * Compose Iceberg catalog properties from Hadoop Configuration.
     *
     * <p>Delegates to either Gopher or original S3 configuration based on gopher.enabled flag.
     */
    public Map<String, String> composeCatalogProperties(Configuration configuration) {
        if (isGopherEnabled(configuration)) {
            return composeGopherCatalogProperties(configuration);
        } else {
            return composeOriginalCatalogProperties(configuration);
        }
    }

    /**
     * Compose catalog properties for Gopher mode.
     *
     * <p>Sets up GopherFileSystem for metadata operations, converts legacy configuration
     * to unified fs.gopher.* format, and configures GopherFileIO for data file operations.
     */
    private Map<String, String> composeGopherCatalogProperties(Configuration configuration) {
        Map<String, String> props = new HashMap<>();
        List<String> configKeys = new ArrayList<>(Arrays.asList(
                CatalogProperties.FILE_IO_IMPL, CatalogProperties.IO_MANIFEST_CACHE_ENABLED,
                CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS,
                CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES,
                CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH));

        for (String key : configKeys) {
            String val = configuration.get("iceberg." + key);
            if (val != null) {
                props.put(key, val);
            }
        }

        // Set GopherFileIO if not explicitly configured
        if (!props.containsKey(CatalogProperties.FILE_IO_IMPL)) {
            props.put(CatalogProperties.FILE_IO_IMPL, "org.cbdb.iceberg.gopher.client.GopherFileIO");
            LOG.info("Gopher enabled, using GopherFileIO");
        }

        // Setup GopherFileSystem and related configuration
        Map<String, String> gopherProps = setupGopherConfiguration(configuration);
        props.putAll(gopherProps);

        return props;
    }

    /**
     * Compose catalog properties for original S3 mode.
     *
     * <p>Uses HadoopFileIO with original S3 configuration (pre-Gopher implementation).
     */
    private Map<String, String> composeOriginalCatalogProperties(Configuration configuration) {
        //maybe throw is better
        //throw new DlRuntimeException("Iceberg requires Gopher mode (gopher.enabled=true).");
        Map<String, String> props = new HashMap<>();
        List<String> configKeys = new ArrayList<>(Arrays.asList(
                CatalogProperties.FILE_IO_IMPL, CatalogProperties.IO_MANIFEST_CACHE_ENABLED,
                CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS,
                CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES,
                CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH));

        for (String key : configKeys) {
            String val = configuration.get("iceberg." + key);
            if (val != null) {
                props.put(key, val);
            }
        }

        // Gopher off: use the mixed ResolvingFileIO, which dispatches s3:// to
        // iceberg-aws S3FileIO (reads the s3.* keys) and other schemes (hdfs://)
        // to HadoopFileIO — no explicit per-scheme FileIO selection needed.
        if (!props.containsKey(CatalogProperties.FILE_IO_IMPL)) {
            props.put(CatalogProperties.FILE_IO_IMPL, org.apache.iceberg.io.ResolvingFileIO.class.getName());
            LOG.info("Gopher not enabled, using ResolvingFileIO (s3->S3FileIO, others->HadoopFileIO)");
        }

        // S3FileIO (reached via ResolvingFileIO for s3:// paths) reads its
        // credentials/region/endpoint from the catalog properties, not the Hadoop
        // Configuration. Propagate the s3.* keys that emitS3Inline / s3.conf set so
        // S3FileIO can authenticate (otherwise it fails with "Unable to load region").
        for (String key : new String[] {
                IcebergConfigConstants.S3FILEIO_ACCESS_KEY_ID,
                IcebergConfigConstants.S3FILEIO_SECRET_ACCESS_KEY,
                IcebergConfigConstants.S3FILEIO_ENDPOINT,
                IcebergConfigConstants.S3FILEIO_REGION,
                IcebergConfigConstants.S3FILEIO_PATH_STYLE_ACCESS}) {
            String val = configuration.get(key);
            if (val != null) {
                props.put(key, val);
            }
        }
        // Iceberg's S3FileIO reads the AWS client region from "client.region"
        // (AwsClientProperties), not "s3.region". Map it across so the AWS SDK
        // doesn't fall back to its region provider chain and fail.
        String s3Region = configuration.get(IcebergConfigConstants.S3FILEIO_REGION);
        if (s3Region != null && !s3Region.isEmpty()) {
            props.put("client.region", s3Region);
        }

        return props;
    }

    /**
     * Checks if Gopher should be enabled based on configuration.
     *
     * <p>Delegates to {@link GopherPropertiesResolver#isEnabled(Configuration)}
     * so the master-switch lookup lives in a single place.
     *
     * @param configuration Hadoop Configuration
     * @return true if Gopher should be used, false otherwise
     */
    public boolean isGopherEnabled(Configuration configuration) {
        return gopherResolver != null
                ? gopherResolver.isEnabled(configuration)
                : "true".equalsIgnoreCase(configuration.get("gopher.enabled"));
    }

    /**
     * Sets up GopherFileSystem and converts configuration to unified Gopher format.
     *
     * <p>This performs two main tasks:
     * <ol>
     * <li>Registers GopherFileSystem for gopher:// URI scheme</li>
     * <li>Converts legacy fs.&lt;protocol&gt;.* config to fs.gopher.* format</li>
     * </ol>
     *
     * @param configuration Hadoop Configuration with legacy settings
     * @return Map of Gopher-formatted properties for Iceberg FileIO
     */
    private Map<String, String> setupGopherConfiguration(Configuration configuration) {
        Map<String, String> gopherProps = new HashMap<>();

        // Determine the storage protocol
        String protocol = inferStorageProtocol(configuration);

        // Register GopherFileSystem for all relevant schemes
        String gopherFileSystemClass = "org.cbdb.iceberg.gopher.fs.GopherFileSystem";
        configuration.set("fs.gopher.impl", gopherFileSystemClass);
        configuration.set("fs.gs.impl", gopherFileSystemClass);
        configuration.set("fs." + protocol + ".impl", gopherFileSystemClass);

        LOG.info("Gopher mode: set fs.{}.impl to {}", protocol, gopherFileSystemClass);

        // Set UFS type
        gopherProps.put("gopher.ufs_type", protocol);
        configuration.set("fs.gopher.ufs_type", protocol);

        // Convert protocol-specific configuration to Gopher format
        convertProtocolConfiguration(configuration, gopherProps, protocol);

        // Copy basic Gopher configuration if present
        copyBasicGopherConfig(configuration, gopherProps);

        LOG.info("GopherFileSystem configured with protocol: {}", protocol);

        return gopherProps;
    }

    /**
     * Infers the storage protocol from configuration.
     *
     * <p>Checks in order:
     * <ol>
     * <li>Explicit fs.gopher.ufs_type setting</li>
     * <li>fs.defaultFS scheme</li>
     * <li>Default to "s3a"</li>
     * </ol>
     */
    private String inferStorageProtocol(Configuration configuration) {
        // Check explicit protocol setting
        String protocol = configuration.get("fs.gopher.ufs_type");
        if (protocol != null && !protocol.isEmpty()) {
            return protocol;
        }

        // Check fs.defaultFS
        String defaultFS = configuration.get("fs.defaultFS");
        if (defaultFS != null && !defaultFS.isEmpty()) {
            return defaultFS.split("://")[0];
        }

        // Default to S3A
        LOG.warn("Could not determine storage protocol, defaulting to s3a");
        return "s3a";
    }

    /**
     * Converts protocol-specific configuration to Gopher format.
     *
     * <p>Maps fs.&lt;protocol&gt;.&lt;key&gt; to gopher.&lt;key&gt;
     */
    private void convertProtocolConfiguration(Configuration configuration,
                                             Map<String, String> gopherProps,
                                             String protocol) {
        String protocolPrefix = "fs.gopher.";

        // Common object storage key mappings
        Map<String, String> objectStorageMappings = new HashMap<>();
        objectStorageMappings.put("bucket", "gopher.bucket");
        objectStorageMappings.put("access_key", "gopher.access_key");
        objectStorageMappings.put("secret_key", "gopher.secret_key");
        objectStorageMappings.put("endpoint", "gopher.endpoint");
        objectStorageMappings.put("region", "gopher.region");
        objectStorageMappings.put("use_https", "gopher.useHttps");
        objectStorageMappings.put("use_virtual_host", "gopher.useVirtualHost");

        // HDFS-specific mappings (keys match fs.gopher.* set by transformHdfsConfig)
        Map<String, String> hdfsMappings = new HashMap<>();
        hdfsMappings.put("name_node", "gopher.name_node");
        hdfsMappings.put("port", "gopher.port");
        hdfsMappings.put("auth_method", "gopher.auth_method");
        hdfsMappings.put("is_ha_supported", "gopher.is_ha_supported");
        hdfsMappings.put("ufs_type", "gopher.ufs_type");
        hdfsMappings.put("hadoop_rpc_protection", "gopher.hadoop_rpc_protection");
        hdfsMappings.put("krb_principal", "gopher.krb_principal");
        hdfsMappings.put("data_transfer_protocol", "gopher.data_transfer_protocol");
        hdfsMappings.put("dfs_nameservices", "gopher.dfs_nameservices");
        hdfsMappings.put("dfs_ha_namenodes", "gopher.dfs_ha_namenodes");
        hdfsMappings.put("dfs_namenode_rpc_address", "gopher.dfs_namenode_rpc_address");
        hdfsMappings.put("dfs_client_failover_proxy_provider", "gopher.dfs_client_failover_proxy_provider");
        hdfsMappings.put("dfs_client_use_datanode_hostname", "gopher.dfs_client_use_datanode_hostname");
        hdfsMappings.put("krb5_ticket_cache_path", "gopher.krb5_ticket_cache_path");
        hdfsMappings.put("krb_server_key_file", "gopher.krb_server_key_file");
        hdfsMappings.put("krb_delegation_token", "gopher.krb_delegation_token");

        // Choose mappings based on protocol
        Map<String, String> mappings = ("hdfs".equals(protocol)) ? hdfsMappings : objectStorageMappings;

        // Iterate through configuration and convert matching keys
        for (Map.Entry<String, String> entry : configuration) {
            String key = entry.getKey();
            if (key.startsWith(protocolPrefix)) {
                String suffix = key.substring(protocolPrefix.length());
                String gopherKey = mappings.get(suffix);

                if (gopherKey != null) {
                    String value = entry.getValue();
                    gopherProps.put(gopherKey, value);
                    configuration.set(gopherKey, value);
                    LOG.debug("Converted {} -> {}", key, gopherKey);
                }
            }
        }
    }

    /**
     * Copies basic Gopher configuration if present.
     */
    private void copyBasicGopherConfig(Configuration configuration,
                                       Map<String, String> gopherProps) {
        String[] basicKeys = {
            "gopher.worker_path",
            "gopher.connect_path",
            "gopher.connect_plasma_path",
            "gopher.cache_strategy",
            "gopher.mode",
            "gopher.log_level"
        };

        for (String key : basicKeys) {
            String value = configuration.get(key);
            if (value != null && !value.isEmpty()) {
                gopherProps.put(key, value);
                configuration.set("fs." + key, value);
            }
        }
    }

    /**
     * Checks that iceberg fields and partitions match the Greenplum schema.
     * Throws an exception if:
     * - A Greenplum column does not match any columns or partitions on the
     * Iceberg table definition
     * - The iceberg fields types do not match the Greenplum fields.
     *
     * @param schema the iceberg table
     */
    public void verifySchema(Schema schema, RequestContext context) {
        List<Types.NestedField> icebergColumns = schema.columns();
        Map<String, Types.NestedField> columnNameToFieldSchema = icebergColumns.stream()
                        .collect(Collectors.toMap(Types.NestedField::name, fieldSchema -> fieldSchema));

        Types.NestedField fieldSchema;
        for (ColumnDescriptor cd : context.getTupleDescription()) {
            if ((fieldSchema = columnNameToFieldSchema.get(cd.columnName())) == null &&
                    (fieldSchema = columnNameToFieldSchema.get(cd.columnName().toLowerCase())) == null) {
                throw new DlRuntimeException(
                        String.format("column '%s' does not exist in the Iceberg schema",
                                cd.columnName()),
                        "Ensure the column exists and check the name spelling and case."
                );
            }

            validateTypeCompatible(
                    cd.getDataType(),
                    cd.columnTypeModifiers(),
                    EnumIcebergToGpdbType.getFullIcebergTypeName(fieldSchema),
                    cd.columnName());
        }
    }

    public String extractWarehouseFromTableName(String tableName) {
        if (tableName.contains(".")) {
            return tableName.split("\\.")[0];
        }
        return "polaris";
    }

    /**
     *
     */
    public Schema formSchemaFromTupleDes(RequestContext context) {
        List<NestedField> fields = new ArrayList<>();
        for (ColumnDescriptor cd : context.getTupleDescription()) {
            Type icebergType = GpdbToIcebergType.getGpdbToIcebergType(cd);
            NestedField field = NestedField.optional(cd.columnIndex(), cd.columnName(), icebergType);
            fields.add(field);
        }
        return new Schema(fields);
    }

    /**
     * Create DataFile from FileEntry (from JSON POST request body).
     *
     * @param fileEntry FileEntry from JSON request body
     * @return DataFile for Iceberg
     */
    public DataFile transFileFromFileEntry(FileListRequest.FileEntry fileEntry,
                                           org.apache.iceberg.io.FileIO io,
                                           org.apache.iceberg.MetricsConfig metricsConfig) {
        // Parse format string to FileFormat enum
        org.apache.iceberg.FileFormat format;
        String formatStr = fileEntry.getFormat();
        if (formatStr == null) {
            format = org.apache.iceberg.FileFormat.PARQUET; // default
        } else {
            switch (formatStr.toUpperCase()) {
                case "PARQUET":
                    format = org.apache.iceberg.FileFormat.PARQUET;
                    break;
                case "ORC":
                    format = org.apache.iceberg.FileFormat.ORC;
                    break;
                case "AVRO":
                    format = org.apache.iceberg.FileFormat.AVRO;
                    break;
                default:
                    format = org.apache.iceberg.FileFormat.PARQUET;
                    break;
            }
        }

        DataFiles.Builder builder = DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath(fileEntry.getFilePath())
                        .withFormat(format)
                        .withFileSizeInBytes(fileEntry.getFileSize() != null ? fileEntry.getFileSize() : 0L)
                        .withRecordCount(fileEntry.getRecordCount() != null ? fileEntry.getRecordCount() : 0L);
        org.apache.iceberg.Metrics metrics =
                computeParquetMetrics(io, metricsConfig, fileEntry.getFilePath(), format.name());
        if (metrics != null) {
            builder.withMetrics(metrics);
        }
        return builder.build();
    }

    /**
     * Create DeleteFile from FileEntry (for POSITION_DELETE).
     *
     * @param fileEntry FileEntry from JSON request body
     * @return DeleteFile for Iceberg
     */
    public DeleteFile transPosDeleteFromFileEntry(FileListRequest.FileEntry fileEntry) {
        // Parse format string to FileFormat enum
        org.apache.iceberg.FileFormat format;
        String formatStr = fileEntry.getFormat();
        if (formatStr == null) {
            format = org.apache.iceberg.FileFormat.PARQUET; // default
        } else {
            switch (formatStr.toUpperCase()) {
                case "PARQUET":
                    format = org.apache.iceberg.FileFormat.PARQUET;
                    break;
                case "ORC":
                    format = org.apache.iceberg.FileFormat.ORC;
                    break;
                case "AVRO":
                    format = org.apache.iceberg.FileFormat.AVRO;
                    break;
                default:
                    format = org.apache.iceberg.FileFormat.PARQUET;
                    break;
            }
        }

        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                           .withPath(fileEntry.getFilePath())
                           .withFormat(format)
                           .withFileSizeInBytes(fileEntry.getFileSize() != null ? fileEntry.getFileSize() : 0L)
                           .withRecordCount(fileEntry.getRecordCount() != null ? fileEntry.getRecordCount() : 0L)
                           .ofPositionDeletes()
                           .build();
    }

    public void setupNativeLibGopherClientLibrary() {
        try {
            // Set LD_LIBRARY_PATH environment variable for native library dependencies
            String ldLibraryPath = System.getenv("LD_LIBRARY_PATH");
            String thirdPartyLibPath = "/workspace/dist/thirdparty/lib";
            String newLdLibraryPath;

            if (ldLibraryPath == null || ldLibraryPath.isEmpty()) {
                newLdLibraryPath = thirdPartyLibPath;
            } else {
                newLdLibraryPath = ldLibraryPath + ":" + thirdPartyLibPath;
            }

            // Try to set LD_LIBRARY_PATH via reflection
            try {
                java.lang.reflect.Field field = System.getenv().getClass().getDeclaredField("m");
                field.setAccessible(true);
                java.util.Map<String, String> env = (java.util.Map<String, String>) field.get(System.getenv());
                env.put("LD_LIBRARY_PATH", newLdLibraryPath);
                System.out.println("Set LD_LIBRARY_PATH: " + newLdLibraryPath);
            } catch (Exception e) {
                System.out.println("Failed to set LD_LIBRARY_PATH: " + e.getMessage());
            }

            // Set java.library.path to include the directory containing libgopherClient.so
            String libraryPath = System.getProperty("java.library.path");
            String gopherLibPath = "/workspace/dist/thirdparty/lib";
            String newLibraryPath;

            if (libraryPath == null || libraryPath.isEmpty()) {
                newLibraryPath = gopherLibPath;
            } else {
                newLibraryPath = libraryPath + ":" + gopherLibPath;
            }

            System.setProperty("java.library.path", newLibraryPath);
            System.out.println("Set java.library.path: " + newLibraryPath);

            // Try to load native library using System.loadLibrary first
            try {
                System.loadLibrary("gopherClient");
                System.out.println("Successfully loaded gopherClient native library using System.loadLibrary");
            } catch (UnsatisfiedLinkError e) {
                System.out.println(
                        "Failed to load gopherClient native library using System.loadLibrary: " + e.getMessage());
                System.out.println("Trying to load from absolute path...");

                // Try to load from absolute path
                try {
                    System.load("/workspace/dist/thirdparty/lib/libgopherClient.so");
                    System.out.println("Successfully loaded gopherClient native library from absolute path");
                } catch (UnsatisfiedLinkError e2) {
                    System.out.println(
                            "Failed to load gopherClient native library from absolute path: " + e2.getMessage());
                    System.out.println("Trying alternative path...");

                    // Try alternative path
                    try {
                        System.load("/workspace/share/Gopher/swig/libgopherClient.so");
                        System.out.println("Successfully loaded gopherClient native library from alternative path");
                    } catch (UnsatisfiedLinkError e3) {
                        System.out.println(
                                "Failed to load gopherClient native library from alternative path: " + e3.getMessage());
                        throw new RuntimeException("Unable to load gopherClient native library from any path", e3);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Error setting native library path: " + e.getMessage());
            throw new RuntimeException("Failed to setup native library", e);
        }
    }

    public DataFile transFileFromGpdb(Fragment gpdbFile,
                                      org.apache.iceberg.io.FileIO io,
                                      org.apache.iceberg.MetricsConfig metricsConfig) {
        GpdbFragmentMetadata metadata = (GpdbFragmentMetadata) gpdbFile.getMetadata();
        DataFiles.Builder builder = DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath(gpdbFile.getSourceName())
                        .withFormat(metadata.getFileFormat())
                        .withFileSizeInBytes(metadata.getFileSize())
                        .withRecordCount(metadata.getRowCount());
        org.apache.iceberg.Metrics metrics =
                computeParquetMetrics(io, metricsConfig, gpdbFile.getSourceName(), metadata.getFileFormat());
        if (metrics != null) {
            builder.withMetrics(metrics);
        }
        return builder.build();
    }

    /**
     * Derive column-level Metrics (record/value/null counts and lower/upper
     * bounds) by reading the just-written Parquet file footer, so the Iceberg
     * manifest carries the per-file statistics that TableScan.filter() needs to
     * prune data files at planning time.  Only Parquet is handled here; other
     * formats, a null FileIO, or any read failure return null -- pruning then
     * degrades to a full scan but results are never wrong.
     */
    private org.apache.iceberg.Metrics computeParquetMetrics(org.apache.iceberg.io.FileIO io,
                                                             org.apache.iceberg.MetricsConfig metricsConfig,
                                                             String path,
                                                             String fileFormat) {
        if (io == null || fileFormat == null || !"PARQUET".equalsIgnoreCase(fileFormat)) {
            return null;
        }
        try {
            org.apache.iceberg.io.InputFile inputFile = io.newInputFile(path);
            org.apache.iceberg.MetricsConfig mc =
                    (metricsConfig != null) ? metricsConfig : org.apache.iceberg.MetricsConfig.getDefault();
            return org.apache.iceberg.parquet.ParquetUtil.fileMetrics(inputFile, mc);
        } catch (Exception e) {
            LOG.warn("failed to compute parquet metrics for {}: {}", path, e.toString());
            return null;
        }
    }

    /**
     * Build the manifest entry for a position-delete file.
     *
     * Unlike data files, the Metrics here are not just a planning optimization.
     * Iceberg 1.3.0 has no DeleteFile.referencedDataFile(), so DeleteFileIndex
     * decides whether a position-delete file applies to a given data file solely
     * by comparing that data file's path against the delete file's file_path
     * column bounds (DeleteFileIndex.canContainPosDeletesForFile).  When no bounds
     * are recorded it returns true unconditionally, so every delete file gets
     * attached to every sequence-eligible data file -- an O(data files x delete
     * files) fan-out.  On a table with only 2910 data files and 2176 delete files
     * that produced 3.25M pairs and blew dlagent's heap (issue #407).
     *
     * The writer already makes each delete parquet file-scoped (a single distinct
     * file_path value, see iceberg_posdel_write.cpp), so lower == upper == that one
     * data file path and the range check degenerates to an exact match.
     *
     * MetricsConfig.forPositionDelete() is required rather than the table's normal
     * config: the default metrics mode is truncate(16), and data file paths share a
     * long common prefix, so truncated bounds would be identical across every
     * delete file and prune nothing.  forPositionDelete() pins file_path and pos to
     * Full.
     */
    public DeleteFile transPosDeleteFromGpdb(Fragment gpdbFile,
                                             org.apache.iceberg.io.FileIO io,
                                             org.apache.iceberg.MetricsConfig metricsConfig) {
        GpdbFragmentMetadata metadata = (GpdbFragmentMetadata) gpdbFile.getMetadata();
        FileMetadata.Builder builder =
                FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                            .withPath(gpdbFile.getSourceName())
                            .withFormat(metadata.getFileFormat())
                            .withFileSizeInBytes(metadata.getFileSize())
                            .withRecordCount(metadata.getRowCount())
                            .ofPositionDeletes();
        org.apache.iceberg.Metrics metrics =
                computeParquetMetrics(io, metricsConfig, gpdbFile.getSourceName(), metadata.getFileFormat());
        if (metrics != null) {
            builder.withMetrics(metrics);
        }
        return builder.build();
    }
}
