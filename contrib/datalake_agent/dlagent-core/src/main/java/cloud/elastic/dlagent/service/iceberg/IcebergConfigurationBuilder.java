/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.service.iceberg;

import cloud.elastic.dlagent.api.configuration.GopherConfigurationProperties;
import cloud.elastic.dlagent.api.model.BaseConfigurationFactory;
import cloud.elastic.dlagent.constants.IcebergConfigConstants;
import cloud.elastic.dlagent.plugins.iceberg.utilities.SensitiveDataRedactor;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

/**
 * Turns the flat {@code properties} map a datalake_fdw request carries into the
 * Hadoop {@link Configuration} the Iceberg catalogs and FileIO implementations
 * are built from.
 *
 * <p>Lives in core because the gRPC transport needs exactly the same
 * configuration for exactly the same requests as REST. Nothing here needs
 * {@code javax.servlet}, {@code org.springframework} or
 * {@code org.apache.catalina} to run, which is the criterion for what belongs
 * in core.
 *
 * <p>The method bodies are deliberately identical to the ones the REST
 * controller used to carry, because a configuration key that silently stops
 * being emitted is a failure no functional test would notice (the REST path
 * is the rollback path and may not drift).
 */
@Named
@Singleton
public class IcebergConfigurationBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergConfigurationBuilder.class);

    /**
     * Recognized {@code fileIOConfig.type} values posted by the datalake_fdw
     * consumer.  Kept as named constants so the supported storage schemes are
     * managed in one place rather than scattered as string literals.
     */
    private static final String FILEIO_CONFIG_TYPE_KEY = "type";
    private static final String FILEIO_TYPE_S3 = "s3";
    private static final String FILEIO_TYPE_HDFS = "hdfs";
    private static final String FILEIO_TYPE_ABFSS = "abfss";

    private final BaseConfigurationFactory configurationFactory;
    private final GopherConfigurationProperties gopherConfigurationProperties;

    @Inject
    public IcebergConfigurationBuilder(BaseConfigurationFactory configurationFactory,
                                       GopherConfigurationProperties gopherConfigurationProperties) {
        this.configurationFactory = configurationFactory;
        this.gopherConfigurationProperties = gopherConfigurationProperties;
    }

    /**
     * The one entry point: the Hadoop Configuration for a request's properties.
     * Called by {@code IcebergRequestContextFactory} while building the
     * RequestContext every endpoint starts from.
     */
    public Configuration getConfiguration(Map<String, String> properties) {
        LOG.debug("Initializing configuration for server");
        Configuration configuration = new Configuration(false);

        // gopher.enabled is a process-wide toggle from application.properties; it is
        // intentionally NOT taken from the per-request SQL options so a single call
        // cannot silently flip the FileIO stack.
        if (gopherConfigurationProperties != null
                && gopherConfigurationProperties.getEnabled() != null) {
            configuration.set("gopher.enabled",
                    String.valueOf(gopherConfigurationProperties.getEnabled()));
        }

        // Inject gopher.* system paths (connect_path / connect_plasma_path /
        // worker_path) the C side sends per request, so setupGopherConfiguration()
        // can reach gopher even without config files. These are gopher runtime
        // plumbing — not meta/data config — so they go in before both domains.
        // The request uses underscore keys (gopher.worker_path); Spring's
        // application.properties uses hyphen keys (gopher.worker-path) — different
        // namespaces, and the request's underscore keys are the ones consumed.
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("gopher.") && !key.equals("gopher.enabled")) {
                configuration.set(key, entry.getValue());
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("gopher: enabled={}, system paths worker_path={}, connect_path={}, connect_plasma_path={}",
                    configuration.get("gopher.enabled"),
                    configuration.get("gopher.worker_path"),
                    configuration.get("gopher.connect_path"),
                    configuration.get("gopher.connect_plasma_path"));
        }

        // Two independent domains: META (catalog) then DATA (storage/FileIO).
        buildMetaConfig(configuration, properties);
        buildDataConfig(configuration, properties);

        logResolvedConfig(configuration, properties);
        return configuration;
    }
    private void buildDataConfig(Configuration configuration, Map<String, String> properties) {
        String volumeType = properties.getOrDefault(
                volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE), "");
        if (volumeType.isEmpty()) {
            // No volume (e.g. some polaris setups). Storage credentials, if any,
            // reach the catalog through gopherProps; nothing to do here.
            LOG.debug("data: no volume_server_type; skipping data config (credentials, if any, via gopherProps)");
            return;
        }
        String serverName = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SERVER_NAME));
        boolean useConfFile = serverName != null && !serverName.isEmpty();

        if (volumeType.equals(IcebergConfigConstants.VOLUME_TYPE_S3) ||
            volumeType.equals(IcebergConfigConstants.VOLUME_TYPE_S3V2) ||
            volumeType.equals(IcebergConfigConstants.VOLUME_TYPE_ABFSS)) {
            if (useConfFile) {
                String s3Location = deriveS3Location(configuration, properties);
                LOG.debug("data: type={}, source=conf-file s3.conf[{}] location={} (inline access_key/secret ignored)",
                        volumeType, serverName, s3Location);
                loadConfFile("s3.conf", serverName, "data", s3Location, configuration);
            } else {
                LOG.debug("data: type={}, source=inline", volumeType);
                emitS3Inline(configuration, properties);
            }
        } else if (volumeType.equals(IcebergConfigConstants.VOLUME_TYPE_HDFS)) {
            if (useConfFile) {
                LOG.debug("data: type=hdfs, source=conf-file gphdfs.conf[{}] (inline options ignored)", serverName);
                loadConfFile("gphdfs.conf", serverName, "data", "", configuration);
            } else {
                LOG.debug("data: type=hdfs, source=inline");
                emitHdfsInline(configuration, properties);
            }
        } else {
            throw new UnsupportedOperationException(
                    volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE) + " '" + volumeType
                    + "' is not supported yet. Supported types are: ["
                    + IcebergConfigConstants.VOLUME_TYPE_S3 + ", " + IcebergConfigConstants.VOLUME_TYPE_S3V2 + ", "
                    + IcebergConfigConstants.VOLUME_TYPE_HDFS + ", " + IcebergConfigConstants.VOLUME_TYPE_ABFSS + "]");
        }
    }
    private void buildMetaConfig(Configuration configuration, Map<String, String> properties) {
        String catalogType = properties.getOrDefault(
                catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE), "");
        String serverName = properties.get(catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_NAME));
        boolean useConfFile = serverName != null && !serverName.isEmpty();

        if (catalogType.equals(IcebergConfigConstants.CATALOG_TYPE_HIVE)) {
            if (useConfFile) {
                LOG.debug("meta: type=hive, source=conf-file gphive.conf[{}]", serverName);
                loadConfFile("gphive.conf", serverName, "meta", "", configuration);
            } else {
                LOG.debug("meta: type=hive, source=inline");
                setHiveMetaInline(configuration, properties);
            }
        } else if (catalogType.equals(IcebergConfigConstants.CATALOG_TYPE_POLARIS)) {
            LOG.debug("meta: type=polaris, source=inline");
            setPolarisInline(configuration, properties);
        } else if (catalogType.equals(IcebergConfigConstants.CATALOG_TYPE_REST)) {
            LOG.debug("meta: type=rest, source=inline");
            setRestInline(configuration, properties);
        } else if (catalogType.equals(IcebergConfigConstants.CATALOG_TYPE_S3)) {
            LOG.debug("meta: type={}, source=inline (warehouse)", catalogType);
            setCatalogWarehouseInline(configuration, properties, false);
        } else if (catalogType.equals(IcebergConfigConstants.CATALOG_TYPE_HADOOP)) {
            LOG.debug("meta: type=hadoop, source=inline (warehouse)");
            setCatalogWarehouseInline(configuration, properties, true);
        } else if (catalogType.equals(IcebergConfigConstants.CATALOG_TYPE_BUILDIN)) {
            LOG.debug("meta: type=builtin, no catalog-side config");
        } else {
            throw new UnsupportedOperationException("This server type '" + catalogType + "' is not supported yet. " +
                    "Supported types are: [" +
                    IcebergConfigConstants.CATALOG_TYPE_S3 + ", " +
                    IcebergConfigConstants.CATALOG_TYPE_HADOOP + ", " +
                    IcebergConfigConstants.CATALOG_TYPE_HIVE + ", " + IcebergConfigConstants.CATALOG_TYPE_BUILDIN + ", " +
                    IcebergConfigConstants.CATALOG_TYPE_POLARIS + ", " +
                    IcebergConfigConstants.CATALOG_TYPE_REST + "]");
        }
    }
    private void emitS3Inline(Configuration configuration, Map<String, String> properties) {
        String accessKey = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ACCESS_KEY_ID));
        String secretKey = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SECRET_ACCESS_KEY));

        /* Diagnostic trace: see iceberg_volume_option.c step2 for context. */
        LOG.debug("[trace_ak] step5 emitS3Inline: read key=[{}] access_key len={}, secret len={}",
            volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ACCESS_KEY_ID),
            accessKey == null ? -1 : accessKey.length(),
            secretKey == null ? -1 : secretKey.length());

        String endpoint = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_ENDPOINT));
        String region = properties.getOrDefault(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_REGION),
                IcebergConfigConstants.DEFAULT_S3_REGION_VALUE);
        Boolean pathStyleAccess = Boolean.parseBoolean(
                properties.getOrDefault(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.PATH_STYLE_ACCESS), "true"));

        // hadoop-aws (HadoopFileIO via S3AFileSystem)
        configuration.set(IcebergConfigConstants.FS_S3A_IMPL, IcebergConfigConstants.S3A_FILESYSTEM_IMPL);
        configuration.set(IcebergConfigConstants.FS_S3A_AWS_CREDENTIALS_PROVIDER, IcebergConfigConstants.S3A_CREDENTIALS_PROVIDER);
        if (accessKey != null) configuration.set(IcebergConfigConstants.FS_S3A_ACCESS_KEY, accessKey);
        if (secretKey != null) configuration.set(IcebergConfigConstants.FS_S3A_SECRET_KEY, secretKey);
        if (endpoint != null) configuration.set(IcebergConfigConstants.FS_S3A_ENDPOINT, endpoint);
        /*
         * hadoop-aws 3.4.0 (HADOOP-18908): when fs.s3a.endpoint.region is not
         * set, S3A wraps the SDK client in S3CrossRegionSyncClient, whose
         * region probing bypasses the custom endpoint and dies with 60s
         * ApiCallTimeoutException retry storms on hosts without AWS egress
         * (CI runners).  Pin the region so S3A builds a plain client that
         * talks only to the configured endpoint.
         */
        if (region != null) configuration.set(IcebergConfigConstants.FS_S3A_ENDPOINT_REGION, region);
        configuration.set(IcebergConfigConstants.FS_S3A_PATH_STYLE_ACCESS, pathStyleAccess.toString());

        // iceberg-aws (S3FileIO / ResolvingFileIO)
        if (accessKey != null) configuration.set(IcebergConfigConstants.S3FILEIO_ACCESS_KEY_ID, accessKey);
        if (secretKey != null) configuration.set(IcebergConfigConstants.S3FILEIO_SECRET_ACCESS_KEY, secretKey);
        if (endpoint != null) configuration.set(IcebergConfigConstants.S3FILEIO_ENDPOINT, endpoint);
        if (region != null) configuration.set(IcebergConfigConstants.S3FILEIO_REGION, region);
        configuration.set(IcebergConfigConstants.S3FILEIO_PATH_STYLE_ACCESS, pathStyleAccess.toString());

        // Warehouse location for Hive-metastore-backed catalogs.
        String bucketName = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BUCKET_NAME));
        String basePath = properties.getOrDefault(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BASE_PATH), "/hive");
        if (bucketName != null) {
            configuration.set("hive.metastore.warehouse.dir", String.format("s3a://%s%s", bucketName, basePath));
        }

        // gopher path: IcebergUtilities.convertProtocolConfiguration only harvests
        // fs.gopher.* keys (the shape s3.conf produces). Mirror credentials there so
        // GopherFileIO/GopherFileSystem get endpoint + keys in the inline case too;
        // without this the gopher S3 path reaches gophermeta with empty OSS config.
        // These keys are inert when gopher is off (ResolvingFileIO reads s3.*).
        if (bucketName != null) configuration.set("fs.gopher.bucket", bucketName);
        if (accessKey != null) configuration.set("fs.gopher.access_key", accessKey);
        if (secretKey != null) configuration.set("fs.gopher.secret_key", secretKey);
        if (endpoint != null) {
            boolean useHttps = endpoint.startsWith("https://");
            String hostPort = endpoint.replaceFirst("^https?://", "");
            configuration.set("fs.gopher.endpoint", hostPort);
            configuration.set("fs.gopher.use_https", Boolean.toString(useHttps));
        }
        // Deliberately NOT setting fs.gopher.region: when present, gopher's native
        // OSS client builds an AWS-style host (s3.<region>.amazonaws.com) instead of
        // using fs.gopher.endpoint, which produces CURLE_COULDNT_RESOLVE_HOST against
        // self-hosted endpoints like minio. The working s3.conf shape omits region.
        configuration.set("fs.gopher.use_virtual_host", Boolean.toString(!pathStyleAccess));
        // Pass volume_server_type through verbatim — iceberg-gopher accepts
        // s3 / s3v2 natively. Hadoop fs.s3a URI/key naming is unaffected.
        String volType = properties.getOrDefault(
                volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE), "");
        if (!volType.isEmpty()) configuration.set("fs.gopher.ufs_type", volType);

        // Pass through all FileIOConfig.properties to configuration.
        String prefix = IcebergConfigConstants.FILE_IO_CONFIG_PROPERTIES_PREFIX + ".";
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                configuration.set(entry.getKey().substring(prefix.length()), entry.getValue());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("data: emitS3Inline wrote fs.s3a.*/s3.* (endpoint={}, region={}, pathStyle={}, warehouse={})",
                    endpoint, region, pathStyleAccess,
                    bucketName == null ? "<none>" : "s3a://" + bucketName + basePath);
            // Also dump the fs.gopher.* keys we just set so future "gopher param wrong"
            // debugging (e.g. endpoint with scheme, ufs_type=s3, virtual-host flips)
            // can be checked at this layer without bumping native LIBOSS2 trace.
            Map<String, String> fsGopher = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, String> e : configuration) {
                if (e.getKey().startsWith("fs.gopher.")) {
                    fsGopher.put(e.getKey(), e.getValue());
                }
            }
            LOG.debug("data: emitS3Inline fs.gopher.* set: {}", redactSecrets(fsGopher));
        }
    }
    private void emitHdfsInline(Configuration configuration, Map<String, String> properties) {
        String endpoint = properties.getOrDefault(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_ENDPOINT), "");

        // Namenode host[:port] resolution. Prefer the explicit hdfs_namenodes /
        // hdfs_port options; fall back to the legacy endpoint 'hdfs://host:port'.
        // A port spliced into hdfs_namenodes wins over hdfs_port. HA deployments
        // put the nameservice name in hdfs_namenodes (no port).
        String namenodes = properties.getOrDefault(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.HDFS_NAMENODES), "");
        String port = properties.getOrDefault(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.HDFS_PORT), "");
        boolean isHa = "true".equalsIgnoreCase(properties.getOrDefault(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.IS_HA_SUPPORTED), ""));

        String host = "";
        if (!namenodes.isEmpty()) {
            host = namenodes;
        } else if (!endpoint.isEmpty()) {
            String hostPort = endpoint.replaceFirst("^hdfs://", "");
            int colon = hostPort.lastIndexOf(':');
            if (colon >= 0) {
                host = hostPort.substring(0, colon);
                if (port.isEmpty()) {
                    port = hostPort.substring(colon + 1);
                }
            } else {
                host = hostPort;
            }
        }
        if (host.indexOf(':') >= 0) {
            int colon = host.lastIndexOf(':');
            port = host.substring(colon + 1);
            host = host.substring(0, colon);
        }
        if (port.isEmpty() && !isHa) {
            port = "8020";              // documented hdfs_port default
        }

        if (!host.isEmpty()) {
            String defaultFS = "hdfs://" + host + (port.isEmpty() ? "" : ":" + port);
            // fs.defaultFS already set by setCatalogWarehouseInline for hadoop
            // catalog; only set when not already configured (e.g. builtin/hive
            // catalog with hdfs volume).
            if (configuration.get("fs.defaultFS") == null
                || configuration.get("fs.defaultFS").isEmpty()) {
                configuration.set("fs.defaultFS", defaultFS);
            }
        }

        String authMethod = properties.getOrDefault(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.HDFS_AUTH_METHOD), "simple");
        // Default to simple auth when caller didn't specify; matches
        // the C-side iceberg_volume_fdw default in parseVolumeOption.
        if (configuration.get("hadoop.security.authentication") == null) {
            configuration.set("hadoop.security.authentication", authMethod);
        }

        // Gopher path: GopherFileSystem (the warehouse-dir filesystem when
        // gopher.enabled and fs.hdfs.impl=GopherFileSystem) and
        // convertProtocolConfiguration both read the fs.gopher.* keys; the
        // native client also reads the bare gopher.* keys. Without these the
        // gopher worker reaches HDFS with no namenode/port and fails with
        // "Unrecognized hdfs port 0". Mirror the proven legacy
        // transformHdfsConfig key set (both prefixes). emitS3Inline does the
        // equivalent for object storage.
        setGopherHdfsKey(configuration, "ufs_type", "hdfs");
        // Only emit name_node for non-HA deployments. In HA mode `host` is the
        // nameservice name (e.g. "mycluster"), not a real namenode hostname, so
        // setting gopher.name_node to it would make gopher attempt a direct TCP
        // connection to the nameservice id instead of following the HA failover
        // path. The legacy transformHdfsHaConfig intentionally never sets
        // name_node in HA mode; gopher resolves the namenode from the
        // dfs_nameservices / dfs_ha_namenodes / dfs_namenode_rpc_address keys
        // emitted in the isHa block below.
        if (!isHa) {
            setGopherHdfsKey(configuration, "name_node", host);
        }
        setGopherHdfsKey(configuration, "port", port);
        setGopherHdfsKey(configuration, "auth_method", authMethod);
        setGopherHdfsKey(configuration, "hadoop_rpc_protection", properties.get(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.HADOOP_RPC_PROTECTION)));
        setGopherHdfsKey(configuration, "data_transfer_protocol", properties.get(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DATA_TRANSFER_PROTOCOL)));
        setGopherHdfsKey(configuration, "data_transfer_protection", properties.get(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DATA_TRANSFER_PROTECTION)));
        setGopherHdfsKey(configuration, "krb_principal", properties.get(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.KRB_PRINCIPAL)));
        setGopherHdfsKey(configuration, "krb_server_key_file", properties.get(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.KRB_PRINCIPAL_KEYTAB)));
        // Not gated on isHa: using the DataNode hostname (rather than IP) is
        // meaningful in non-HA deployments too (e.g. a NAT between the segment
        // hosts and the HDFS DataNodes).
        setGopherHdfsKey(configuration, "dfs_client_use_datanode_hostname", properties.get(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DFS_CLIENT_USE_DATANODE_HOSTNAME)));
        if (isHa) {
            setGopherHdfsKey(configuration, "is_ha_supported", "true");
            setGopherHdfsKey(configuration, "dfs_nameservices", properties.get(volKey(
                IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DFS_NAMESERVICES)));
            setGopherHdfsKey(configuration, "dfs_ha_namenodes", properties.get(volKey(
                IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DFS_HA_NAMENODES)));
            setGopherHdfsKey(configuration, "dfs_namenode_rpc_address", properties.get(volKey(
                IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DFS_NAMENODE_RPC_ADDRESS)));
            setGopherHdfsKey(configuration, "dfs_client_failover_proxy_provider", properties.get(volKey(
                IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DFS_CLIENT_FAILOVER_PROXY_PROVIDER)));
        }
    }
    private void setGopherHdfsKey(Configuration configuration, String suffix, String value) {
        if (value == null || value.isEmpty()) {
            return;
        }
        configuration.set("gopher." + suffix, value);
        configuration.set("fs.gopher." + suffix, value);
    }
    private void setHiveMetaInline(Configuration configuration, Map<String, String> properties) {
        String hive_metastore_uris = properties.getOrDefault(
                catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.HIVE_METASTORE_URI), "");
        if (hive_metastore_uris.isEmpty()) {
            throw new IllegalArgumentException(
                    catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.HIVE_METASTORE_URI) + " is empty, must specify it");
        }
        configuration.set("hive.metastore.uris", hive_metastore_uris);

        String auth_method = properties.getOrDefault(
                catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.AUTH_METHOD), "");
        if (!auth_method.isEmpty()) {
            configuration.set("hadoop.security.authentication", auth_method);
        }
    }
    /** Copies the generic REST catalog allowlist into its runtime configuration. */
    private void setRestInline(Configuration configuration, Map<String, String> properties) {
        String[] keys = {
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.URL,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.AUTH_METHOD,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SIGNING_NAME,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ENDPOINT,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.PREFIX,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.REGION,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ACCESS_KEY_ID,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SECRET_ACCESS_KEY,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.PATH_STYLE_ACCESS,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.VENDED_CREDENTIALS,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_ID,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_SECRET,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SCOPE,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.WAREHOUSE_LOCATION_PERFIX,
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_REALM
        };
        for (String key : keys) {
            String value = properties.get(catKey(key));
            if (value != null && !value.isEmpty()) {
                configuration.set(key, value);
            }
        }
    }

    private void setPolarisInline(Configuration configuration, Map<String, String> properties) {
        String polarisServerUrlKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_URL;
        String polaris_server_url = properties.getOrDefault(polarisServerUrlKey, "");
        if (!polaris_server_url.isEmpty()) {
            configuration.set(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_URL, polaris_server_url);
        }

        String clientIdKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_ID;
        String client_id = properties.getOrDefault(clientIdKey, "");
        if (!client_id.isEmpty()) {
            configuration.set(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_ID, client_id);
        }

        String clientSecretKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_SECRET;
        String client_secret = properties.getOrDefault(clientSecretKey, "");
        if (!client_secret.isEmpty()) {
            configuration.set(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_SECRET, client_secret);
        }

        String scopeKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SCOPE;
        String scope = properties.getOrDefault(scopeKey, "");
        if (!scope.isEmpty()) {
            configuration.set(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SCOPE, scope);
        }

        // Realm for the Polaris-Realm header; IcebergPolarisCatalog falls back
        // to "POLARIS" when unset (issue #841).
        String realmKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_REALM;
        String realm = properties.getOrDefault(realmKey, "");
        if (!realm.isEmpty()) {
            configuration.set(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_REALM, realm);
        }
        // Volume credentials reach IcebergPolarisCatalog through
        // gopherProperties (translated from SQL options by
        // GopherPropertiesResolver via IcebergRequestConfigParser); Polaris
        // always uses GopherFileIO, so no S3A / S3FileIO config here.
    }
    private void setCatalogWarehouseInline(Configuration configuration,
                                                  Map<String, String> properties,
                                                  boolean isHadoopCatalog) {
        String warehouse = getCatalogWarehouseLocationPrefix(properties);
        if (warehouse == null || warehouse.isEmpty()) {
            String key = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING
                + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.WAREHOUSE_LOCATION_PERFIX;
            throw new IllegalArgumentException(
                key + " is required for s3/hadoop catalog server");
        }
        int schemeEnd = warehouse.indexOf("://");
        if (schemeEnd < 0) {
            throw new IllegalArgumentException(
                "warehouse_location_prefix must be a URL like scheme://host/path: " + warehouse);
        }
        int pathStart = warehouse.indexOf('/', schemeEnd + 3);
        String defaultFS;
        String pathPart; // includes leading '/'
        if (pathStart < 0) {
            defaultFS = warehouse;
            pathPart = "";
        } else {
            defaultFS = warehouse.substring(0, pathStart);
            pathPart = warehouse.substring(pathStart);
        }
        configuration.set("fs.defaultFS", defaultFS);

        if (!isHadoopCatalog) {
            // S3Catalog: warehouse = fs.defaultFS + "/" + fs.prefix.
            // The convention is no leading slash on fs.prefix. Strip
            // trailing slashes too to avoid double "//".
            String prefix = pathPart.startsWith("/") ? pathPart.substring(1) : pathPart;
            while (prefix.endsWith("/")) {
                prefix = prefix.substring(0, prefix.length() - 1);
            }
            configuration.set("fs.prefix", prefix);
        }

    }
    private void loadConfFile(String configFile, String serverName, String domain,
                              String location, Configuration configuration) {
        try {
            configurationFactory.processServerResource("", configFile, serverName, configuration, location);
        } catch (RuntimeException e) {
            throw new RuntimeException(String.format(
                    "%s config: failed to load section '%s' from '%s': %s",
                    domain, serverName, configFile, e.getMessage()), e);
        }
        LOG.debug("{}: loaded conf-file {}[{}] (location={})", domain, configFile, serverName, location);
    }
    private String deriveS3Location(Configuration configuration, Map<String, String> properties) {
        String defaultFS = configuration.get("fs.defaultFS");
        if (defaultFS != null && (defaultFS.startsWith("s3a://") || defaultFS.startsWith("s3://"))) {
            String prefix = configuration.get("fs.prefix");
            if (prefix != null && !prefix.isEmpty()) {
                return defaultFS + "/" + prefix;
            }
            return defaultFS;
        }
        String bucket = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BUCKET_NAME));
        if (bucket != null && !bucket.isEmpty()) {
            String basePath = properties.getOrDefault(
                    volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BASE_PATH), "/");
            if (!basePath.startsWith("/")) {
                basePath = "/" + basePath;
            }
            return String.format("s3a://%s%s", bucket, basePath);
        }
        return "";
    }
    private void logResolvedConfig(Configuration configuration, Map<String, String> properties) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        String metaType = properties.getOrDefault(catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE), "");
        String metaServer = properties.get(catKey(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_NAME));
        String dataType = properties.getOrDefault(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE), "");
        String dataServer = properties.get(volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SERVER_NAME));
        boolean gopherOn = "true".equalsIgnoreCase(configuration.get("gopher.enabled"));

        String metaSource = (IcebergConfigConstants.CATALOG_TYPE_HIVE.equals(metaType)
                && metaServer != null && !metaServer.isEmpty())
                ? "conf-file gphive.conf[" + metaServer + "]" : "inline";
        String dataSource;
        if (dataType.isEmpty()) {
            dataSource = "none";
        } else if (dataServer != null && !dataServer.isEmpty()) {
            dataSource = "conf-file[" + dataServer + "]";
        } else {
            dataSource = "inline";
        }
        LOG.info("iceberg config resolved: meta[type={}, source={}], data[type={}, source={}], gopher.enabled={}, fileIO={}",
                metaType.isEmpty() ? "<none>" : metaType, metaSource,
                dataType.isEmpty() ? "<none>" : dataType, dataSource,
                gopherOn, gopherOn ? "GopherFileIO" : "ResolvingFileIO");
    }
    /*
     * The four below plus getConfiguration are what IcebergRequestContextFactory
     * needs while assembling a RequestContext; inside the old controller they
     * were private siblings of that method.
     */
    public Map<String, String> getGopherProps(Map<String, String> properties) {
        Map<String, String> gopherProps = new HashMap<>();
        String prefix = IcebergConfigConstants.GOPHER_CONFIG.GOPHER_HEADER + ".";
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                gopherProps.put(entry.getKey(), entry.getValue());
            }
        }
        return gopherProps;
    }
    public Map<String, String> getBuildInCatalogProps(Map<String, String> properties) {
        Map<String, String> buildInCatalogProps = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(IcebergConfigConstants.BUILDIN_CATALOG_OPTION.BUILDIN_CATALOG_STRING + ".")) {
                buildInCatalogProps.put(entry.getKey(), entry.getValue());
            }
        }
        return buildInCatalogProps;
    }
    private static String volKey(String key) {
        return IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + key;
    }
    private static String catKey(String key) {
        return IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + key;
    }
    /** One redaction rule for the whole agent: see SensitiveDataRedactor. */
    public static Map<String, String> redactSecrets(Map<String, String> props) {
        @SuppressWarnings("unchecked")
        Map<String, String> safe = (Map<String, String>) (Map<?, ?>)
                SensitiveDataRedactor.redact(props);
        return safe;
    }
    /**
     * Public because {@code MetadataFileCleaner} builds its own FileIO
     * configuration with it (19 call sites there).
     */
    public static void putIfPresent(Configuration conf,
                                     Map<String, String> cfg,
                                     String cfgKey,
                                     String confKey) {
        String v = cfg.get(cfgKey);
        if (v != null && !v.isEmpty()) {
            conf.set(confKey, v);
        }
    }
    public String getCatalogWarehouseLocationPrefix(Map<String, String> properties) {
        String warehouseLocationPrefix = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.WAREHOUSE_LOCATION_PERFIX;
        String basePath = properties.getOrDefault(warehouseLocationPrefix, "");
        return basePath;
    }

    /*
     * VOLUME CONFIG FOR loadTable (issue #410 / #409).
     *
     * These live in core, next to their callers in IcebergOperations' shared
     * loadTable orchestration, rather than on the REST controller. This class
     * is where they belong anyway: volKey() below is theirs, and the gRPC
     * transport resolves volume config the same way REST does, which is the
     * whole point of having one core.
     */
    /**
     * Fill the load-table response's S3 configuration from the request config
     * this agent already resolved, for the keys {@code fileIO.properties()}
     * did not supply.
     *
     * <p>The FileIO behind a volume-backed table is GopherFileIO, which does
     * not implement {@code properties()}, so the block above produced an empty
     * map for every such table -- including on catalogs that do vend S3
     * settings. The segments consume this object to reach object storage, so
     * an empty one leaves them with whatever the SQL OPTIONS happened to carry
     * and no way to see the rest: precisely the gap that made a volume naming
     * only a conf section creatable but unusable (issue #410).
     *
     * <p>The values come from the parsed request, which the parser has already
     * merged with the {@code server_name} section of s3.conf, SQL winning per
     * key. Existing entries are never overwritten: a FileIO that does report
     * properties stays authoritative.
     */
    /* Package-private, not private: the two halves below have to agree on
     * which volume types they serve, and they have drifted apart twice.
     * IcebergResolvedVolumeConfigTest pins that they do not again. */
    static void addResolvedVolumeConfig(Map<String, String> config,
                                        Map<String, String> properties) {
        String type = properties.get(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE));

        if ("hdfs".equalsIgnoreCase(type)) {
            addResolvedHdfsVolumeConfig(config, properties);
            return;
        }

        /*
         * Everything else takes the S3 keys below, which is deliberate rather
         * than a fall-through: buildDataConfig groups s3, s3v2 and abfss into
         * one branch and configures all three with S3-style settings, so a
         * conf-only volume of those types needs the same keys returned to the
         * segments.  This did nothing for s3v2 and abfss before -- the guard
         * here returned early for any type that was not exactly "s3", so they
         * got an empty config.  Only "s3" is exercised end to end; the other
         * two have no environment here.
         */

        putResolved(config, "s3.endpoint", properties,
                    volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_ENDPOINT));
        putResolved(config, "s3.region", properties,
                    volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_REGION));
        putResolved(config, "client.region", properties,
                    volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_REGION));
        putResolved(config, "s3.path-style-access", properties,
                    volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.PATH_STYLE_ACCESS));
    }

    /**
     * The HDFS counterpart. Keys keep the SQL OPTION spelling rather than
     * taking an "hdfs." prefix, so a value can be traced from the conf file
     * through this response to gopher without a rename in the middle -- and
     * they are the same names iceberg_site_config uses on the C side.
     *
     * <p>Nothing here is sensitive: a keytab path and a principal name are
     * configuration, and the keytab's contents never travel.
     */
    private static void addResolvedHdfsVolumeConfig(Map<String, String> config,
                                             Map<String, String> properties) {
        String[] keys = {
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.HDFS_NAMENODES,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.HDFS_PORT,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.HDFS_AUTH_METHOD,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.KRB_PRINCIPAL,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.KRB_PRINCIPAL_KEYTAB,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.HADOOP_RPC_PROTECTION,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DATA_TRANSFER_PROTOCOL,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DATA_TRANSFER_PROTECTION,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.IS_HA_SUPPORTED,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DFS_NAMESERVICES,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DFS_HA_NAMENODES,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DFS_NAMENODE_RPC_ADDRESS,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DFS_CLIENT_FAILOVER_PROXY_PROVIDER,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.DFS_CLIENT_USE_DATANODE_HOSTNAME,
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.USERNAME,
        };

        for (String key : keys) {
            putResolved(config, key, properties, volKey(key));
        }
    }

    /**
     * The credential half of {@link #addResolvedVolumeConfig}. Kept separate
     * because these belong in storage-credentials rather than config, and must
     * never be logged.
     */
    static void addResolvedVolumeCredentials(Map<String, String> credConfig,
                                             Map<String, String> properties) {
        String type = properties.get(volKey(
            IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE));

        /*
         * Only HDFS is excluded, matching addResolvedVolumeConfig.  This used
         * to return for any type that was not exactly "s3", which stopped
         * mattering for the config half once that method started treating
         * s3v2 and abfss as the S3 family -- but left this half behind: those
         * volumes then received an endpoint, a region and a path-style flag
         * with no credentials to authenticate with, so a conf-only s3v2 or
         * abfss volume would fail at authentication rather than at
         * configuration, which is much harder to read.
         */
        if ("hdfs".equalsIgnoreCase(type)) {
            return;
        }

        putResolved(credConfig, "s3.access-key-id", properties,
                    volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ACCESS_KEY_ID));
        putResolved(credConfig, "s3.secret-access-key", properties,
                    volKey(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SECRET_ACCESS_KEY));
    }

    private static void putResolved(Map<String, String> out, String outKey,
                                    Map<String, String> properties, String inKey) {
        if (out.containsKey(outKey)) {
            return;
        }
        String value = properties.get(inKey);
        if (value != null && !value.isEmpty()) {
            out.put(outKey, value);
        }
    }
}
