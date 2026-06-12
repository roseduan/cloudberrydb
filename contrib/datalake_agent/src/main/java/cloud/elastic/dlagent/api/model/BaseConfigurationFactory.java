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

package cloud.elastic.dlagent.api.model;

import cloud.elastic.dlagent.api.configuration.DlServerProperties;
import cloud.elastic.dlagent.api.configuration.GopherPropertiesResolver;
import cloud.elastic.dlagent.api.security.SecureLogin;
import cloud.elastic.dlagent.api.utilities.Utilities;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import cloud.elastic.dlagent.plugins.hudi.utilities.FilePathUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL;

@Component
public class BaseConfigurationFactory implements ConfigurationFactory {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Map<String, String> hiveOptionMapping = ImmutableMap.of(
            "uris", "hive.metastore.uris",
            "auth_method", "hadoop.security.authentication",
            "krb_service_principal", "hive.metastore.kerberos.principal",
            "krb_client_principal", SecureLogin.CONFIG_KEY_SERVICE_PRINCIPAL,
            "krb_client_keytab", SecureLogin.CONFIG_KEY_SERVICE_KEYTAB);

    private final Map<String, String> hdfsOptionMapping = ImmutableMap.of(
            "hdfs_auth_method", "hadoop.security.authentication",
            "krb_principal", SecureLogin.CONFIG_KEY_SERVICE_PRINCIPAL,
            "krb_principal_keytab", SecureLogin.CONFIG_KEY_SERVICE_KEYTAB,
            "hadoop_rpc_protection","hadoop.rpc.protection",
            "data_transfer_protocol", "dfs.encrypt.data.transfer");

    /** Resolver that owns gopher baseline + per-request normalization (single source of truth). */
    private final GopherPropertiesResolver gopherResolver;

    private final IcebergRequestConfigParser icebergRequestParser;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    public BaseConfigurationFactory(DlServerProperties dlagentServerProperties,
                                    GopherPropertiesResolver gopherResolver,
                                    IcebergRequestConfigParser icebergRequestParser) {
        this.gopherResolver = gopherResolver;
        this.icebergRequestParser = icebergRequestParser;
        LOG.info("BaseConfigurationFactory initialized; gopher baseline + iceberg parsing delegated to dedicated components");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration initConfiguration(String catalogType,
                                           String configFies,
                                           String serverName,
                                           String userName,
                                           String location,
                                           Map<String, String> additionalProperties) {
        // start with built-in Hadoop configuration that loads core-site.xml
        LOG.debug("Initializing configuration for server {}", serverName);

        Configuration configuration = new Configuration(false);

        if (!"s3".equals(catalogType)) {
            // while implementing multiple kerberized support we noticed that non-kerberized hadoop
            // access was trying to use SASL-client authentication. Setting the fallback to simple auth
            // allows us to still access non-kerberized hadoop clusters when there exists at least one
            // kerberized hadoop cluster. The root cause is that UGI has static fields and many hadoop
            // libraries depend on the state of the UGI
            // allow using SIMPLE auth for non-Kerberized HCFS access by SASL-enabled IPC client
            // that is created due to the fact that it uses UGI.isSecurityEnabled
            // and will try to use SASL if there is at least one Kerberized Hadoop cluster
            configuration.set(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, "true");

            // Starting with Hadoop 2.10.0, the "DEFAULT" rule will throw an
            // exception when no rules are applied while getting the principal
            // name translation into operating system user name. See
            // org.apache.hadoop.security.authentication.util.KerberosName#getShortName
            // We add a default rule that will return the service name as the
            // short name, i.e. gpadmin/_HOST@REALM will map to gpadmin
            configuration.set(HADOOP_SECURITY_AUTH_TO_LOCAL, "RULE:[1:$1] RULE:[2:$1] DEFAULT");
        }

        // set synthetic property dlagent.session.user so that is can be used in config files for interpolation in other properties
        // for example in JDBC when setting session authorization from a proxy user to the end-user
        configuration.set(DLAGENT_SESSION_USER_PROPERTY, userName);

        // add the server name itself as a configuration property
        configuration.set(DLAGENT_SERVER_NAME_PROPERTY, serverName);

        // Inject Gopher baseline from application.properties via the shared resolver.
        // GopherPropertiesResolver is the single source of truth for gopher.* keys.
        Map<String, String> gopherProps = gopherResolver.baseline();
        if (!gopherProps.isEmpty()) {
            LOG.debug("Injecting {} Gopher baseline properties from application.properties", gopherProps.size());
            gopherResolver.applyTo(configuration, gopherProps);
        }

        // add additional properties, if provided
        if (additionalProperties != null) {
            LOG.debug("Adding {} additional properties to configuration for server {}", additionalProperties.size(), serverName);
            additionalProperties.forEach(configuration::set);
        }

        if (!catalogType.toLowerCase().equals("polaris")) {
            // add all site files as URL resources to the configuration, no resources will be added from the classpath
            LOG.debug("Using config file {} for server {} configuration", configFies, serverName);
            String[] files = configFies.split("0");
            for (String file : files) {
                processServerResource(catalogType, file, serverName, configuration, location);
            }
        }

        try {
            // We need to set the restrict system properties to false so
            // variables in the configuration get replaced by system property
            // values
            configuration.setRestrictSystemProps(false);
        } catch (NoSuchMethodError e) {
            // Expected exception for MapR
        }

        return configuration;
    }

    /**
     * Initialize iceberg-related fields on {@link RequestContext} from the
     * iceberg config JSON string carried by dlproxy legacy clients.
     *
     * <p>Supports two on-wire shapes:
     * <ul>
     *   <li><b>New (REST shape)</b>: nested {@code IcebergConfig.IcebergAdditionalConfig.fileIOConfig.gopherConfig.common.*}.
     *       Delegated to {@link IcebergRequestConfigParser} — the sole entry
     *       point used by REST endpoints in {@code IcebergRestController}.</li>
     *   <li><b>Legacy (dlproxy shape)</b>: top-level {@code "gopher": { ... }} object
     *       carrying pre-translated {@code gopher.*} keys. Emitted by
     *       {@code contrib/datalake_fdw/src/dlproxy/icebergConfig.c::convertIcebergConfigToJson}.
     *       Parsed in-place here; merged directly into
     *       {@code RequestContext.gopherProperties}.</li>
     * </ul>
     *
     * <p>The legacy branch will be retired once the dlproxy emitter migrates
     * to the REST shape.
     */
    @Override
    public void initIcebergConfigFormJson(RequestContext context) {
        String jsonString = context.getIcebergConfigJsonString();
        if (jsonString == null || jsonString.isEmpty()) {
            LOG.warn("Invalid input parameters for initIcebergConfigFormJson");
            return;
        }

        try {
            Map<String, Object> jsonMap = OBJECT_MAPPER.readValue(jsonString,
                    new TypeReference<Map<String, Object>>() {});

            // Legacy dlproxy shape: top-level "iceberg_config_version" / "set_catalog_default_impl".
            Object versionRaw = jsonMap.get("iceberg_config_version");
            if (versionRaw != null) {
                context.setIcebergConfigVersion(versionRaw.toString());
            }
            Object catalogImplRaw = jsonMap.get("set_catalog_default_impl");
            if (catalogImplRaw != null) {
                context.setIcebergConfigUseDefaultCatalogImpl(catalogImplRaw.toString());
            }

            // New REST shape: hand the request body to the single iceberg parser.
            cloud.elastic.dlagent.api.model.iceberg.IcebergRequestConfig cfg =
                    icebergRequestParser.parse(jsonMap);

            if (cfg.getIcebergConfigVersion() != null) {
                context.setIcebergConfigVersion(cfg.getIcebergConfigVersion());
            }
            if (cfg.getSetCatalogDefaultImpl() != null) {
                context.setIcebergConfigUseDefaultCatalogImpl(cfg.getSetCatalogDefaultImpl());
            }

            // The parser has already resolved gopher.* keys (translated from the
            // request body or, when gopher.enabled=false, emitted s3.* keys
            // instead). Merge into RequestContext.gopherProperties so legacy
            // consumers see the same shape as before.
            Map<String, String> fileIOProps = cfg.getFileIOProps();
            if (fileIOProps != null && !fileIOProps.isEmpty()) {
                context.getGopherProperties().putAll(fileIOProps);
            }

            // Legacy dlproxy shape: top-level "gopher" object whose entries are
            // already pre-prefixed (or are bare runtime keys that we coerce to
            // the gopher.* namespace below).
            //
            // Merged LAST: for a legacy-shape request the parser above sees no
            // IcebergConfig section and still emits its baseline fileIOProps
            // (application.properties defaults with empty worker/connect
            // paths); merging the legacy values first let those empty baseline
            // values clobber the real ones from the request, and GopherClient
            // creation failed with "Gopher Worker path is required" (#844).
            Object legacyGopher = jsonMap.get("gopher");
            if (legacyGopher instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> gopherMap = (Map<String, Object>) legacyGopher;
                for (Map.Entry<String, Object> entry : gopherMap.entrySet()) {
                    if (entry.getValue() == null) {
                        continue;
                    }
                    String key = entry.getKey();
                    String propKey = key.startsWith("gopher.") ? key : "gopher." + key;
                    context.getGopherProperties().put(propKey, entry.getValue().toString());
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to init iceberg config from json string", e);
        }
    }

    /**
     * Legacy site-config loader retained for the hive/hudi/dlproxy plugin paths
     * which feed Hadoop {@link Configuration} directly.
     *
     * <p>The iceberg REST path does <b>not</b> call this method anymore — it
     * goes through {@code IcebergRequestConfigParser} (and
     * {@code SiteConfigLoader} for typed YAML loading). This shim continues to
     * exist solely so that hive/hudi {@code BaseServiceImpl} requests, which
     * invoke {@code initConfiguration} with a non-empty {@code configFiles},
     * keep working unchanged.
     */
    public void processServerResource(String catalogType,
                                      String configFile,
                                      String serverName,
                                      Configuration configuration,
                                      String location) {
        try (InputStream stream = new FileInputStream(configFile)) {
            Yaml yaml = new Yaml();
            Map<String, Map<String, Object>> configMap = yaml.load(stream);
            Map<String, Object> serverConfig = configMap.get(serverName);
            if (serverConfig == null) {
                throw new Exception("server \"" + serverName + "\" not found");
            }

            if (configFile.equals("gphive.conf")) {
                transformHiveConfig(serverConfig, configuration);
            } else if (configFile.equals("gphdfs.conf")) {
                transformHdfsConfig(serverConfig, configuration);
            } else if (configFile.equals("s3.conf")) {
                transformS3Config(serverConfig, configuration, location);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to read configuration for server \"%s\" from \"%s\": %s",
                    serverName, configFile, e.toString()));
        }
    }

    private void transformS3Config(Map<String, Object> serverMap, Configuration configuration, String location) {
        String[] bucketName = new String[1];
        String[] prefix = new String[1];
        Utilities.parserBucketAndPrefix(FilePathUtils.unescapeString(location), bucketName, prefix);

        // Check if Gopher mode is enabled
        configuration.set("fs.prefix", prefix[0]);

        String gopherEnabled = configuration.get("gopher.enabled");
        boolean isGopherMode = "true".equalsIgnoreCase(gopherEnabled);

        if (isGopherMode) {
            // Gopher mode: set fs.gopher.* configurations
            LOG.info("Configuring S3 in Gopher mode");
            configuration.set("fs.gopher.bucket", bucketName[0]);
            configuration.set("fs.gopher.prefix", prefix[0]);

            String protocol = (String)serverMap.get("fs.gopher.ufs_type");
            if (protocol != null && !protocol.isEmpty()) {
                configuration.set("fs.gopher.ufs_type", protocol);
                configuration.set("fs.defaultFS", String.format("%s://%s", protocol, bucketName[0]));
            }
        }

        // Process remaining server configurations
        serverMap.forEach((key, value) -> {
            if (key.equals("fs.defaultFS") && !isGopherMode) {
                String defaultFs = String.format("%s%s", value.toString(), bucketName[0]);
                configuration.set(key, defaultFs);
            } else {
                configuration.set(key, value.toString());
            }
        });
    }

    private void transformOptions(Map<String, Object> serverMap,
                                  Map<String, String> configMap,
                                  Configuration configuration) {
        serverMap.forEach((oldKey, value) -> {
            if (value == null) {
                return;
            }
            String newKey = configMap.get(oldKey);
            if (newKey != null) {
                configuration.set(newKey, value.toString());
                return;
            }

            configuration.set(oldKey, value.toString());
        });
    }

    private void transformHiveConfig(Map<String, Object> serverMap, Configuration configuration) {
        transformOptions(serverMap, hiveOptionMapping, configuration);
        if (Utilities.isSecurityEnabled(configuration)) {
            configuration.set("hive.metastore.sasl.enabled", "true");
            String rpcProtection = (String) serverMap.get("hadoop_rpc_protection");

            configuration.set("hadoop.rpc.protection", rpcProtection == null ? "authentication" : rpcProtection);
        }
    }

    private void transformHdfsConfig(Map<String, Object> serverMap, Configuration configuration) {
        transformOptions(serverMap, hdfsOptionMapping, configuration);

        // Check if Gopher mode is enabled
        String gopherEnabled = configuration.get("gopher.enabled");
        boolean isGopherMode = "true".equalsIgnoreCase(gopherEnabled);

        configuration.set("fs.protocol", "hdfs");
        configuration.set("ipc.client.connection.maxidletime", "900000");

        if (isGopherMode) {
            // Gopher mode: set fs.gopher.* and gopher.* configurations for HDFS
            LOG.info("Configuring HDFS in Gopher mode");

            // Set UFS type to hdfs
            configuration.set("gopher.ufs_type", "hdfs");
            configuration.set("fs.gopher.ufs_type", "hdfs");

            // Set GopherFileSystem for hdfs:// scheme
            String gopherFileSystemClass = "org.cbdb.iceberg.gopher.fs.GopherFileSystem";
            configuration.set("fs.hdfs.impl", gopherFileSystemClass);
            LOG.info("Gopher mode: set fs.hdfs.impl to {}", gopherFileSystemClass);
        } else {
            // Original HDFS mode
            LOG.info("Configuring HDFS in original HDFS mode");
        }

        if (Utilities.isSecurityEnabled(configuration)) {
            String servicePrincipal = (String) serverMap.get("krb_service_principal");
            if (servicePrincipal != null) {
                configuration.set("dfs.namenode.kerberos.principal", servicePrincipal);
                if (isGopherMode) {
                    configuration.set("gopher.krb_principal", servicePrincipal);
                    configuration.set("fs.gopher.krb_principal", servicePrincipal);
                }
            }

            String transferProtection = (String) serverMap.get("data_transfer_protection");
            if (transferProtection != null) {
                if (isGopherMode) {
                    // Gopher handles its own data transfer protection, including "none"
                    configuration.set("dfs.data.transfer.protection", transferProtection);
                    configuration.set("gopher.data_transfer_protocol", transferProtection);
                    configuration.set("fs.gopher.data_transfer_protocol", transferProtection);
                } else if (!"none".equalsIgnoreCase(transferProtection)) {
                    // HadoopFileIO: only set valid QualityOfProtection values (authentication/integrity/privacy).
                    // "none" is not a valid enum value and DataTransferSaslUtil copies dfs.data.transfer.protection
                    // to hadoop.rpc.protection, causing IllegalArgumentException in SaslPropertiesResolver.
                    configuration.set("dfs.data.transfer.protection", transferProtection);
                }
            }

            String rpcProtection = (String) serverMap.get("hadoop_rpc_protection");
            if (rpcProtection != null) {
                if (isGopherMode) {
                    configuration.set("gopher.hadoop_rpc_protection", rpcProtection);
                    configuration.set("fs.gopher.hadoop_rpc_protection", rpcProtection);
                }
            }
        }

        String serviceUser = (String) serverMap.get("service_user_name");
        if (serviceUser != null) {
            configuration.set(SecureLogin.CONFIG_KEY_SERVICE_USER_NAME, serviceUser);
        }

        // Get HDFS authentication method
        String authMethod = (String) serverMap.get("hdfs_auth_method");
        if (authMethod != null && isGopherMode) {
            configuration.set("gopher.auth_method", authMethod);
            configuration.set("fs.gopher.auth_method", authMethod);
        }

        Boolean enableHa = (Boolean) serverMap.get("is_ha_supported");
        if (enableHa == null || enableHa == false) {
            // Non-HA mode
            String host = String.valueOf(serverMap.get("hdfs_namenode_host"));
            String port = String.valueOf(serverMap.get("hdfs_namenode_port"));
            String defaultFs = String.format("hdfs://%s:%s", host, port);

            if (isGopherMode) {
                // Set Gopher-specific HDFS configurations
                configuration.set("gopher.name_node", host);
                configuration.set("fs.gopher.name_node", host);
                configuration.set("gopher.port", port);
                configuration.set("fs.gopher.port", port);
                configuration.set("gopher.is_ha_supported", "false");
                configuration.set("fs.gopher.is_ha_supported", "false");
            }

            configuration.set("fs.namenode.host", host);
            configuration.set("fs.namenode.port", port);
            configuration.set("fs.defaultFS", defaultFs);
            return;
        }

        // HA mode
        transformHdfsHaConfig(serverMap, configuration, isGopherMode);
    }

    private void transformHdfsHaConfig(Map<String, Object> serverMap, Configuration configuration, boolean isGopherMode) {
        String nameServices = (String) serverMap.get("dfs.nameservices");

        configuration.set("fs.nameservices", nameServices);
        configuration.set("fs.defaultFS", String.format("hdfs://%s", nameServices));

        if (isGopherMode) {
            // Set Gopher-specific HA configurations
            configuration.set("gopher.dfs_nameservices", nameServices);
            configuration.set("fs.gopher.dfs_nameservices", nameServices);
            configuration.set("gopher.is_ha_supported", "true");
            configuration.set("fs.gopher.is_ha_supported", "true");

            // Get HA namenodes
            String haNamenodes = (String) serverMap.get("dfs.ha.namenodes");
            if (haNamenodes != null) {
                configuration.set("gopher.dfs_ha_namenodes", haNamenodes);
                configuration.set("fs.gopher.dfs_ha_namenodes", haNamenodes);
            }

            // Get namenode RPC addresses
            String namenodeRpcAddress = (String) serverMap.get("dfs.namenode.rpc-address");
            if (namenodeRpcAddress != null) {
                configuration.set("gopher.dfs_namenode_rpc_address", namenodeRpcAddress);
                configuration.set("fs.gopher.dfs_namenode_rpc_address", namenodeRpcAddress);
            }

            // Get failover proxy provider
            String providerKey = String.format("dfs.client.failover.proxy.provider.%s", nameServices);
            String providerValue = (String) serverMap.get(providerKey);
            if (providerValue != null) {
                configuration.set("gopher.dfs_client_failover_proxy_provider", providerValue);
                configuration.set("fs.gopher.dfs_client_failover_proxy_provider", providerValue);
            }
        }

        serverMap.forEach((key, value) -> {
            if (value == null) {
                return;
            }

            if (key.startsWith("dfs")) {
                configuration.set(key, value.toString());
            }
        });

        String providerKey = String.format("dfs.client.failover.proxy.provider.%s", nameServices);
        String providerValue = (String) serverMap.get(providerKey);
        if (providerValue == null) {
            configuration.set(providerKey, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        }

        Boolean useHostName = (Boolean) serverMap.get("dfs_client_use_datanode_hostname");
        if (useHostName != null && useHostName == true) {
            configuration.set("dfs.client.use.datanode.hostname", "true");
            if (isGopherMode) {
                configuration.set("gopher.dfs_client_use_datanode_hostname", "true");
                configuration.set("fs.gopher.dfs_client_use_datanode_hostname", "true");
            }
        }
    }
}
