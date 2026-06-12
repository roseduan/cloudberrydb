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

package cloud.elastic.dlagent.api.configuration;

import cloud.elastic.dlagent.api.model.iceberg.CatalogInfo;
import cloud.elastic.dlagent.api.model.iceberg.VolumeInfo;
import cloud.elastic.dlagent.api.utilities.Utilities;
import cloud.elastic.dlagent.plugins.hudi.utilities.FilePathUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Single source of truth for loading {@code gphive.conf} / {@code s3.conf} /
 * {@code gphdfs.conf} YAML site configuration files.
 *
 * <p><b>Key naming contract (WYSIWYG)</b>: the keys inside a conf section are
 * the SQL OPTION names of the corresponding SERVER / USER MAPPING objects,
 * verbatim. There is no second vocabulary: what the user writes in
 * {@code CREATE SERVER ... OPTIONS (endpoint '...')} is the same key they
 * write in the conf section ({@code endpoint: ...}). Legacy Hadoop-style
 * spellings ({@code fs.s3a.*}, {@code fs.gopher.*}, {@code uris}, dotted
 * {@code dfs.*}, {@code hdfs_namenode_host/_port}) are hard errors here;
 * the error message names the replacement key. The legacy generic-FDW path
 * ({@code BaseConfigurationFactory}) keeps its own tolerant reader.
 *
 * <p>This class performs the YAML parse and translates the matched section into
 * a typed {@link CatalogInfo} / {@link VolumeInfo} POJO. Downstream consumers
 * decide how to project those POJOs into their backend-specific key namespaces
 * (gopher.* via {@link GopherPropertiesResolver}, s3.* via
 * {@link S3FileIOPropertiesTransformer}, or hive.* / dfs.* / hadoop.* via the
 * legacy paths inside {@code BaseConfigurationFactory}).
 *
 * <p>All loader methods are <b>no-op when {@code serverName} is null or empty</b>.
 * This implements the design rule that site files are read only when a
 * {@code server_name} is explicitly supplied by the FDW. The returned POJO is
 * merged underneath the SQL OPTIONS by {@code IcebergRequestConfigParser}
 * (SQL wins per key; conf supplies the fallback).
 */
@Component
@Slf4j
public class SiteConfigLoader {

    static final String FILE_GPHIVE = "gphive.conf";
    static final String FILE_S3     = "s3.conf";
    static final String FILE_GPHDFS = "gphdfs.conf";

    /**
     * Directory the conf files are resolved against. Defaults to the process
     * working directory (the agent's deployment contract); tests inject a
     * temp directory because the JVM cwd cannot be changed mid-process.
     */
    private String baseDir = ".";

    void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    /*
     * Legacy-key tables: exact old spelling -> replacement SQL OPTION name.
     * Iteration order shows up in error messages, hence LinkedHashMap.
     */
    private static final Map<String, String> S3_LEGACY_KEYS = new LinkedHashMap<>();
    private static final Map<String, String> HDFS_LEGACY_KEYS = new LinkedHashMap<>();
    private static final Map<String, String> HIVE_LEGACY_KEYS = new LinkedHashMap<>();

    /* Legacy prefix -> replacement hint (covers keys not in the exact maps). */
    private static final Map<String, String> S3_LEGACY_PREFIXES = new LinkedHashMap<>();
    private static final Map<String, String> HDFS_LEGACY_PREFIXES = new LinkedHashMap<>();
    private static final Map<String, String> HIVE_LEGACY_PREFIXES = new LinkedHashMap<>();

    static {
        S3_LEGACY_KEYS.put("fs.gopher.ufs_type", "type");
        S3_LEGACY_KEYS.put("fs.s3a.endpoint", "endpoint");
        S3_LEGACY_KEYS.put("fs.s3a.access.key", "access_key_id");
        S3_LEGACY_KEYS.put("fs.s3a.secret.key", "secret_access_key");
        S3_LEGACY_KEYS.put("fs.s3a.endpoint.region", "region");
        S3_LEGACY_KEYS.put("fs.s3a.path.style.access", "path_style_access");
        S3_LEGACY_PREFIXES.put("fs.s3a.", "the matching SQL OPTION name");
        S3_LEGACY_PREFIXES.put("fs.gopher.", "the matching SQL OPTION name");

        HDFS_LEGACY_KEYS.put("hdfs_namenode_host", "hdfs_namenodes");
        HDFS_LEGACY_KEYS.put("hdfs_namenode_port", "hdfs_port");
        HDFS_LEGACY_KEYS.put("dfs.nameservices", "dfs_nameservices");
        HDFS_LEGACY_KEYS.put("dfs.ha.namenodes", "dfs_ha_namenodes");
        HDFS_LEGACY_KEYS.put("dfs.namenode.rpc-address", "dfs_namenode_rpc_address");
        HDFS_LEGACY_PREFIXES.put("dfs.client.failover.proxy.provider",
                "dfs_client_failover_proxy_provider");
        HDFS_LEGACY_PREFIXES.put("dfs.", "the underscore spelling, e.g. \"dfs_nameservices\"");

        HIVE_LEGACY_KEYS.put("uris", "url");
    }

    /**
     * Load and translate the {@code gphive.conf[serverName]} section into a
     * fresh {@link CatalogInfo}. Section keys are the catalog SERVER /
     * USER MAPPING option names: {@code url}, {@code username},
     * {@code auth_method}, {@code krb_service_principal},
     * {@code krb_client_principal}, {@code krb_client_keytab}.
     *
     * @param catalogServerName server name from {@code IcebergCatalogConfig.server_name};
     *                          when null or empty, an empty {@link CatalogInfo} is returned
     *                          and no file IO is performed
     * @return a populated or empty {@link CatalogInfo}; never null
     */
    public CatalogInfo loadHiveSite(String catalogServerName) {
        CatalogInfo info = new CatalogInfo();
        if (isBlank(catalogServerName)) {
            return info;
        }
        Map<String, Object> serverMap = readServerSection(FILE_GPHIVE, catalogServerName);
        if (serverMap == null) {
            return info;
        }
        rejectLegacyKeys(FILE_GPHIVE, catalogServerName, serverMap,
                HIVE_LEGACY_KEYS, HIVE_LEGACY_PREFIXES);
        info.setServerName(catalogServerName);
        info.setHiveMetastoreUri(asString(serverMap.get("url")));
        info.setUsername(asString(serverMap.get("username")));
        info.setAuthMethod(asString(serverMap.get("auth_method")));
        info.setKrbServicePrincipal(asString(serverMap.get("krb_service_principal")));
        info.setKrbClientPrincipal(asString(serverMap.get("krb_client_principal")));
        info.setKrbClientKeytab(asString(serverMap.get("krb_client_keytab")));

        // Preserve any unknown keys as pass-through hadoop configuration entries
        // (e.g. hadoop_rpc_protection).
        for (Map.Entry<String, Object> entry : serverMap.entrySet()) {
            String key = entry.getKey();
            if (entry.getValue() == null) {
                continue;
            }
            if (isHiveTypedKey(key)) {
                continue;
            }
            info.getExtraProperties().put(key, entry.getValue().toString());
        }
        return info;
    }

    /**
     * Load and translate the {@code s3.conf[serverName]} section into a fresh
     * {@link VolumeInfo}. Section keys are the volume SERVER / USER MAPPING
     * option names: {@code type}, {@code endpoint}, {@code region},
     * {@code bucket_name}, {@code path_style_access}, {@code username},
     * {@code access_key_id}, {@code secret_access_key}.
     *
     * <p>The {@code location} argument is a fallback for the bucket name only:
     * when the section carries no {@code bucket_name}, the bucket is derived
     * from the S3 location URL (such as {@code s3a://bucket/prefix}).
     *
     * @param volumeServerName server name from {@code IcebergVolumeConfig.server_name};
     *                         when null or empty, an empty {@link VolumeInfo} is returned
     *                         and no file IO is performed
     * @param location         optional S3 URL whose bucket portion populates
     *                         {@link VolumeInfo#getBucketName()} as a fallback
     * @return a populated or empty {@link VolumeInfo}; never null
     */
    public VolumeInfo loadS3Site(String volumeServerName, String location) {
        VolumeInfo info = new VolumeInfo();
        if (isBlank(volumeServerName)) {
            return info;
        }
        Map<String, Object> serverMap = readServerSection(FILE_S3, volumeServerName);
        if (serverMap == null) {
            return info;
        }
        rejectLegacyKeys(FILE_S3, volumeServerName, serverMap,
                S3_LEGACY_KEYS, S3_LEGACY_PREFIXES);
        info.setServerName(volumeServerName);
        info.setVolumeServerType(asString(serverMap.get("type")));
        info.setVolumeEndpoint(asString(serverMap.get("endpoint")));
        info.setAccessKeyId(asString(serverMap.get("access_key_id")));
        info.setSecretAccessKey(asString(serverMap.get("secret_access_key")));
        info.setVolumeRegion(asString(serverMap.get("region")));
        info.setUsername(asString(serverMap.get("username")));
        if (serverMap.get("path_style_access") != null) {
            info.setPathStyleAccess(parseBoolean(serverMap.get("path_style_access")));
        }
        info.setBucketName(asString(serverMap.get("bucket_name")));

        // Fallback: derive bucket from location (e.g. "s3a://mybucket/path").
        if (info.getBucketName() == null && location != null && !location.isEmpty()) {
            String[] bucket = new String[1];
            String[] prefix = new String[1];
            Utilities.parserBucketAndPrefix(FilePathUtils.unescapeString(location), bucket, prefix);
            if (bucket[0] != null) {
                info.setBucketName(bucket[0]);
            }
        }

        // Keep all remaining keys as pass-through extras (e.g. fs.defaultFS).
        for (Map.Entry<String, Object> entry : serverMap.entrySet()) {
            String key = entry.getKey();
            if (entry.getValue() == null || isS3TypedKey(key)) {
                continue;
            }
            info.getExtraProperties().put(key, entry.getValue().toString());
        }
        return info;
    }

    /**
     * Load and translate the {@code gphdfs.conf[serverName]} section into a
     * fresh {@link VolumeInfo}. Section keys are the volume SERVER hdfs option
     * names: {@code hdfs_namenodes} (host or host:port; the spliced port wins
     * over {@code hdfs_port}), {@code hdfs_port}, {@code hdfs_auth_method},
     * {@code krb_principal}, {@code krb_principal_keytab},
     * {@code hadoop_rpc_protection}, {@code data_transfer_protocol},
     * {@code is_ha_supported}, the underscore {@code dfs_*} HA keys,
     * {@code dfs_client_use_datanode_hostname} and {@code username}.
     *
     * <p>The object-storage fields on the returned VolumeInfo are deliberately
     * left null.
     *
     * @param volumeServerName server name from {@code IcebergVolumeConfig.server_name};
     *                         when null or empty, an empty {@link VolumeInfo} is returned
     *                         and no file IO is performed
     * @return a populated or empty {@link VolumeInfo}; never null
     */
    public VolumeInfo loadHdfsSite(String volumeServerName) {
        VolumeInfo info = new VolumeInfo();
        if (isBlank(volumeServerName)) {
            return info;
        }
        Map<String, Object> serverMap = readServerSection(FILE_GPHDFS, volumeServerName);
        if (serverMap == null) {
            return info;
        }
        rejectLegacyKeys(FILE_GPHDFS, volumeServerName, serverMap,
                HDFS_LEGACY_KEYS, HDFS_LEGACY_PREFIXES);
        info.setServerName(volumeServerName);
        info.setVolumeServerType("hdfs");

        // hdfs_namenodes carries "host" or "host:port"; a port spliced into
        // the value wins over the separate hdfs_port key. HA deployments put
        // the nameservice name here (no port).
        String namenodes = asString(serverMap.get("hdfs_namenodes"));
        String port = asString(serverMap.get("hdfs_port"));
        if (namenodes != null && namenodes.indexOf(':') >= 0) {
            int idx = namenodes.lastIndexOf(':');
            info.setHdfsNamenodeHost(namenodes.substring(0, idx));
            info.setHdfsNamenodePort(namenodes.substring(idx + 1));
        } else {
            info.setHdfsNamenodeHost(namenodes);
            info.setHdfsNamenodePort(port);
        }

        info.setHdfsAuthMethod(asString(serverMap.get("hdfs_auth_method")));
        info.setKrbPrincipal(asString(serverMap.get("krb_principal")));
        info.setKrbPrincipalKeytab(asString(serverMap.get("krb_principal_keytab")));
        info.setHadoopRpcProtection(asString(serverMap.get("hadoop_rpc_protection")));
        info.setDataTransferProtocol(asString(serverMap.get("data_transfer_protocol")));
        info.setUsername(asString(serverMap.get("username")));

        Object haFlag = serverMap.get("is_ha_supported");
        if (haFlag != null) {
            info.setIsHaSupported(parseBoolean(haFlag));
        }
        info.setDfsNameservices(asString(serverMap.get("dfs_nameservices")));
        info.setDfsHaNamenodes(asString(serverMap.get("dfs_ha_namenodes")));
        info.setDfsNamenodeRpcAddress(asString(serverMap.get("dfs_namenode_rpc_address")));
        info.setDfsClientFailoverProxyProvider(asString(serverMap.get("dfs_client_failover_proxy_provider")));

        Object useHostname = serverMap.get("dfs_client_use_datanode_hostname");
        if (useHostname != null) {
            info.setDfsClientUseDatanodeHostname(parseBoolean(useHostname));
        }

        // Keep all remaining keys (e.g. krb_service_principal) as pass-through extras.
        for (Map.Entry<String, Object> entry : serverMap.entrySet()) {
            String key = entry.getKey();
            if (entry.getValue() == null || isHdfsTypedKey(key)) {
                continue;
            }
            info.getExtraProperties().put(key, entry.getValue().toString());
        }
        return info;
    }

    // ---- helpers ----------------------------------------------------------

    @SuppressWarnings("unchecked")
    private Map<String, Object> readServerSection(String configFile, String serverName) {
        try (InputStream stream = new FileInputStream(new File(baseDir, configFile))) {
            Yaml yaml = new Yaml();
            Map<String, Map<String, Object>> configMap = yaml.load(stream);
            if (configMap == null) {
                log.warn("Site config file {} is empty", configFile);
                return null;
            }
            Map<String, Object> serverSection = configMap.get(serverName);
            if (serverSection == null) {
                throw new IOException("server \"" + serverName + "\" not found in " + configFile);
            }
            return serverSection;
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "Unable to read configuration for server \"%s\" from \"%s\": %s",
                    serverName, configFile, e.toString()), e);
        }
    }

    /**
     * Fail fast when a section still uses legacy key spellings. All offending
     * keys are aggregated into one error so the user fixes the file in one
     * pass instead of replaying error-by-error.
     */
    private static void rejectLegacyKeys(String configFile, String serverName,
                                         Map<String, Object> serverMap,
                                         Map<String, String> legacyExact,
                                         Map<String, String> legacyPrefixes) {
        StringBuilder offending = new StringBuilder();
        for (String key : serverMap.keySet()) {
            String replacement = legacyExact.get(key);
            if (replacement == null) {
                for (Map.Entry<String, String> prefix : legacyPrefixes.entrySet()) {
                    if (key.startsWith(prefix.getKey())) {
                        replacement = prefix.getValue();
                        break;
                    }
                }
            }
            if (replacement == null) {
                continue;
            }
            if (offending.length() > 0) {
                offending.append(", ");
            }
            offending.append('"').append(key).append('"');
            if (replacement.startsWith("the ")) {
                offending.append(" (use ").append(replacement).append(')');
            } else {
                offending.append(" (use \"").append(replacement).append("\")");
            }
        }
        if (offending.length() > 0) {
            throw new RuntimeException(String.format(
                    "configuration file %s, server \"%s\": legacy key(s) no longer supported: %s; "
                            + "keys must match the SQL OPTION names",
                    configFile, serverName, offending));
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isEmpty();
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static Boolean parseBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private static boolean isHiveTypedKey(String key) {
        switch (key) {
            case "url":
            case "username":
            case "auth_method":
            case "krb_service_principal":
            case "krb_client_principal":
            case "krb_client_keytab":
                return true;
            default:
                return false;
        }
    }

    private static boolean isS3TypedKey(String key) {
        switch (key) {
            case "type":
            case "endpoint":
            case "region":
            case "bucket_name":
            case "path_style_access":
            case "username":
            case "access_key_id":
            case "secret_access_key":
                return true;
            default:
                return false;
        }
    }

    private static boolean isHdfsTypedKey(String key) {
        switch (key) {
            case "hdfs_namenodes":
            case "hdfs_port":
            case "hdfs_auth_method":
            case "krb_principal":
            case "krb_principal_keytab":
            case "hadoop_rpc_protection":
            case "data_transfer_protocol":
            case "is_ha_supported":
            case "dfs_nameservices":
            case "dfs_ha_namenodes":
            case "dfs_namenode_rpc_address":
            case "dfs_client_failover_proxy_provider":
            case "dfs_client_use_datanode_hostname":
            case "username":
                return true;
            default:
                return false;
        }
    }

    /**
     * Convert a {@link VolumeInfo}'s extra HDFS pass-through entries into a
     * fresh map prefixed by the failover-proxy-provider key when not already
     * supplied. This is used by the legacy {@code BaseConfigurationFactory}
     * path to preserve {@code transformHdfsHaConfig}'s default provider value.
     *
     * @param volume the loaded volume info; must not be null
     * @return a fresh map suitable for direct write into a Hadoop Configuration
     */
    public Map<String, String> hdfsFailoverDefaults(VolumeInfo volume) {
        Map<String, String> out = new HashMap<>();
        String ns = volume.getDfsNameservices();
        if (ns == null || ns.isEmpty()) {
            return out;
        }
        if (volume.getDfsClientFailoverProxyProvider() == null) {
            out.put("dfs.client.failover.proxy.provider." + ns,
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        }
        return out;
    }
}
