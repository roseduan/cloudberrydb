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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Single source of truth for loading {@code gphive.conf} / {@code s3.conf} /
 * {@code gphdfs.conf} YAML site configuration files.
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
 * {@code server_name} is explicitly supplied by the FDW.
 */
@Component
@Slf4j
public class SiteConfigLoader {

    static final String FILE_GPHIVE = "gphive.conf";
    static final String FILE_S3     = "s3.conf";
    static final String FILE_GPHDFS = "gphdfs.conf";

    /**
     * Load and translate the {@code gphive.conf[serverName]} section into a
     * fresh {@link CatalogInfo}.
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
        info.setServerName(catalogServerName);
        info.setHiveMetastoreUri(asString(serverMap.get("uris")));
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
     * {@link VolumeInfo}.
     *
     * <p>The {@code location} argument is used to derive the bucket name (the
     * S3 location URL such as {@code s3a://bucket/prefix}); when blank, no
     * bucket is extracted but other YAML fields are still populated.
     *
     * @param volumeServerName server name from {@code IcebergVolumeConfig.server_name};
     *                         when null or empty, an empty {@link VolumeInfo} is returned
     *                         and no file IO is performed
     * @param location         optional S3 URL whose bucket portion populates
     *                         {@link VolumeInfo#getBucketName()}
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
        info.setServerName(volumeServerName);
        info.setVolumeServerType(asString(serverMap.get("fs.gopher.ufs_type")));
        info.setVolumeEndpoint(asString(serverMap.get("fs.s3a.endpoint")));
        info.setAccessKeyId(asString(serverMap.get("fs.s3a.access.key")));
        info.setSecretAccessKey(asString(serverMap.get("fs.s3a.secret.key")));
        info.setVolumeRegion(asString(serverMap.get("fs.s3a.endpoint.region")));
        if (serverMap.get("fs.s3a.path.style.access") != null) {
            info.setPathStyleAccess(parseBoolean(serverMap.get("fs.s3a.path.style.access")));
        }

        // Derive bucket from location (e.g. "s3a://mybucket/path" -> "mybucket").
        if (location != null && !location.isEmpty()) {
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
     * fresh {@link VolumeInfo}.
     *
     * <p>Populates HDFS-specific fields (namenode host/port, HA layout, Kerberos
     * principal, RPC protection). The object-storage fields on the returned
     * VolumeInfo are deliberately left null.
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
        info.setServerName(volumeServerName);
        info.setVolumeServerType("hdfs");
        info.setHdfsNamenodeHost(asString(serverMap.get("hdfs_namenode_host")));
        info.setHdfsNamenodePort(asString(serverMap.get("hdfs_namenode_port")));
        info.setHdfsAuthMethod(asString(serverMap.get("hdfs_auth_method")));
        info.setKrbPrincipal(asString(serverMap.get("krb_principal")));
        info.setKrbPrincipalKeytab(asString(serverMap.get("krb_principal_keytab")));
        info.setHadoopRpcProtection(asString(serverMap.get("hadoop_rpc_protection")));
        info.setDataTransferProtocol(asString(serverMap.get("data_transfer_protocol")));

        Object haFlag = serverMap.get("is_ha_supported");
        if (haFlag != null) {
            info.setIsHaSupported(parseBoolean(haFlag));
        }
        info.setDfsNameservices(asString(serverMap.get("dfs.nameservices")));
        info.setDfsHaNamenodes(asString(serverMap.get("dfs.ha.namenodes")));
        info.setDfsNamenodeRpcAddress(asString(serverMap.get("dfs.namenode.rpc-address")));

        Object useHostname = serverMap.get("dfs_client_use_datanode_hostname");
        if (useHostname != null) {
            info.setDfsClientUseDatanodeHostname(parseBoolean(useHostname));
        }
        if (info.getDfsNameservices() != null) {
            info.setDfsClientFailoverProxyProvider(asString(
                    serverMap.get("dfs.client.failover.proxy.provider." + info.getDfsNameservices())));
        }

        // Keep all remaining keys (e.g. dfs.* overrides) as pass-through extras.
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
        try (InputStream stream = new FileInputStream(configFile)) {
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
        return "uris".equals(key)
                || "auth_method".equals(key)
                || "krb_service_principal".equals(key)
                || "krb_client_principal".equals(key)
                || "krb_client_keytab".equals(key);
    }

    private static boolean isS3TypedKey(String key) {
        return "fs.gopher.ufs_type".equals(key)
                || "fs.s3a.endpoint".equals(key)
                || "fs.s3a.access.key".equals(key)
                || "fs.s3a.secret.key".equals(key)
                || "fs.s3a.endpoint.region".equals(key)
                || "fs.s3a.path.style.access".equals(key);
    }

    private static boolean isHdfsTypedKey(String key) {
        switch (key) {
            case "hdfs_namenode_host":
            case "hdfs_namenode_port":
            case "hdfs_auth_method":
            case "krb_principal":
            case "krb_principal_keytab":
            case "hadoop_rpc_protection":
            case "data_transfer_protocol":
            case "is_ha_supported":
            case "dfs.nameservices":
            case "dfs.ha.namenodes":
            case "dfs.namenode.rpc-address":
            case "dfs_client_use_datanode_hostname":
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
