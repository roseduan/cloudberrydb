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

import cloud.elastic.dlagent.api.model.iceberg.GopherRuntimeConfig;
import cloud.elastic.dlagent.api.model.iceberg.VolumeInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Sole entry point that resolves the final {@code gopher.*} key map consumed by
 * {@code GopherFileIO} / {@code GopherFileSystem}.
 *
 * <p>Inputs:
 * <ol>
 *   <li>{@link GopherConfigurationProperties} baseline loaded from
 *       {@code application.properties} (process-wide defaults for socket paths,
 *       cache strategy, log levels, ...).</li>
 *   <li>Per-request {@link GopherRuntimeConfig} carrying runtime overrides
 *       parsed from {@code fileIOConfig.gopherConfig.common}.</li>
 *   <li>Per-request {@link VolumeInfo} carrying connection information
 *       (endpoint, bucket, credentials, region, HDFS HA layout, ...).</li>
 * </ol>
 *
 * <p>Output: a flat {@code Map<String, String>} whose keys are all prefixed with
 * {@value #GOPHER_PREFIX}. Connection fields are always derived from
 * {@link VolumeInfo}; the per-request {@link GopherRuntimeConfig} <b>must not</b>
 * carry connection info.
 *
 * <p>This class is the only place that performs gopher-side normalization
 * (endpoint scheme stripping, {@code useHttps} / {@code useVirtualHost}
 * derivation). The {@code volume_server_type} value is passed through to
 * {@code gopher.ufs_type} verbatim; the iceberg-gopher native client now
 * accepts {@code s3} / {@code s3v2} natively (since the s3a-only era), so
 * no on-the-fly rewriting is needed here. All other modules must consume
 * the output verbatim.
 */
@Component
@Slf4j
public class GopherPropertiesResolver {

    /** All gopher Hadoop Configuration keys are prefixed with this string. */
    public static final String GOPHER_PREFIX = "gopher.";

    /** Master switch key (also exposed via {@link GopherConfigurationProperties#getEnabled()}). */
    public static final String KEY_ENABLED = GOPHER_PREFIX + "enabled";

    // Runtime keys (populated from baseline + GopherRuntimeConfig).
    static final String KEY_WORKER_PATH         = GOPHER_PREFIX + "worker_path";
    static final String KEY_CONNECT_PATH        = GOPHER_PREFIX + "connect_path";
    static final String KEY_CONNECT_PLASMA_PATH = GOPHER_PREFIX + "connect_plasma_path";
    static final String KEY_CACHE_STRATEGY      = GOPHER_PREFIX + "cache_strategy";
    static final String KEY_LOG_LEVEL           = GOPHER_PREFIX + "log_level";
    static final String KEY_LIBOSS2_LOG_LEVEL   = GOPHER_PREFIX + "liboss2_log_level";

    // Object-storage connection keys (populated from VolumeInfo).
    static final String KEY_UFS_TYPE          = GOPHER_PREFIX + "ufs_type";
    static final String KEY_ENDPOINT          = GOPHER_PREFIX + "endpoint";
    static final String KEY_REGION            = GOPHER_PREFIX + "region";
    static final String KEY_BUCKET            = GOPHER_PREFIX + "bucket";
    static final String KEY_ACCESS_KEY        = GOPHER_PREFIX + "access_key";
    static final String KEY_SECRET_KEY        = GOPHER_PREFIX + "secret_key";
    static final String KEY_USE_HTTPS         = GOPHER_PREFIX + "useHttps";
    static final String KEY_USE_VIRTUAL_HOST  = GOPHER_PREFIX + "useVirtualHost";
    static final String KEY_PATH_STYLE_ACCESS = GOPHER_PREFIX + "path_style_access";

    // HDFS-specific keys (populated from VolumeInfo).
    static final String KEY_NAME_NODE                 = GOPHER_PREFIX + "name_node";
    static final String KEY_PORT                      = GOPHER_PREFIX + "port";
    static final String KEY_IS_HA_SUPPORTED           = GOPHER_PREFIX + "is_ha_supported";
    static final String KEY_DFS_NAMESERVICES          = GOPHER_PREFIX + "dfs_nameservices";
    static final String KEY_DFS_HA_NAMENODES          = GOPHER_PREFIX + "dfs_ha_namenodes";
    static final String KEY_DFS_NAMENODE_RPC_ADDRESS  = GOPHER_PREFIX + "dfs_namenode_rpc_address";
    static final String KEY_DFS_FAILOVER_PROXY        = GOPHER_PREFIX + "dfs_client_failover_proxy_provider";
    static final String KEY_DFS_USE_DATANODE_HOSTNAME = GOPHER_PREFIX + "dfs_client_use_datanode_hostname";
    static final String KEY_AUTH_METHOD               = GOPHER_PREFIX + "auth_method";
    static final String KEY_KRB_PRINCIPAL             = GOPHER_PREFIX + "krb_principal";
    static final String KEY_HADOOP_RPC_PROTECTION    = GOPHER_PREFIX + "hadoop_rpc_protection";
    static final String KEY_DATA_TRANSFER_PROTOCOL   = GOPHER_PREFIX + "data_transfer_protocol";

    private static final String UFS_TYPE_HDFS = "hdfs";

    private final GopherConfigurationProperties baseline;

    public GopherPropertiesResolver(GopherConfigurationProperties baseline) {
        this.baseline = baseline;
    }

    /**
     * Returns the baseline {@code gopher.*} map derived from
     * {@code application.properties}.
     *
     * @return a fresh mutable copy of the baseline; callers may modify freely
     */
    public Map<String, String> baseline() {
        return new HashMap<>(baseline.toGopherPropertiesMap());
    }

    /**
     * Whether gopher mode is enabled per the {@code application.properties} baseline.
     *
     * @return true if {@code gopher.enabled=true} in the baseline
     */
    public boolean isEnabled() {
        return Boolean.TRUE.equals(baseline.getEnabled());
    }

    /**
     * Whether gopher mode is enabled per the given Hadoop {@link Configuration}.
     *
     * <p>This is the post-{@code applyTo} variant used by downstream catalog
     * implementations that only hold a {@link Configuration} reference.
     *
     * @param configuration Hadoop Configuration that has already been populated
     * @return true if {@code gopher.enabled=true} in the configuration
     */
    public boolean isEnabled(Configuration configuration) {
        return "true".equalsIgnoreCase(configuration.get(KEY_ENABLED));
    }

    /**
     * Build the final {@code gopher.*} map from baseline + per-request inputs.
     *
     * <p>Composition order (later layers override earlier ones):
     * <ol>
     *   <li>baseline from {@code application.properties}</li>
     *   <li>per-request runtime overrides from {@link GopherRuntimeConfig}</li>
     *   <li>connection info translated from {@link VolumeInfo} (always overrides)</li>
     * </ol>
     *
     * @param volume  per-request volume info; may be null in degenerate cases
     * @param runtime per-request runtime overrides; may be null
     * @return a fresh mutable map of fully normalized {@code gopher.*} entries
     */
    public Map<String, String> build(VolumeInfo volume, GopherRuntimeConfig runtime) {
        Map<String, String> props = baseline();
        applyRuntimeOverrides(props, runtime);
        applyVolumeConnection(props, volume);
        return props;
    }

    /**
     * Write the resolved {@code gopher.*} map into the given Hadoop
     * {@link Configuration}.
     *
     * @param configuration target configuration (modified in place)
     * @param gopherProps   resolved gopher properties; ignored if null or empty
     */
    public void applyTo(Configuration configuration, Map<String, String> gopherProps) {
        if (configuration == null || gopherProps == null || gopherProps.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : gopherProps.entrySet()) {
            if (entry.getValue() != null) {
                configuration.set(entry.getKey(), entry.getValue());
            }
        }
    }

    // ---- helpers ----------------------------------------------------------

    private void applyRuntimeOverrides(Map<String, String> props, GopherRuntimeConfig runtime) {
        if (runtime == null) {
            return;
        }
        putIfNotBlank(props, KEY_WORKER_PATH,         runtime.getWorkerPath());
        putIfNotBlank(props, KEY_CONNECT_PATH,        runtime.getConnectPath());
        putIfNotBlank(props, KEY_CONNECT_PLASMA_PATH, runtime.getConnectPlasmaPath());
        putIfNotBlank(props, KEY_CACHE_STRATEGY,      runtime.getCacheStrategy());
        putIfNotBlank(props, KEY_LOG_LEVEL,           runtime.getLogLevel());
        putIfNotBlank(props, KEY_LIBOSS2_LOG_LEVEL,   runtime.getLiboss2LogSeverity());
    }

    private void applyVolumeConnection(Map<String, String> props, VolumeInfo volume) {
        if (volume == null) {
            return;
        }
        String volumeType = volume.getVolumeServerType();
        if (UFS_TYPE_HDFS.equalsIgnoreCase(volumeType)) {
            applyHdfsConnection(props, volume);
        } else {
            applyObjectStorageConnection(props, volume);
        }
    }

    private void applyObjectStorageConnection(Map<String, String> props, VolumeInfo volume) {
        // Pass volume_server_type through verbatim. The iceberg-gopher native
        // client accepts s3 / s3v2 (and the legacy s3a / s3av2) directly.
        putIfNotBlank(props, KEY_UFS_TYPE, volume.getVolumeServerType());
        putIfNotBlank(props, KEY_BUCKET,        volume.getBucketName());
        putIfNotBlank(props, KEY_REGION,        volume.getVolumeRegion());
        putIfNotBlank(props, KEY_ACCESS_KEY,    volume.getAccessKeyId());
        putIfNotBlank(props, KEY_SECRET_KEY,    volume.getSecretAccessKey());

        // endpoint + derived useHttps: strip scheme so native OSS clients
        // (LIBOSS2) get a bare host[:port]; otherwise DNS resolves "http".
        String endpoint = volume.getVolumeEndpoint();
        if (endpoint != null && !endpoint.isEmpty()) {
            boolean isHttps = endpoint.startsWith("https://");
            String hostPort = endpoint.replaceFirst("^https?://", "");
            props.put(KEY_ENDPOINT,  hostPort);
            props.put(KEY_USE_HTTPS, Boolean.toString(isHttps));
        }

        // useVirtualHost is the negation of path_style_access; we also keep the
        // raw path_style_access key for downstream code that reads it directly.
        Boolean pathStyle = volume.getPathStyleAccess();
        if (pathStyle != null) {
            props.put(KEY_PATH_STYLE_ACCESS, Boolean.toString(pathStyle));
            props.put(KEY_USE_VIRTUAL_HOST,  Boolean.toString(!pathStyle));
        }
    }

    private void applyHdfsConnection(Map<String, String> props, VolumeInfo volume) {
        props.put(KEY_UFS_TYPE, UFS_TYPE_HDFS);

        putIfNotBlank(props, KEY_NAME_NODE,                volume.getHdfsNamenodeHost());
        putIfNotBlank(props, KEY_PORT,                     volume.getHdfsNamenodePort());
        putIfNotBlank(props, KEY_AUTH_METHOD,              volume.getHdfsAuthMethod());
        putIfNotBlank(props, KEY_KRB_PRINCIPAL,            volume.getKrbPrincipal());
        putIfNotBlank(props, KEY_HADOOP_RPC_PROTECTION,    volume.getHadoopRpcProtection());
        putIfNotBlank(props, KEY_DATA_TRANSFER_PROTOCOL,   volume.getDataTransferProtocol());

        Boolean haSupported = volume.getIsHaSupported();
        if (haSupported != null) {
            props.put(KEY_IS_HA_SUPPORTED, Boolean.toString(haSupported));
        }
        putIfNotBlank(props, KEY_DFS_NAMESERVICES,         volume.getDfsNameservices());
        putIfNotBlank(props, KEY_DFS_HA_NAMENODES,         volume.getDfsHaNamenodes());
        putIfNotBlank(props, KEY_DFS_NAMENODE_RPC_ADDRESS, volume.getDfsNamenodeRpcAddress());
        putIfNotBlank(props, KEY_DFS_FAILOVER_PROXY,       volume.getDfsClientFailoverProxyProvider());
        Boolean useHostname = volume.getDfsClientUseDatanodeHostname();
        if (useHostname != null) {
            props.put(KEY_DFS_USE_DATANODE_HOSTNAME, Boolean.toString(useHostname));
        }
    }

    private static void putIfNotBlank(Map<String, String> props, String key, String value) {
        if (value != null && !value.isEmpty()) {
            props.put(key, value);
        }
    }
}
