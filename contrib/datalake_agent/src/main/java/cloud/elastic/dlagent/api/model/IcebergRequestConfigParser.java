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

import cloud.elastic.dlagent.api.configuration.GopherPropertiesResolver;
import cloud.elastic.dlagent.api.configuration.S3FileIOPropertiesTransformer;
import cloud.elastic.dlagent.api.configuration.SiteConfigLoader;
import cloud.elastic.dlagent.api.model.iceberg.AdditionalInfo;
import cloud.elastic.dlagent.api.model.iceberg.CatalogInfo;
import cloud.elastic.dlagent.api.model.iceberg.GopherRuntimeConfig;
import cloud.elastic.dlagent.api.model.iceberg.IcebergRequestConfig;
import cloud.elastic.dlagent.api.model.iceberg.VolumeInfo;
import cloud.elastic.dlagent.constants.IcebergConfigConstants;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Sole entry point that parses an Iceberg REST request body (and the same
 * structure carried by dlproxy legacy clients via
 * {@code RequestContext.icebergConfigJsonString}) into a typed
 * {@link IcebergRequestConfig}.
 *
 * <p>Pipeline:
 * <ol>
 *   <li><b>Phase 1</b>: read {@code IcebergCatalogConfig.server_name} and
 *       {@code IcebergVolumeConfig.server_name} from the request.</li>
 *   <li><b>Phase 2</b>: <em>mutually exclusive</em> source selection based on
 *       the two server names — when set, the catalog/volume info comes
 *       entirely from the corresponding site file (gphive.conf / s3.conf /
 *       gphdfs.conf); otherwise from SQL OPTIONS.</li>
 *   <li><b>Phase 3</b>: parse {@code IcebergAdditionalConfig} and the gopher
 *       runtime block ({@code fileIOConfig.gopherConfig.common}).</li>
 *   <li><b>Phase 4</b>: branch on the {@code gopher.enabled} baseline:
 *       gopher path delegates to {@link GopherPropertiesResolver}; non-gopher
 *       path delegates to {@link S3FileIOPropertiesTransformer}.</li>
 * </ol>
 *
 * <p>No other class in the codebase is permitted to parse the Iceberg request
 * body. {@code IcebergRestController} and the dlproxy shim inside
 * {@code BaseConfigurationFactory} both call {@link #parse(Map)}.
 */
@Component
@Slf4j
public class IcebergRequestConfigParser {

    private final SiteConfigLoader siteConfigLoader;
    private final GopherPropertiesResolver gopherResolver;
    private final S3FileIOPropertiesTransformer s3Transformer;

    public IcebergRequestConfigParser(SiteConfigLoader siteConfigLoader,
                                      GopherPropertiesResolver gopherResolver,
                                      S3FileIOPropertiesTransformer s3Transformer) {
        this.siteConfigLoader = siteConfigLoader;
        this.gopherResolver = gopherResolver;
        this.s3Transformer = s3Transformer;
    }

    /**
     * Parse the given request body into a typed {@link IcebergRequestConfig}.
     *
     * @param request raw request body as deserialized from JSON (may be null)
     * @return typed config; never null
     */
    public IcebergRequestConfig parse(Map<String, Object> request) {
        IcebergRequestConfig out = new IcebergRequestConfig();
        if (request == null) {
            return out;
        }

        Map<String, Object> icebergConfig = mapOrNull(request.get(IcebergConfigConstants.ICEBERG_CONFIG));

        CatalogInfo catalog = resolveCatalog(icebergConfig);
        VolumeInfo  volume  = resolveVolume(icebergConfig, locationOf(request));
        AdditionalInfo additional = parseAdditional(icebergConfig);
        GopherRuntimeConfig runtime = parseGopherRuntime(icebergConfig);

        Map<String, String> fileIOProps = gopherResolver.isEnabled()
                ? gopherResolver.build(volume, runtime)
                : s3Transformer.build(volume);

        out.setCatalog(catalog);
        out.setVolume(volume);
        out.setAdditional(additional);
        out.setGopherRuntime(runtime);
        out.setFileIOProps(fileIOProps);
        out.setUserTableProperties(parseUserTableProperties(request));
        out.setIcebergConfigVersion(stringOrNull(maybeNested(icebergConfig, "iceberg_config_version")));
        out.setSetCatalogDefaultImpl(stringOrNull(maybeNested(icebergConfig, "set_catalog_default_impl")));
        if (icebergConfig != null && icebergConfig.containsKey("config_files")) {
            out.setConfigFiles(icebergConfig.get("config_files").toString());
        }
        return out;
    }

    /**
     * Extract ONLY user-facing Iceberg {@code TBLPROPERTIES} from the request
     * body — never catalog/volume/gopher runtime configuration.
     *
     * <p>Mirrors the open-source Iceberg separation between
     * {@code Catalog.initialize(catalogProps)} (runtime-only) and
     * {@code Catalog.createTable(..., tableProps)} (persisted into
     * metadata.json). The C side piggy-backs {@code buildInCatalog.*} plumbing
     * keys into the same JSON {@code properties} field, so we filter those out
     * via {@link IcebergUtilities#isInternalConfigKey(String)}.
     *
     * @param request raw request body (may be null)
     * @return a fresh map with only user-facing TBLPROPERTIES; never null
     */
    public Map<String, String> parseUserTableProperties(Map<String, Object> request) {
        Map<String, String> out = new HashMap<>();
        if (request == null) {
            return out;
        }
        Map<String, Object> userProps = mapOrNull(request.get(IcebergConfigConstants.PROPERTIES));
        if (userProps == null) {
            return out;
        }
        for (Map.Entry<String, Object> entry : userProps.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            if (IcebergUtilities.isInternalConfigKey(entry.getKey())) {
                continue;
            }
            out.put(entry.getKey(), entry.getValue().toString());
        }
        return out;
    }

    // ---- Phase 2: server_name XOR SQL OPTIONS -----------------------------

    private CatalogInfo resolveCatalog(Map<String, Object> icebergConfig) {
        Map<String, Object> body = catalogSection(icebergConfig);
        String serverName = stringOrNull(maybeNested(body, IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_NAME));
        if (serverName != null && !serverName.isEmpty()) {
            return siteConfigLoader.loadHiveSite(serverName);
        }
        return parseCatalogFromBody(body);
    }

    private VolumeInfo resolveVolume(Map<String, Object> icebergConfig, String location) {
        Map<String, Object> body = volumeSection(icebergConfig);
        String serverName = stringOrNull(maybeNested(body, IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SERVER_NAME));
        String volumeType = stringOrNull(maybeNested(body, IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE));
        if (serverName != null && !serverName.isEmpty()) {
            return "hdfs".equalsIgnoreCase(volumeType)
                    ? siteConfigLoader.loadHdfsSite(serverName)
                    : siteConfigLoader.loadS3Site(serverName, location);
        }
        return parseVolumeFromBody(body);
    }

    // ---- Phase 3: typed parse of remaining sections -----------------------

    private CatalogInfo parseCatalogFromBody(Map<String, Object> body) {
        CatalogInfo info = new CatalogInfo();
        if (body == null) {
            return info;
        }
        info.setServerType(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE)));
        info.setServerName(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_NAME)));
        info.setHiveMetastoreUri(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.HIVE_METASTORE_URI)));
        info.setUsername(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.USERNAME)));
        info.setAuthMethod(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.AUTH_METHOD)));
        info.setKrbServicePrincipal(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.KRB_SERVICE_PRINCIPAL)));
        info.setKrbClientPrincipal(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.KRB_CLIENT_PRINCIPAL)));
        info.setKrbClientKeytab(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.KRB_CLIENT_KEYTAB)));
        info.setCatalogName(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CATALOG_NAME)));
        info.setEnableMetadataCache(booleanOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ENABLE_METADATA_CACHE)));
        info.setMetadataCacheTtl(integerOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.METADATA_CACHE_TTL)));
        info.setAutoRefreshMetadata(booleanOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.AUTO_REFRESH_METADATA)));
        info.setWarehouseLocationPrefix(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.WAREHOUSE_LOCATION_PERFIX)));
        info.setPolarisServerUrl(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_URL)));
        info.setPolarisServerRealm(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_REALM)));
        info.setClientId(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_ID)));
        info.setClientSecret(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_SECRET)));
        info.setScope(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SCOPE)));
        return info;
    }

    private VolumeInfo parseVolumeFromBody(Map<String, Object> body) {
        VolumeInfo info = new VolumeInfo();
        if (body == null) {
            return info;
        }
        info.setVolumeServerType(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE)));
        info.setServerName(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SERVER_NAME)));
        info.setVolumeEndpoint(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_ENDPOINT)));
        info.setVolumeRegion(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_REGION)));
        info.setBucketName(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BUCKET_NAME)));
        info.setPathStyleAccess(booleanOrNull(body.get(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.PATH_STYLE_ACCESS)));
        info.setAccessKeyId(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ACCESS_KEY_ID)));
        info.setSecretAccessKey(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SECRET_ACCESS_KEY)));

        /* Diagnostic trace: see iceberg_volume_option.c step2 for context. */
        log.debug("[trace_ak] step4 parseVolBody: access_key_id len={}, secret_access_key len={}, body_keys={}",
            info.getAccessKeyId() == null ? -1 : info.getAccessKeyId().length(),
            info.getSecretAccessKey() == null ? -1 : info.getSecretAccessKey().length(),
            body.keySet());

        info.setBasePath(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BASE_PATH)));
        info.setEnableCaching(booleanOrNull(body.get(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ENABLE_CACHING)));
        info.setAllowWrites(booleanOrNull(body.get(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ALLOW_WRITES)));
        info.setUsername(stringOrNull(body.get(IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.USERNAME)));
        return info;
    }

    private AdditionalInfo parseAdditional(Map<String, Object> icebergConfig) {
        AdditionalInfo info = new AdditionalInfo();
        Map<String, Object> additional = additionalSection(icebergConfig);
        if (additional == null) {
            return info;
        }
        info.setTotalSegment(stringOrNull(additional.get(IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.TOTAL_SEGMENT)));
        info.setSplitSize(stringOrNull(additional.get(IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.SPLIT_SIZE)));
        info.setFilterString(stringOrNull(additional.get(IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.FILTER_STRING)));
        info.setTableIdentifier(stringOrNull(additional.get(IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.TABLE_IDENTIFIER)));
        return info;
    }

    /**
     * Extract the gopher runtime block. Tolerates both the new flat shape
     * ({@code fileIOConfig.gopherConfig.common}) and the legacy
     * {@code gopherFileIOConfig} wrapper still emitted by older C clients,
     * so a single agent can run with mixed C-side versions during rollout.
     */
    @SuppressWarnings("unchecked")
    private GopherRuntimeConfig parseGopherRuntime(Map<String, Object> icebergConfig) {
        GopherRuntimeConfig runtime = new GopherRuntimeConfig();
        Map<String, Object> additional = additionalSection(icebergConfig);
        if (additional == null) {
            return runtime;
        }
        Object fileIORaw = additional.get(IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.FILE_IO_CONFIG);
        if (!(fileIORaw instanceof Map)) {
            return runtime;
        }
        Map<String, Object> fileIO = (Map<String, Object>) fileIORaw;

        // Preferred (new) shape: fileIOConfig.gopherConfig.common
        Map<String, Object> gopherConfig = mapOrNull(fileIO.get(
                IcebergConfigConstants.GOPHER_CONFIG.GOPHER_CONFIG_STRING));
        if (gopherConfig == null) {
            // Legacy shape: fileIOConfig.gopherFileIOConfig.gopherConfig
            Map<String, Object> wrapper = mapOrNull(fileIO.get(
                    IcebergConfigConstants.FILE_IO_CONFIG.GOPHER_FILEIO_CONFIG));
            if (wrapper != null) {
                gopherConfig = mapOrNull(wrapper.get(
                        IcebergConfigConstants.GOPHER_CONFIG.GOPHER_CONFIG_STRING));
            }
        }
        if (gopherConfig == null) {
            return runtime;
        }
        Map<String, Object> common = mapOrNull(gopherConfig.get(IcebergConfigConstants.COMMON));
        if (common == null) {
            return runtime;
        }
        runtime.setWorkerPath(stringOrNull(common.get(IcebergConfigConstants.GOPHER_CONFIG.WORKER_PATH)));
        runtime.setConnectPath(stringOrNull(common.get(IcebergConfigConstants.GOPHER_CONFIG.CONNECT_PATH)));
        runtime.setConnectPlasmaPath(stringOrNull(common.get(IcebergConfigConstants.GOPHER_CONFIG.CONNECT_PLASMA_PATH)));
        runtime.setCacheStrategy(stringOrNull(common.get(IcebergConfigConstants.GOPHER_CONFIG.CACHE_STRATEGY)));
        runtime.setLogLevel(stringOrNull(common.get(IcebergConfigConstants.GOPHER_CONFIG.LOG_LEVEL)));
        runtime.setLiboss2LogSeverity(stringOrNull(common.get(IcebergConfigConstants.GOPHER_CONFIG.LIBOSS2_LOG_SEVERITY)));
        return runtime;
    }

    // ---- section accessors ------------------------------------------------

    @SuppressWarnings("unchecked")
    private Map<String, Object> catalogSection(Map<String, Object> icebergConfig) {
        if (icebergConfig == null) {
            return null;
        }
        return (Map<String, Object>) icebergConfig.get(
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> volumeSection(Map<String, Object> icebergConfig) {
        if (icebergConfig == null) {
            return null;
        }
        return (Map<String, Object>) icebergConfig.get(
                IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> additionalSection(Map<String, Object> icebergConfig) {
        if (icebergConfig == null) {
            return null;
        }
        return (Map<String, Object>) icebergConfig.get(
                IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.ICEBERG_ADDITIONAL_CONFIG_STRING);
    }

    // ---- conversion utilities --------------------------------------------

    private static String locationOf(Map<String, Object> request) {
        Object loc = request.get("location");
        return loc == null ? "" : loc.toString();
    }

    private static String stringOrNull(Object value) {
        return value == null ? null : value.toString();
    }

    private static Boolean booleanOrNull(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private static Integer integerOrNull(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapOrNull(Object value) {
        return value instanceof Map ? (Map<String, Object>) value : null;
    }

    private static Object maybeNested(Map<String, Object> map, String key) {
        return map == null ? null : map.get(key);
    }
}
