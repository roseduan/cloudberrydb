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

package cloud.elastic.dlagent.api.model.iceberg;

import cloud.elastic.dlagent.constants.IcebergConfigConstants;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * Top-level result of {@code IcebergRequestConfigParser.parse(...)}.
 *
 * <p>Holds the four typed sections plus the translated FileIO key map. Callers
 * that have not yet migrated to the typed form can use {@link #toFlatPropertiesMap()}
 * to obtain the legacy flat {@code Map<String, String>} shape consumed by
 * downstream {@code IcebergService} methods.
 */
@Getter
@Setter
@NoArgsConstructor
public class IcebergRequestConfig {

    private CatalogInfo catalog = new CatalogInfo();
    private VolumeInfo volume = new VolumeInfo();
    private AdditionalInfo additional = new AdditionalInfo();
    private GopherRuntimeConfig gopherRuntime = new GopherRuntimeConfig();

    /**
     * Translated FileIO key map ready for downstream consumers.
     *
     * <p>When gopher is enabled this contains {@code gopher.*} keys
     * (connection info translated from {@link #volume} plus runtime
     * params from {@link #gopherRuntime}). When gopher is disabled this
     * contains {@code s3.*} / {@code fs.s3a.*} / {@code client.region}
     * keys for iceberg-aws {@code ResolvingFileIO}.
     */
    private Map<String, String> fileIOProps = new HashMap<>();

    /** User-facing iceberg {@code TBLPROPERTIES} (already filtered of internal keys). */
    private Map<String, String> userTableProperties = new HashMap<>();

    private String icebergConfigVersion;
    private String setCatalogDefaultImpl;

    /** Comma-zero-separated list of site config file names; retained for dlproxy legacy callers. */
    private String configFiles;

    /**
     * Flatten this typed POJO back into the legacy {@code Map<String, String>} shape
     * historically consumed by {@code IcebergService} methods.
     *
     * <p>Each {@link CatalogInfo} field is emitted under the
     * {@code IcebergCatalogConfig.&lt;field&gt;} prefix, each {@link VolumeInfo} field
     * under {@code IcebergVolumeConfig.&lt;field&gt;}, each {@link AdditionalInfo}
     * field under {@code IcebergAdditionalConfig.&lt;field&gt;}. The translated
     * {@link #fileIOProps} entries (already prefixed with {@code gopher.}, {@code s3.}
     * etc.) and {@link #userTableProperties} are merged verbatim.
     *
     * @return a fresh map with the flattened representation
     */
    public Map<String, String> toFlatPropertiesMap() {
        Map<String, String> out = new HashMap<>();
        flattenCatalog(out);
        flattenVolume(out);
        flattenAdditional(out);
        out.putAll(fileIOProps);
        out.putAll(userTableProperties);
        if (configFiles != null) {
            out.put("config_files", configFiles);
        }
        return out;
    }

    private void flattenCatalog(Map<String, String> out) {
        String prefix = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + ".";
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE,            catalog.getServerType());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_NAME,            catalog.getServerName());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.HIVE_METASTORE_URI,     catalog.getHiveMetastoreUri());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.USERNAME,               catalog.getUsername());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.AUTH_METHOD,            catalog.getAuthMethod());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.KRB_SERVICE_PRINCIPAL,  catalog.getKrbServicePrincipal());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.KRB_CLIENT_PRINCIPAL,   catalog.getKrbClientPrincipal());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.KRB_CLIENT_KEYTAB,      catalog.getKrbClientKeytab());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CATALOG_NAME,           catalog.getCatalogName());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ENABLE_METADATA_CACHE,  toStringOrNull(catalog.getEnableMetadataCache()));
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.METADATA_CACHE_TTL,     toStringOrNull(catalog.getMetadataCacheTtl()));
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.AUTO_REFRESH_METADATA,  toStringOrNull(catalog.getAutoRefreshMetadata()));
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.WAREHOUSE_LOCATION_PERFIX, catalog.getWarehouseLocationPrefix());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_URL,     catalog.getPolarisServerUrl());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_ID,              catalog.getClientId());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_SECRET,          catalog.getClientSecret());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SCOPE,                  catalog.getScope());
        if (catalog.getExtraProperties() != null) {
            out.putAll(catalog.getExtraProperties());
        }
    }

    private void flattenVolume(Map<String, String> out) {
        String prefix = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + ".";
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE,  volume.getVolumeServerType());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SERVER_NAME,         volume.getServerName());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_ENDPOINT,     volume.getVolumeEndpoint());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_REGION,       volume.getVolumeRegion());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BUCKET_NAME,         volume.getBucketName());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.PATH_STYLE_ACCESS,   toStringOrNull(volume.getPathStyleAccess()));
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ACCESS_KEY_ID,       volume.getAccessKeyId());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SECRET_ACCESS_KEY,   volume.getSecretAccessKey());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.BASE_PATH,           volume.getBasePath());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ENABLE_CACHING,      toStringOrNull(volume.getEnableCaching()));
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ALLOW_WRITES,        toStringOrNull(volume.getAllowWrites()));
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.USERNAME,            volume.getUsername());
        if (volume.getExtraProperties() != null) {
            out.putAll(volume.getExtraProperties());
        }
    }

    private void flattenAdditional(Map<String, String> out) {
        String prefix = IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.ICEBERG_ADDITIONAL_CONFIG_STRING + ".";
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.TOTAL_SEGMENT,     additional.getTotalSegment());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.SPLIT_SIZE,        additional.getSplitSize());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.FILTER_STRING,     additional.getFilterString());
        putIfNotNull(out, prefix + IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.TABLE_IDENTIFIER,  additional.getTableIdentifier());
    }

    private static void putIfNotNull(Map<String, String> out, String key, String value) {
        if (value != null) {
            out.put(key, value);
        }
    }

    private static String toStringOrNull(Object value) {
        return value == null ? null : value.toString();
    }
}
