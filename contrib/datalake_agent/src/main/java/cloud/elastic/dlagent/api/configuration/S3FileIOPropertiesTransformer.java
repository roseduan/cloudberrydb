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

import cloud.elastic.dlagent.api.model.iceberg.VolumeInfo;
import cloud.elastic.dlagent.constants.IcebergConfigConstants;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Translates a {@link VolumeInfo} into the {@code s3.*} / {@code fs.s3a.*} /
 * {@code client.region} key set consumed by iceberg-aws {@code S3FileIO} via
 * {@code ResolvingFileIO} on the non-gopher path
 * ({@code gopher.enabled=false}).
 *
 * <p>This complements {@link GopherPropertiesResolver}: both classes take the
 * same {@link VolumeInfo} as input but emit different key namespaces so that
 * downstream FileIO implementations consume what they understand natively.
 */
@Component
public class S3FileIOPropertiesTransformer {

    /**
     * Translate the given {@link VolumeInfo} into iceberg-aws S3FileIO keys.
     *
     * @param volume volume info (may be null; returns empty map then)
     * @return a fresh mutable map of {@code s3.*} / {@code fs.s3a.*} /
     *         {@code client.region} entries; never null
     */
    public Map<String, String> build(VolumeInfo volume) {
        Map<String, String> props = new HashMap<>();
        if (volume == null) {
            return props;
        }

        // Iceberg-aws S3FileIO (catalog-property side) keys.
        putIfNotBlank(props, IcebergConfigConstants.S3FILEIO_ACCESS_KEY_ID,     volume.getAccessKeyId());
        putIfNotBlank(props, IcebergConfigConstants.S3FILEIO_SECRET_ACCESS_KEY, volume.getSecretAccessKey());
        putIfNotBlank(props, IcebergConfigConstants.S3FILEIO_ENDPOINT,          volume.getVolumeEndpoint());
        putIfNotBlank(props, IcebergConfigConstants.S3FILEIO_REGION,            volume.getVolumeRegion());
        if (volume.getPathStyleAccess() != null) {
            props.put(IcebergConfigConstants.S3FILEIO_PATH_STYLE_ACCESS,
                    Boolean.toString(volume.getPathStyleAccess()));
        }

        // AwsClientProperties reads the SDK client region from "client.region",
        // not "s3.region"; without this the SDK falls back to its region
        // provider chain and frequently fails with "Unable to load region".
        String region = volume.getVolumeRegion();
        if (region != null && !region.isEmpty()) {
            props.put("client.region", region);
        }

        // Hadoop S3A filesystem keys, used by HadoopFileIO for non-s3:// schemes
        // or when explicit fs.s3a.* access is required. Mirrors the legacy
        // BaseConfigurationFactory.transformS3Config behavior for non-gopher mode.
        putIfNotBlank(props, IcebergConfigConstants.FS_S3A_ACCESS_KEY,    volume.getAccessKeyId());
        putIfNotBlank(props, IcebergConfigConstants.FS_S3A_SECRET_KEY,   volume.getSecretAccessKey());
        putIfNotBlank(props, IcebergConfigConstants.FS_S3A_ENDPOINT,     volume.getVolumeEndpoint());
        // Pin fs.s3a.endpoint.region alongside the endpoint: without it
        // hadoop-aws 3.4.0 wraps the SDK client in S3CrossRegionSyncClient
        // (HADOOP-18908), whose region probing bypasses the custom endpoint
        // and times out on hosts without AWS egress.
        putIfNotBlank(props, IcebergConfigConstants.FS_S3A_ENDPOINT_REGION, volume.getVolumeRegion());
        if (volume.getPathStyleAccess() != null) {
            props.put(IcebergConfigConstants.FS_S3A_PATH_STYLE_ACCESS,
                    Boolean.toString(volume.getPathStyleAccess()));
        }

        return props;
    }

    private static void putIfNotBlank(Map<String, String> props, String key, String value) {
        if (value != null && !value.isEmpty()) {
            props.put(key, value);
        }
    }
}
