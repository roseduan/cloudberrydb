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

package cloud.elastic.dlagent.service.iceberg;

import cloud.elastic.dlagent.constants.IcebergConfigConstants;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The load-table response's config and credential halves have to serve the
 * same set of volume types.  They have drifted apart twice: first the config
 * half started treating s3v2 and abfss as the S3 family while the credential
 * half still returned early for anything that was not exactly "s3", which left
 * those volumes with an endpoint and no key to authenticate with -- a failure
 * that surfaces at authentication rather than at configuration and reads like
 * a storage problem.  These tests pin the symmetry rather than the wording.
 */
public class IcebergResolvedVolumeConfigTest {

    private static final String P =
        IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + ".";

    private static Map<String, String> volumeOf(String type) {
        Map<String, String> p = new HashMap<>();
        p.put(P + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_SERVER_TYPE, type);
        p.put(P + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_ENDPOINT, "http://minio:9000");
        p.put(P + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.VOLUME_REGION, "us-east-1");
        p.put(P + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ACCESS_KEY_ID, "admin");
        p.put(P + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.SECRET_ACCESS_KEY, "admin12345");
        p.put(P + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.HDFS_NAMENODES, "hadoop");
        return p;
    }

    /** Every S3-family type gets both halves: settings and the key to use them. */
    @Test
    public void s3FamilyGetsConfigAndCredentials() {
        for (String type : new String[] {"s3", "s3v2", "abfss"}) {
            Map<String, String> config = new HashMap<>();
            Map<String, String> creds = new HashMap<>();
            IcebergConfigurationBuilder.addResolvedVolumeConfig(config, volumeOf(type));
            IcebergConfigurationBuilder.addResolvedVolumeCredentials(creds, volumeOf(type));

            assertEquals("http://minio:9000", config.get("s3.endpoint"), type);
            assertEquals("us-east-1", config.get("s3.region"), type);
            /* The point of the test: settings without credentials is the bug. */
            assertEquals("admin", creds.get("s3.access-key-id"), type);
            assertEquals("admin12345", creds.get("s3.secret-access-key"), type);
        }
    }

    /** HDFS is the one exclusion, and it must be excluded from both halves. */
    @Test
    public void hdfsGetsNeitherS3ConfigNorS3Credentials() {
        Map<String, String> config = new HashMap<>();
        Map<String, String> creds = new HashMap<>();

        IcebergConfigurationBuilder.addResolvedVolumeConfig(config, volumeOf("hdfs"));
        IcebergConfigurationBuilder.addResolvedVolumeCredentials(creds, volumeOf("hdfs"));

        assertNull(config.get("s3.endpoint"));
        assertNull(config.get("s3.region"));
        assertNotNull(config.get("hdfs_namenodes"));
        /* No S3 credentials exist for HDFS; emitting them would be noise. */
        assertNull(creds.get("s3.access-key-id"));
        assertNull(creds.get("s3.secret-access-key"));
    }

    /** A value the FileIO already reported stays authoritative in both halves. */
    @Test
    public void existingEntriesAreNotOverwritten() {
        Map<String, String> config = new HashMap<>();
        Map<String, String> creds = new HashMap<>();
        config.put("s3.endpoint", "http://from-fileio:9000");
        creds.put("s3.access-key-id", "from-fileio");

        IcebergConfigurationBuilder.addResolvedVolumeConfig(config, volumeOf("s3"));
        IcebergConfigurationBuilder.addResolvedVolumeCredentials(creds, volumeOf("s3"));

        assertEquals("http://from-fileio:9000", config.get("s3.endpoint"));
        assertEquals("from-fileio", creds.get("s3.access-key-id"));
    }
}
