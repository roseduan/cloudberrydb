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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3FileIOPropertiesTransformerTest {

    private final S3FileIOPropertiesTransformer transformer = new S3FileIOPropertiesTransformer();

    @Test
    public void build_nullVolumeReturnsEmptyMap() {
        assertTrue(transformer.build(null).isEmpty());
    }

    @Test
    public void build_populatesS3AndFsS3aAndClientRegion() {
        VolumeInfo volume = new VolumeInfo();
        volume.setAccessKeyId("ak");
        volume.setSecretAccessKey("sk");
        volume.setVolumeEndpoint("http://s3.example.com");
        volume.setVolumeRegion("us-west-2");
        volume.setPathStyleAccess(true);

        Map<String, String> out = transformer.build(volume);

        // s3.* keys (iceberg-aws S3FileIO)
        assertEquals("ak", out.get("s3.access-key-id"));
        assertEquals("sk", out.get("s3.secret-access-key"));
        assertEquals("http://s3.example.com", out.get("s3.endpoint"));
        assertEquals("us-west-2", out.get("s3.region"));
        assertEquals("true", out.get("s3.path-style-access"));

        // client.region (AwsClientProperties)
        assertEquals("us-west-2", out.get("client.region"));

        // fs.s3a.* keys (HadoopFileIO for fs.s3a:// scheme access)
        assertEquals("ak", out.get("fs.s3a.access.key"));
        assertEquals("sk", out.get("fs.s3a.secret.key"));
        assertEquals("http://s3.example.com", out.get("fs.s3a.endpoint"));
        assertEquals("true", out.get("fs.s3a.path.style.access"));

        // gopher.* must not leak in
        assertNull(out.get("gopher.endpoint"));
        assertNull(out.get("gopher.ufs_type"));
    }

    @Test
    public void build_emptyValuesAreSkipped() {
        VolumeInfo volume = new VolumeInfo();
        volume.setAccessKeyId("");
        volume.setSecretAccessKey(null);
        Map<String, String> out = transformer.build(volume);
        assertTrue(out.isEmpty());
    }

    @Test
    public void build_omitsClientRegionWhenRegionMissing() {
        VolumeInfo volume = new VolumeInfo();
        volume.setAccessKeyId("ak");
        Map<String, String> out = transformer.build(volume);
        assertNull(out.get("client.region"));
        assertNull(out.get("s3.region"));
    }
}
