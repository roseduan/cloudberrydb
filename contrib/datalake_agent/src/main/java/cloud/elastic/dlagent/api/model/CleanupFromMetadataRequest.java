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

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * Request body for POST /api/v1/files/cleanup-from-metadata.
 *
 * Sent by the datalake_fdw autovacuum consumer after a DROP ICEBERG TABLE has
 * left an orphaned metadata.json behind.  The consumer reconstructs the
 * fileIOConfig from pg_foreign_volume / pg_foreign_server / pg_user_mapping
 * and posts the metadata path + config; the agent owns the actual S3/HDFS
 * deletion by walking the snapshot tree.
 */
@Getter
@Setter
public class CleanupFromMetadataRequest {

    /**
     * Absolute path of the root metadata.json (e.g. "s3a://bucket/.../metadata.json").
     */
    private String metadataPath;

    /**
     * Flat key/value map describing the storage configuration.  Recognized
     * "type" values: "s3", "hdfs", "abfss".  Field set depends on type; see
     * docs/datalake_fdw_iceberg_guide_zh.md §4.3 for the canonical list.
     */
    private Map<String, String> fileIOConfig;
}
