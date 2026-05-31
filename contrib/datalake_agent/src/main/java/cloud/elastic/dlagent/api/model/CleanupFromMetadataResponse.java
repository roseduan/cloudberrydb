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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Response body for POST /api/v1/files/cleanup-from-metadata.
 *
 * Returned with HTTP 200 even on partial failure -- per-file errors are
 * reported in the "failed" array.  The caller (datalake_fdw consumer) treats
 * a non-empty failed array as ERROR so the queue entry stays / is retried /
 * eventually moved to the DLQ.
 */
@Getter
@Setter
public class CleanupFromMetadataResponse {

    /** Number of files successfully deleted (including the root metadata.json). */
    private int deletedCount;

    /**
     * Per-file failures.  Each map has two keys: "path" and "error".  Empty
     * list means every referenced file was deleted cleanly.
     */
    private List<Map<String, String>> failed = new ArrayList<>();
}
