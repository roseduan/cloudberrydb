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

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Gopher runtime parameters parsed from {@code fileIOConfig.gopherConfig.common}.
 *
 * <p>This POJO holds <b>only</b> gopher process-level runtime settings (socket
 * paths, log levels, cache strategy). Connection information (endpoint, bucket,
 * credentials, region, etc.) <b>must not</b> be placed here; it belongs on
 * {@link VolumeInfo} and is translated to {@code gopher.*} keys by the
 * {@code GopherPropertiesResolver}.
 *
 * <p>Per-field semantics map to the {@code GopherCommonConfig} schema in
 * {@code iceberg-openapi.yaml}.
 */
@Getter
@Setter
@NoArgsConstructor
public class GopherRuntimeConfig {

    private String workerPath;
    private String connectPath;
    private String connectPlasmaPath;
    private String cacheStrategy;
    private String logLevel;
    private String liboss2LogSeverity;
}
