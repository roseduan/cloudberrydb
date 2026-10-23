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

import java.util.HashMap;
import java.util.Map;

/**
 * Volume (storage backend) connection info parsed from the Iceberg request body.
 *
 * <p>VolumeInfo is the <b>single source of truth</b> for connection information
 * such as endpoint, bucket, credentials and HDFS HA layout. Downstream FileIO
 * translators ({@code GopherPropertiesResolver} for the gopher path and
 * {@code S3FileIOPropertiesTransformer} for the iceberg-aws path) consume this
 * POJO and emit backend-specific key sets (gopher.* or s3.* / fs.s3a.*).
 *
 * <p>Sourced exclusively from one of:
 * <ul>
 *   <li>SQL OPTIONS via {@code IcebergVolumeConfig.*} when {@code server_name} is absent</li>
 *   <li>{@code s3.conf[server_name]} or {@code gphdfs.conf[server_name]} when {@code server_name} is given,
 *       picked by {@code volumeServerType}</li>
 * </ul>
 */
@Getter
@Setter
@NoArgsConstructor
public class VolumeInfo {

    // ---- Common fields -----------------------------------------------------

    /** s3 / s3v2 / hdfs / abfss / oss / cos / ... — verbatim user-facing protocol name. */
    private String volumeServerType;

    /** When set, VolumeInfo is loaded from a site file; otherwise from SQL OPTIONS. */
    private String serverName;

    // ---- Object-storage fields --------------------------------------------

    private String volumeEndpoint;
    private String volumeRegion;
    private String bucketName;
    private Boolean pathStyleAccess;
    private String accessKeyId;
    private String secretAccessKey;
    private String basePath;
    private Boolean enableCaching;
    private Boolean allowWrites;
    private String username;

    // ---- HDFS fields (HA + Kerberos) --------------------------------------

    private String hdfsNamenodeHost;
    private String hdfsNamenodePort;
    private Boolean isHaSupported;
    private String dfsNameservices;
    private String dfsHaNamenodes;
    private String dfsNamenodeRpcAddress;
    private String dfsClientFailoverProxyProvider;
    private Boolean dfsClientUseDatanodeHostname;

    private String hdfsAuthMethod;

    /** HDFS NameNode service principal (distinct from {@link CatalogInfo#krbServicePrincipal}). */
    private String krbPrincipal;
    private String krbPrincipalKeytab;

    private String hadoopRpcProtection;
    private String dataTransferProtocol;
    /** SASL QOP for DataNode transfers: authentication / integrity / privacy. */
    private String dataTransferProtection;

    /**
     * Pass-through bag for any extra keys loaded from a site file that do not
     * map to a typed field above (e.g. {@code dfs.*} entries kept verbatim for
     * Hadoop Configuration).
     */
    private Map<String, String> extraProperties = new HashMap<>();

    /**
     * Apply the user-facing {@code hdfs_namenodes} / {@code hdfs_port} option
     * pair onto the typed host/port fields. {@code hdfs_namenodes} carries
     * {@code host} or {@code host:port}; a port spliced into the value wins
     * over the separate {@code hdfs_port} option. HA deployments put the
     * nameservice name here (no port). Null inputs leave the fields untouched
     * so absent options stay mergeable.
     */
    public void applyHdfsNamenodes(String namenodes, String port) {
        if (namenodes != null && namenodes.indexOf(':') >= 0) {
            int idx = namenodes.lastIndexOf(':');
            this.hdfsNamenodeHost = namenodes.substring(0, idx);
            this.hdfsNamenodePort = namenodes.substring(idx + 1);
            return;
        }
        if (namenodes != null) {
            this.hdfsNamenodeHost = namenodes;
        }
        if (port != null) {
            this.hdfsNamenodePort = port;
        }
    }
}
