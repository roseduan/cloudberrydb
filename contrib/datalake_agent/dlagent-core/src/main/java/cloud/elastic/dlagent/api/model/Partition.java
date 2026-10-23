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

import cloud.elastic.dlagent.api.utilities.PartitionMetadata;
import lombok.Getter;
import lombok.Setter;


public class Partition {
    /**
     * File path+name, table name, etc.
     */
    @Getter
    private final String sourceName;

    /**
     * Partition metadata information (starting point + length, region location, etc.).
     */
    @Getter
    @Setter
    private PartitionMetadata metadata;

    /**
     * Profile name, recommended for reading given Partition.
     */
    @Getter
    @Setter
    private String profile;

    public Partition(PartitionMetadata metadata) {
        this(null, metadata, null);
    }

    /**
     * Constructs a Partition.
     *
     * @param sourceName the resource uri (File path+name, table name, etc.)
     */
    public Partition(String sourceName) {
        this(sourceName, null);
    }

    /**
     * Constructs a Partition.
     *
     * @param sourceName the resource uri (File path+name, table name, etc.)
     * @param metadata   the metadata for this Partition
     */
    public Partition(String sourceName,
                     PartitionMetadata metadata) {
        this(sourceName, metadata, null);
    }

    /**
     * Contructs a Partition.
     *
     * @param sourceName the resource uri (File path+name, table name, etc.)
     * @param metadata   the metadata for this Partition
     * @param profile    the profile to use for the query
     */
    public Partition(String sourceName,
                     PartitionMetadata metadata,
                     String profile) {
        this.sourceName = sourceName;
        this.metadata = metadata;
        this.profile = profile;
    }
}
