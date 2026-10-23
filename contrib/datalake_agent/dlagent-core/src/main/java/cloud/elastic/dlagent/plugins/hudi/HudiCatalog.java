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

package cloud.elastic.dlagent.plugins.hudi;

import cloud.elastic.dlagent.api.model.Metadata;
import cloud.elastic.dlagent.api.model.RequestContext;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.common.util.collection.Pair;

import java.util.List;

/**
 * Interface for Hudi catalogs. Only contains a minimal set of methods to make
 * it easy to add support for new Hudi catalogs. Methods that can be implemented in a
 * catalog-agnostic way should be placed in HudiUtil.
 */
public interface HudiCatalog {
    /**
     * Splits files returned by listStatus(JobConf) when they're too big
     */
    Pair<HudiTableOptions, List<CombineHudiSplit>> getSplits(Metadata.Item tableName,
                                                             RequestContext context) throws Exception;

    /**
     * Get the schema of given table
     */
    InternalSchema getSchema(Metadata.Item tableName) throws Exception;
}

