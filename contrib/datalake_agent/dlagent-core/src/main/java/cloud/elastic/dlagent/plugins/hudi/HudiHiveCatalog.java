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
import cloud.elastic.dlagent.api.security.SecureLogin;
import cloud.elastic.dlagent.plugins.hive.utilities.DlCachedClientPool;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Implementation of HudiCatalog for tables stored in HiveCatalog.
 */
public class HudiHiveCatalog extends HudiBaseCatalog implements HudiCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(HudiHiveCatalog.class);

    private final DlCachedClientPool hiveClients;
    private final boolean isPartitionTable;

    public HudiHiveCatalog(HoodieTableMetaClient metaClient, DlCachedClientPool hiveClients, SecureLogin secureLogin, boolean isPartitionTable) {
        super(metaClient, secureLogin);
        this.hiveClients = hiveClients;
        this.isPartitionTable = isPartitionTable;
    }

    @Override
    public Pair<HudiTableOptions, List<CombineHudiSplit>> getSplits(Metadata.Item tableName, RequestContext context) throws Exception {
        return buildInputSplits(tableName, context);
    }

    @Override
    public InternalSchema getSchema(Metadata.Item tableName) throws Exception {
        return getTableSchema();
    }

    @Override
    public HudiFileIndex createFileIndex(HudiPartitionPruner.PartitionPruner partitionPruner,
                                         DataPruner dataPruner,
                                         RequestContext context,
                                         Metadata.Item tableName) throws Exception {
        return HudiFileIndex.builder()
                .path(metaClient.getBasePathV2())
                .context(context)
                .dataPruner(dataPruner)
                .partitionPruner(partitionPruner)
                .secureLogin(secureLogin)
                .clients(hiveClients)
                .tableName(tableName)
                .setPartitionTable(isPartitionTable)
                .build();
    }
}