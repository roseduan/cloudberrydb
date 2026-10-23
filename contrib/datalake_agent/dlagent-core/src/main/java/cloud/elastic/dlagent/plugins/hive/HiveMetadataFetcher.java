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

package cloud.elastic.dlagent.plugins.hive;

import cloud.elastic.dlagent.api.model.BasePlugin;
import cloud.elastic.dlagent.api.model.MetadataFetcher;
import cloud.elastic.dlagent.api.model.Partition;
import cloud.elastic.dlagent.api.model.Metadata;
import cloud.elastic.dlagent.api.model.FragmentDescription;
import cloud.elastic.dlagent.api.utilities.BeanLocator;

import cloud.elastic.dlagent.api.security.SecureLogin;
import cloud.elastic.dlagent.plugins.hive.utilities.DlCachedClientPool;
import cloud.elastic.dlagent.plugins.hudi.utilities.FilePathUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import cloud.elastic.dlagent.plugins.hive.utilities.HiveUtilities;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.thrift.TException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetadataFetcher extends BasePlugin implements MetadataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(HiveMetadataFetcher.class);

    private final HiveClientWrapper hiveClientWrapper;
    protected final HiveUtilities hiveUtilities;
    private final SecureLogin secureLogin;

    private ClientPool<IMetaStoreClient, TException> clients;

    public HiveMetadataFetcher() {
        this(BeanLocator.getBean(HiveUtilities.class),
                BeanLocator.getBean(HiveClientWrapper.class),
                BeanLocator.getBean(SecureLogin.class));
    }

    HiveMetadataFetcher(HiveUtilities hiveUtilities,
                        HiveClientWrapper hiveClientWrapper,
                        SecureLogin secureLogin) {
        this.hiveUtilities = hiveUtilities;
        this.hiveClientWrapper = hiveClientWrapper;
        this.secureLogin = secureLogin;
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();

        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.CLIENT_POOL_SIZE, "5");
        HiveConf conf = new HiveConf(configuration, HiveConf.class);

        this.clients = new DlCachedClientPool(conf,
                                              props,
                                              secureLogin,
                                              context.getServerName(),
                                              context.getConfig());
    }

    public FragmentDescription getFragments(String pattern) throws Exception {
        throw new UnsupportedOperationException("Hive accessor does not support getFragments operation.");
    }

    public List<Partition> getPartitions(String pattern) throws Exception {
        Metadata.Item tblDesc = hiveClientWrapper.extractTableFromName(context.getDataSource());

        return fetchPartitions(tblDesc);
    }

    public Metadata getSchema(String pattern) throws Exception {
        throw new UnsupportedOperationException("Hive accessor does not support getSchema operation.");
    }

    private List<Partition> fetchPartitions(Metadata.Item tblDesc) throws Exception {
        Table table = clients.run(client -> hiveClientWrapper.getTable(client, tblDesc));
        String[] partitionKeys = table.getPartitionKeys()
                .stream().map(FieldSchema::getName).toArray(String[]::new);

        Instant startTime = Instant.now();
        List<String> partitions = clients.run(client -> hiveClientWrapper.listPartitionNames(client, tblDesc));
        Duration duration = Duration.between(startTime, Instant.now());
        LOG.info("Finished listPartitionNames [{}] operation in {} ms.", partitions.size(), duration.toMillis());

        List<Partition> partitionsList = partitions.stream().
                map(p ->
                {List<String> values = FilePathUtils.extractPartitionKeyValues(new org.apache.hadoop.fs.Path(p),
                        true, partitionKeys).values().stream().collect(Collectors.toList());

                    return new Partition(new HivePartitionMetadata(values));
                }).collect(Collectors.toList());

        return partitionsList;
    }

    public boolean open() throws Exception {
        return true;
    }

    public void close() throws Exception {
    }

    @Override
    public Boolean batchAppend() throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'batchAppend'");
    }

    @Override
    public Metadata getOrCreateSchema() throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'getOrCreateSchema'");
    }

    @Override
    public Boolean rowUpdate() throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'rowUpdate'");
    }

    @Override
    public Map<String, String> getCurrentSnapshotSummary() throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'getCurrentSnapshotSummary'");
    }
}
