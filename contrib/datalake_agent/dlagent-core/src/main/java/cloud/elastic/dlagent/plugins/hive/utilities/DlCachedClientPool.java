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

package cloud.elastic.dlagent.plugins.hive.utilities;

import cloud.elastic.dlagent.api.security.SecureLogin;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.thrift.TException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DlCachedClientPool implements ClientPool<IMetaStoreClient, TException> {
    private static Cache<String, DlHiveClientPool> clientPoolCache;
    private static final Object INIT_LOCK = new Object();

    private final Configuration conf;
    private final String metastoreUri;
    private final int clientPoolSize;
    private final long evictionInterval;
    private final SecureLogin secureLogin;
    private final String serverName;
    private final String configFile;

    public DlCachedClientPool(Configuration conf,
                       Map<String, String> properties,
                       SecureLogin secureLogin,
                       String serverName,
                       String configFile) {
        this.conf = conf;
        this.metastoreUri = conf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
        this.clientPoolSize =
                PropertyUtil.propertyAsInt(
                        properties,
                        CatalogProperties.CLIENT_POOL_SIZE,
                        CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
        this.evictionInterval =
                PropertyUtil.propertyAsLong(
                        properties,
                        CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                        CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);
        this.secureLogin = secureLogin;
        this.serverName = serverName;
        this.configFile = configFile;
        init();
    }

    DlHiveClientPool clientPool() {
        return clientPoolCache.get(metastoreUri, k -> new DlHiveClientPool(clientPoolSize,
                conf, secureLogin, serverName, configFile));
    }

    private void init() {
        // clientPoolCache is static; guard initialization with a static lock so two
        // concurrent first-time constructors do not race to build two caches and lose one.
        if (clientPoolCache == null) {
            synchronized (INIT_LOCK) {
                if (clientPoolCache == null) {
                    clientPoolCache =
                            Caffeine.newBuilder()
                                    .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
                                    .removalListener((key, value, cause) -> {
                                        if (value != null) ((DlHiveClientPool) value).close();
                                    })
                                    .build();
                }
            }
        }
    }

    static Cache<String, DlHiveClientPool> clientPoolCache() {
        return clientPoolCache;
    }

    /**
     * Force-evict the cached {@link DlHiveClientPool} for the given metastore URI.
     * Triggered by {@link cloud.elastic.dlagent.plugins.iceberg.IcebergHiveCatalog#close()}
     * when an upstream catalog is dropped due to a connection-class failure, so the next
     * request rebuilds the thrift connection pool instead of reusing the broken one.
     */
    public static void invalidate(String metastoreUri) {
        Cache<String, DlHiveClientPool> c = clientPoolCache;
        if (c != null && metastoreUri != null && !metastoreUri.isEmpty()) {
            c.invalidate(metastoreUri);
        }
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action)
            throws TException, InterruptedException {
        return clientPool().run(action);
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
            throws TException, InterruptedException {
        return clientPool().run(action, retry);
    }
}
