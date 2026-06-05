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

package cloud.elastic.dlagent.plugins.iceberg;

import cloud.elastic.dlagent.api.configuration.DlServerProperties;
import cloud.elastic.dlagent.api.model.RequestContext;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link IcebergCatalogWrapper}'s hive catalog cache: the cache
 * key must cover the full connection definition (so configuration changes
 * map to new entries instead of hitting a stale catalog), must not leak
 * credentials, and idle entries must be evicted and closed after the
 * configured TTL.
 */
public class IcebergCatalogWrapperTest {

    private static final String SECRET = "TOPSECRETACCESSKEY";

    private IcebergCatalogWrapper newWrapper() {
        return new IcebergCatalogWrapper(new DlServerProperties());
    }

    /**
     * Builds a context with every connection-defining input populated.
     */
    private RequestContext baseContext() {
        RequestContext context = new RequestContext();
        context.setServerName("hive1");
        context.setConfig("hive1");
        context.setPath("/warehouse/db.schema");

        Configuration configuration = new Configuration(false);
        configuration.set("hive.metastore.uris", "thrift://hms-1:9083");
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set("hive.metastore.sasl.enabled", "true");
        configuration.set("hive.metastore.kerberos.principal", "hive/_HOST@EXAMPLE.COM");
        context.setConfiguration(configuration);

        Map<String, String> gopherProperties = new HashMap<>();
        gopherProperties.put("endpoint", "http://oss.example.com");
        gopherProperties.put("bucket", "warehouse-bucket");
        gopherProperties.put("access_key_id", "AKIDEXAMPLE");
        gopherProperties.put("secret_access_key", SECRET);
        gopherProperties.put("region", "cn-north-1");
        context.setGopherProperties(gopherProperties);
        return context;
    }

    @Test
    public void formCatalogCacheKey_isStable_forIdenticalConnectionInfo() {
        IcebergCatalogWrapper wrapper = newWrapper();
        // Two independently built contexts with the same connection info must
        // map to the same entry, otherwise caching is defeated.
        assertEquals(wrapper.formCatalogCacheKey(baseContext()),
                wrapper.formCatalogCacheKey(baseContext()));
    }

    @Test
    public void formCatalogCacheKey_ignoresRequestScopedFields() {
        IcebergCatalogWrapper wrapper = newWrapper();
        RequestContext other = baseContext();
        other.setUser("another-user");
        other.setSchemaName("other_schema");
        other.setTableName("other_table");
        // Per-request fields must not fragment the cache.
        assertEquals(wrapper.formCatalogCacheKey(baseContext()),
                wrapper.formCatalogCacheKey(other));
    }

    @Test
    public void formCatalogCacheKey_changes_whenConnectionInfoChanges() {
        IcebergCatalogWrapper wrapper = newWrapper();
        String baseKey = wrapper.formCatalogCacheKey(baseContext());

        RequestContext warehouseChanged = baseContext();
        warehouseChanged.setPath("/warehouse/other");
        assertNotEquals(baseKey, wrapper.formCatalogCacheKey(warehouseChanged));

        RequestContext uriChanged = baseContext();
        uriChanged.getConfiguration().set("hive.metastore.uris", "thrift://hms-2:9083");
        assertNotEquals(baseKey, wrapper.formCatalogCacheKey(uriChanged));

        RequestContext principalChanged = baseContext();
        principalChanged.getConfiguration().set("hive.metastore.kerberos.principal",
                "hive/_HOST@OTHER.COM");
        assertNotEquals(baseKey, wrapper.formCatalogCacheKey(principalChanged));

        RequestContext credentialsChanged = baseContext();
        Map<String, String> gopherProperties = new HashMap<>(credentialsChanged.getGopherProperties());
        gopherProperties.put("secret_access_key", "ROTATEDSECRET");
        credentialsChanged.setGopherProperties(gopherProperties);
        assertNotEquals(baseKey, wrapper.formCatalogCacheKey(credentialsChanged));
    }

    @Test
    public void formCatalogCacheKey_isHexDigest_andDoesNotLeakCredentials() {
        String key = newWrapper().formCatalogCacheKey(baseContext());
        assertTrue(key.matches("[0-9a-f]{64}"), "expected sha256 hex, got: " + key);
        assertFalse(key.contains(SECRET));
    }

    @Test
    public void defaultHiveCatalogCacheTtl_isFiveMinutes() {
        assertEquals(Duration.ofMinutes(5),
                new DlServerProperties().getIceberg().getHiveCatalogCacheTtl());
    }

    @Test
    public void constructor_rejectsNonPositiveTtl() {
        // Zero is accepted by Guava but evicts every entry on its next
        // access, silently disabling the cache; fail startup instead.
        DlServerProperties zero = new DlServerProperties();
        zero.getIceberg().setHiveCatalogCacheTtl(Duration.ZERO);
        assertThrows(IllegalArgumentException.class, () -> new IcebergCatalogWrapper(zero));

        DlServerProperties negative = new DlServerProperties();
        negative.getIceberg().setHiveCatalogCacheTtl(Duration.ofSeconds(-1));
        assertThrows(IllegalArgumentException.class, () -> new IcebergCatalogWrapper(negative));
    }

    @Test
    public void formCatalogCacheKey_toleratesSparseContext() {
        // A context with null configuration and null gopher properties (e.g.
        // on the connection-failure invalidation path) must still produce a
        // stable key instead of throwing.
        RequestContext sparse = Mockito.mock(RequestContext.class);
        String key = newWrapper().formCatalogCacheKey(sparse);
        assertTrue(key.matches("[0-9a-f]{64}"), "expected sha256 hex, got: " + key);
    }

    @Test
    public void catalogCache_evictsAndClosesIdleEntries() throws Exception {
        DlServerProperties properties = new DlServerProperties();
        properties.getIceberg().setHiveCatalogCacheTtl(Duration.ofMillis(50));
        IcebergCatalogWrapper wrapper = new IcebergCatalogWrapper(properties);

        IcebergHiveCatalog catalog = Mockito.mock(IcebergHiveCatalog.class);
        wrapper.getCatalogCache().put("some-key", catalog);
        assertEquals(1, wrapper.getCatalogCache().size());

        // Guava evicts lazily; sleep past the TTL and force maintenance.
        Thread.sleep(200);
        wrapper.getCatalogCache().cleanUp();

        assertEquals(0, wrapper.getCatalogCache().size());
        Mockito.verify(catalog).close();
    }

    @Test
    public void catalogCache_keepsEntriesAccessedWithinTtl() {
        IcebergCatalogWrapper wrapper = newWrapper();
        IcebergHiveCatalog catalog = Mockito.mock(IcebergHiveCatalog.class);
        wrapper.getCatalogCache().put("some-key", catalog);
        wrapper.getCatalogCache().cleanUp();

        assertEquals(catalog, wrapper.getCatalogCache().getIfPresent("some-key"));
        Mockito.verify(catalog, Mockito.never()).close();
    }
}
