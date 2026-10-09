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
import cloud.elastic.dlagent.api.error.DlRuntimeException;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
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

    @Test
    public void restScanStorage_repeatScanDoesNotReachTheCatalog() throws Exception {
        RecordingWrapper wrapper = new RecordingWrapper(new DlServerProperties());
        RequestContext context = restContext();

        IcebergRestCatalog.ScanStorage first =
                wrapper.getRestScanStorage(context, REST_METADATA_LOCATION);
        IcebergRestCatalog.ScanStorage second =
                wrapper.getRestScanStorage(context, REST_METADATA_LOCATION);

        // The whole point: the second scan is answered without a round trip.
        assertEquals(1, wrapper.resolveCalls);
        assertSame(first, second);
    }

    @Test
    public void restScanStorage_separateTablesDoNotShareAnEntry() throws Exception {
        RecordingWrapper wrapper = new RecordingWrapper(new DlServerProperties());

        wrapper.getRestScanStorage(restContext(), REST_METADATA_LOCATION);
        RequestContext other = restContext();
        other.setDataSource("ns.other");
        wrapper.getRestScanStorage(other, REST_METADATA_LOCATION);

        // One bucket per table makes sharing an entry across tables unsound.
        assertEquals(2, wrapper.resolveCalls);
    }

    @Test
    public void restScanStorage_doesNotOutliveAnAlterServer() throws Exception {
        RecordingWrapper wrapper = new RecordingWrapper(new DlServerProperties());
        wrapper.getRestScanStorage(restContext(), REST_METADATA_LOCATION);

        /*
         * Before this cache, every REST scan rebuilt its catalog from the
         * request's configuration, so repointing the server or rotating its
         * keys took effect on the next query.  Keep that: a cached entry keyed
         * only on the Hive connection fields would serve the old endpoint (or
         * the revoked key) for a whole TTL.
         */
        /*
         * Every REST setting, not a sample of them: the one this list left out
         * was polaris_server_realm, and leaving it out of the key meant two
         * catalogs differing only by realm shared an entry -- the second
         * served credentials vended under the first one's tenant.  A new REST
         * option belongs here at the same time it joins
         * REST_CONNECTION_CONFIG_KEYS.
         */
        for (String changed : new String[] {
                "url", "polaris_server_url", "prefix", "warehouse_location_prefix",
                "auth_method", "region", "signing_name", "access_key_id",
                "secret_access_key", "client_id", "client_secret", "scope",
                "endpoint", "path_style_access", "vended_credentials",
                "polaris_server_realm"}) {
            RequestContext altered = restContext();
            altered.getConfiguration().set(changed, "changed-" + changed);
            int before = wrapper.resolveCalls;
            wrapper.getRestScanStorage(altered, REST_METADATA_LOCATION);
            assertEquals(before + 1, wrapper.resolveCalls,
                    "changing " + changed + " must not hit the cached entry");
        }
    }

    @Test
    public void restScanStorage_aNewBucketKeysANewEntry() throws Exception {
        RecordingWrapper wrapper = new RecordingWrapper(new DlServerProperties());
        RequestContext context = restContext();
        wrapper.getRestScanStorage(context, REST_METADATA_LOCATION);

        /*
         * A REST catalog gives every table its own bucket, so a table dropped
         * and recreated under the same name lives somewhere else.  The bucket
         * comes from the head the caller is about to read and is part of the
         * cache key, so the new generation simply resolves its own entry --
         * there is no cross-check to fail and nothing to invalidate; the old
         * entry ages out on its own.
         */
        wrapper.getRestScanStorage(
                context, "s3://new-bucket/table/metadata/v1.metadata.json");

        assertEquals(2, wrapper.resolveCalls);
        assertEquals(2, wrapper.getRestScanStorageCache().size());

        /* And each generation's entry keeps answering without a round trip. */
        wrapper.getRestScanStorage(context, REST_METADATA_LOCATION);
        wrapper.getRestScanStorage(
                context, "s3://new-bucket/table/metadata/v1.metadata.json");
        assertEquals(2, wrapper.resolveCalls);
    }

    @Test
    public void restScanStorage_refusesToGuessWithoutAMetadataLocation() {
        RecordingWrapper wrapper = new RecordingWrapper(new DlServerProperties());

        /*
         * The metadata location is the bucket's only source; without it there
         * is nothing sound to hand back, and silently resolving "something"
         * would revive the very ambiguity the single-source rule removed.
         */
        assertThrows(DlRuntimeException.class,
                () -> wrapper.getRestScanStorage(restContext(), null));
        assertEquals(0, wrapper.resolveCalls);
    }

    @Test
    public void restScanStorage_reresolvesBeforeTheVendedCredentialsExpire() throws Exception {
        RecordingWrapper wrapper = new RecordingWrapper(new DlServerProperties());
        RequestContext context = restContext();

        // Expires inside the 60s margin, so it is good for this scan and no more.
        wrapper.expiresAtMs = System.currentTimeMillis() + 30_000L;
        wrapper.getRestScanStorage(context, REST_METADATA_LOCATION);
        wrapper.getRestScanStorage(context, REST_METADATA_LOCATION);

        assertEquals(2, wrapper.resolveCalls);
    }

    @Test
    public void restScanStorage_keepsCredentialsThatOutlastTheMargin() throws Exception {
        RecordingWrapper wrapper = new RecordingWrapper(new DlServerProperties());
        RequestContext context = restContext();

        // What both platforms actually vend: an hour.
        wrapper.expiresAtMs = System.currentTimeMillis() + 3_600_000L;
        wrapper.getRestScanStorage(context, REST_METADATA_LOCATION);
        wrapper.getRestScanStorage(context, REST_METADATA_LOCATION);

        assertEquals(1, wrapper.resolveCalls);
    }

    @Test
    public void restScanStorage_isBoundedByTheConfiguredTtl() throws Exception {
        DlServerProperties properties = new DlServerProperties();
        properties.getIceberg().setFileIoCacheTtl(Duration.ofMillis(50));
        RecordingWrapper wrapper = new RecordingWrapper(properties);
        RequestContext context = restContext();

        /*
         * Credentials good for an hour still do not pin an entry for an hour:
         * the TTL is the upper bound, and it is what limits how long a revoked
         * authorization can go unnoticed.
         */
        wrapper.expiresAtMs = System.currentTimeMillis() + 3_600_000L;
        wrapper.getRestScanStorage(context, REST_METADATA_LOCATION);
        Thread.sleep(200);
        wrapper.getRestScanStorage(context, REST_METADATA_LOCATION);

        assertEquals(2, wrapper.resolveCalls);
    }

    private static final String REST_METADATA_LOCATION =
            "s3://table-bucket/table/metadata/v1.metadata.json";

    private RequestContext restContext() {
        RequestContext context = new RequestContext();
        context.setServerName("rest1");
        context.setConfig("rest1");
        context.setDataSource("ns.tbl");
        context.setConfiguration(restConfiguration());
        return context;
    }

    /**
     * Replaces the catalog round trip with a counter, so the tests above
     * measure exactly what the cache is for: how often the catalog is asked.
     */
    private static class RecordingWrapper extends IcebergCatalogWrapper {
        private int resolveCalls;
        private String bucket = "table-bucket";
        private long expiresAtMs;

        RecordingWrapper(DlServerProperties properties) {
            super(properties);
            setIcebergUtilities(restUtilities());
        }

        @Override
        protected IcebergCatalog newIcebergCatalog(RequestContext context) {
            return new StubRestCatalog(this);
        }
    }

    private static class StubRestCatalog extends IcebergRestCatalog {
        private final RecordingWrapper owner;

        StubRestCatalog(RecordingWrapper owner) {
            super("cat.ns.tbl", restUtilities(), restConfiguration(),
                    Collections.<String, String>emptyMap());
            this.owner = owner;
        }

        @Override
        public ScanStorage resolveScanStorage(TableIdentifier tableId, String metadataLocation) {
            owner.resolveCalls++;
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("table-location", "s3://" + owner.bucket + "/table/");
            envelope.put("metadata-location",
                    "s3://" + owner.bucket + "/table/metadata/v1.metadata.json");
            return new ScanStorage(Mockito.mock(FileIO.class), envelope,
                    owner.expiresAtMs);
        }
    }

    private static Configuration restConfiguration() {
        Configuration conf = new Configuration(false);
        conf.set("url", "http://catalog.example/api/catalog");
        return conf;
    }

    private static IcebergUtilities restUtilities() {
        IcebergUtilities utilities = Mockito.mock(IcebergUtilities.class);
        Mockito.when(utilities.extractWarehouseFromTableName(ArgumentMatchers.anyString()))
                .thenReturn("cat");
        Mockito.when(utilities.getIcebergTableIdentifier(ArgumentMatchers.anyString()))
                .thenReturn(TableIdentifier.of("ns", "tbl"));
        return utilities;
    }
}
