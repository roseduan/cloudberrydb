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

import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class IcebergRestCatalogTest {

    private static final String TABLE_LOCATION = "s3://bucket/table/";
    private static final String METADATA_LOCATION =
            "s3://bucket/table/metadata/v1.metadata.json";

    @Test
    public void authProperties_noneAddsNothing() {
        Configuration conf = baseConfiguration();
        conf.set("auth_method", "none");

        assertTrue(newCatalog(conf).catalogAuthProperties().isEmpty());
    }

    @Test
    public void authProperties_oauth2AddsCredentialAndScope() {
        Configuration conf = baseConfiguration();
        conf.set("auth_method", "oauth2");
        conf.set("client_id", "client");
        conf.set("client_secret", "secret");
        conf.set("scope", "catalog.read");

        Map<String, String> props = newCatalog(conf).catalogAuthProperties();
        assertEquals("client:secret", props.get(OAuth2Properties.CREDENTIAL));
        assertEquals("catalog.read", props.get(OAuth2Properties.SCOPE));
    }

    @Test
    public void authProperties_sigv4AddsAllSignerProperties() {
        Configuration conf = staticCredentialConfiguration();
        conf.set("auth_method", "sigv4");
        conf.set("signing_name", "execute-api");

        Map<String, String> props = newCatalog(conf).catalogAuthProperties();
        assertEquals("true", props.get("rest.sigv4-enabled"));
        assertEquals("us-east-1", props.get("rest.signing-region"));
        assertEquals("execute-api", props.get("rest.signing-name"));
        assertEquals("static-ak", props.get("rest.access-key-id"));
        assertEquals("static-sk", props.get("rest.secret-access-key"));
    }

    @Test
    public void authProperties_sigv4RejectsEveryMissingRequiredField() {
        for (String missing : Arrays.asList(
                "region", "signing_name", "access_key_id", "secret_access_key")) {
            Configuration conf = staticCredentialConfiguration();
            conf.set("auth_method", "sigv4");
            conf.set("signing_name", "execute-api");
            conf.unset(missing);

            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> newCatalog(conf).catalogAuthProperties());
            assertTrue(error.getMessage().contains(missing));
        }
    }

    @Test
    public void polarisDefaultsRealmAndScopeButDoesNotDefaultConnectionCredentials() {
        Configuration conf = new Configuration(false);
        conf.set("polaris_server_url", "http://polaris.example/api/catalog");
        conf.set("client_id", "client");
        conf.set("client_secret", "secret");

        TestPolarisCatalog catalog = new TestPolarisCatalog(conf);
        Map<String, String> props = catalog.catalogAuthProperties();
        assertEquals("POLARIS", props.get("header.Polaris-Realm"));
        assertEquals("PRINCIPAL_ROLE:ALL", props.get(OAuth2Properties.SCOPE));

        Configuration missing = new Configuration(false);
        missing.set("polaris_server_url", "http://polaris.example/api/catalog");
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> new TestPolarisCatalog(missing));
        assertTrue(error.getMessage().contains("client_id"));
    }

    @Test
    public void polarisRealm_isSentAsAHeaderWhenConfigured() {
        /*
         * A Polaris deployment reached over the generic rest type: without the
         * realm header a non-default realm rejects the token request with 404
         * MissingOrInvalidRealm (issue #841).
         */
        Configuration conf = baseConfiguration();
        conf.set("polaris_server_realm", "tenant-a");

        assertEquals("tenant-a", newCatalog(conf).catalogAuthProperties()
                .get("header." + IcebergRestCatalog.POLARIS_REALM_HEADER));
    }

    @Test
    public void polarisRealm_isAbsentWhenNotConfigured() {
        /*
         * The generic type also talks to platforms that have never heard of
         * realms; it must not invent a header for them.  Note this differs
         * from IcebergPolarisCatalog, which defaults the realm to "POLARIS".
         */
        assertFalse(newCatalog(baseConfiguration()).catalogAuthProperties()
                .containsKey("header." + IcebergRestCatalog.POLARIS_REALM_HEADER));
    }

    @Test
    public void polarisRealm_survivesEveryAuthMethod() {
        /* The realm selects the tenant, not the credential, so it is
         * orthogonal to how requests are signed. */
        for (String method : Arrays.asList("none", "sigv4", "oauth2")) {
            Configuration conf = staticCredentialConfiguration();
            conf.set("auth_method", method);
            conf.set("signing_name", "s3tables");
            conf.set("client_id", "cid");
            conf.set("client_secret", "csec");
            conf.set("polaris_server_realm", "tenant-b");

            assertEquals("tenant-b", newCatalog(conf).catalogAuthProperties()
                            .get("header." + IcebergRestCatalog.POLARIS_REALM_HEADER),
                    "realm must survive auth_method " + method);
        }
    }

    @Test
    public void vending_isRequestedByDefault() {
        Configuration conf = baseConfiguration();

        assertEquals(IcebergRestCatalog.DELEGATION_VALUE,
                newCatalog(conf).catalogRuntimeProperties()
                        .get("header." + IcebergRestCatalog.DELEGATION_HEADER));
    }

    @Test
    public void vending_offOmitsTheDelegationHeader() {
        // Without the header the catalog vends nothing, which is what lets the
        // segment fall through to the statically configured keys.
        Configuration conf = baseConfiguration();
        conf.set("vended_credentials", "false");

        assertFalse(newCatalog(conf).catalogRuntimeProperties()
                .containsKey("header." + IcebergRestCatalog.DELEGATION_HEADER));
    }

    @Test
    public void vending_survivesAnExplicitTrue() {
        Configuration conf = baseConfiguration();
        conf.set("vended_credentials", "true");

        assertTrue(newCatalog(conf).catalogRuntimeProperties()
                .containsKey("header." + IcebergRestCatalog.DELEGATION_HEADER));
    }

    @Test
    public void credentials_completeTopLevelVendedGroupWinsAndCarriesToken() {
        Map<String, String> config = credentials("vended-ak", "vended-sk");
        config.put("s3.session-token", "vended-token");
        config.put("s3.region", "vended-region");

        Map<String, String> result = select(stage(config, null), baseConfiguration());
        assertEquals("vended-ak", result.get("gopher.access_key"));
        assertEquals("vended-sk", result.get("gopher.secret_key"));
        assertEquals("vended-token", result.get("gopher.session_token"));
        assertEquals("vended-region", result.get("gopher.region"));
    }

    @Test
    public void credentials_absentVendedGroupFallsBackToStaticCatalogConfig() {
        Map<String, String> result = select(
                stage(Collections.<String, String>emptyMap(), null),
                staticCredentialConfiguration());

        assertEquals("static-ak", result.get("gopher.access_key"));
        assertEquals("static-sk", result.get("gopher.secret_key"));
    }

    @Test
    public void credentials_absentEverywhereFailsEvenWhenOldGopherKeysExist() {
        Map<String, String> oldGopher = credentials("old-ak", "old-sk");
        Map<String, String> initial = new HashMap<>();
        initial.put("gopher.access_key", oldGopher.get("s3.access-key-id"));
        initial.put("gopher.secret_key", oldGopher.get("s3.secret-access-key"));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> IcebergRestCatalog.selectGopherProperties(
                        stage(Collections.<String, String>emptyMap(), null), "bucket",
                        baseConfiguration(), initial));
        assertTrue(error.getMessage().contains("No usable data-plane credentials"));
    }

    @Test
    public void credentials_halfVendedGroupFailsWithoutStaticFallback() {
        Configuration conf = staticCredentialConfiguration();
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> select(stage(Collections.singletonMap(
                        "s3.access-key-id", "vended-ak"), null), conf));
        assertTrue(error.getMessage().contains("s3.secret-access-key"));
    }

    @Test
    public void credentials_emptyVendedSecretFailsWithoutStaticFallback() {
        Map<String, String> config = credentials("vended-ak", "");
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> select(stage(config, null), staticCredentialConfiguration()));
        assertTrue(error.getMessage().contains("s3.secret-access-key"));
    }

    @Test
    public void credentials_halfStaticGroupFailsClearly() {
        Configuration conf = baseConfiguration();
        conf.set("access_key_id", "only-ak");
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> select(stage(Collections.<String, String>emptyMap(), null), conf));
        assertTrue(error.getMessage().contains("secret_access_key"));
    }

    @Test
    public void credentials_storageCredentialsUsesLongestMatchingPrefix() {
        List<Map<String, Object>> storage = Arrays.asList(
                storageCredential("s3://bucket/", "broad-ak", "broad-sk"),
                storageCredential("s3://bucket/table/", "table-ak", "table-sk"));

        Map<String, String> result = select(
                stage(credentials("top-ak", "top-sk"), storage), baseConfiguration());
        assertEquals("table-ak", result.get("gopher.access_key"));
        assertEquals("table-sk", result.get("gopher.secret_key"));
    }

    @Test
    public void credentials_storageCredentialsWithoutTableMatchFails() {
        List<Map<String, Object>> storage = Collections.singletonList(
                storageCredential("s3://other/", "ak", "sk"));
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> select(stage(Collections.<String, String>emptyMap(), storage),
                        staticCredentialConfiguration()));
        assertTrue(error.getMessage().contains("matches table-location"));
    }

    @Test
    public void credentials_selectedPrefixMustAlsoCoverMetadataLocation() {
        List<Map<String, Object>> storage = Collections.singletonList(
                storageCredential(TABLE_LOCATION, "ak", "sk"));
        IcebergRestCatalog.StageOneResult stage = new IcebergRestCatalog.StageOneResult(
                "s3://bucket/metadata/v1.metadata.json", TABLE_LOCATION,
                Collections.<String, String>emptyMap(), storage, null);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> select(stage, baseConfiguration()));
        assertTrue(error.getMessage().contains("cover metadata-location"));
    }

    @Test
    public void loadTable_retriesWholeTwoStageLoadUntilMetadataMatches() throws Exception {
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Arrays.asList(
                        stageAt("s3://bucket/table/metadata/v1.metadata.json"),
                        stageAt("s3://bucket/table/metadata/v2.metadata.json")),
                Arrays.asList(
                        "s3://bucket/table/metadata/stale.metadata.json",
                        "s3://bucket/table/metadata/v2.metadata.json"));

        Table loaded = catalog.loadTable(TableIdentifier.of("ns", "tbl"), null,
                Collections.<String, String>emptyMap());
        assertEquals(2, catalog.stageOneCalls);
        assertEquals(2, catalog.stageTwoCalls);
        assertEquals("s3://bucket/table/metadata/v2.metadata.json",
                catalog.metadataLocation(loaded));
    }

    @Test
    public void loadTable_afterTwoRetriesThrowsConcurrentModificationError() {
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Arrays.asList(stageAt(METADATA_LOCATION), stageAt(METADATA_LOCATION),
                        stageAt(METADATA_LOCATION)),
                Arrays.asList("s3://bucket/stale-1", "s3://bucket/stale-2",
                        "s3://bucket/stale-3"));

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> catalog.loadTable(TableIdentifier.of("ns", "tbl"), null,
                        Collections.<String, String>emptyMap()));
        assertEquals("table is being modified concurrently, retry", error.getMessage());
        assertEquals(3, catalog.stageOneCalls);
    }

    @Test
    public void loadTable_ryowUsesL0BucketAndDoesNotCompareL1WithL2() throws Exception {
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stageAt(METADATA_LOCATION)),
                Collections.singletonList("s3://bucket/catalog-current.metadata.json"));
        String l0 = "s3://bucket/table/metadata/uncommitted.metadata.json";

        catalog.loadTable(TableIdentifier.of("ns", "tbl"), l0,
                Collections.singletonMap("metadata_location", l0));
        assertEquals(1, catalog.stageOneCalls);
        assertEquals(1, catalog.stageTwoCalls);
    }

    @Test
    public void loadTable_ryowRejectsOldGenerationBucketBeforeStageTwo() {
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stageAt(METADATA_LOCATION)),
                Collections.singletonList(METADATA_LOCATION));
        String l0 = "s3://old-bucket/table/metadata/uncommitted.metadata.json";

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> catalog.loadTable(TableIdentifier.of("ns", "tbl"), l0,
                        Collections.singletonMap("metadata_location", l0)));
        assertTrue(error.getMessage().contains("older table generation"));
        assertEquals(0, catalog.stageTwoCalls);
    }

    @Test
    public void resolveScanStorage_asksTheCatalogOnceAndBuildsNoRestCatalog() throws Exception {
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stageAt(METADATA_LOCATION)),
                Collections.<String>emptyList());

        IcebergRestCatalog.ScanStorage storage =
                catalog.resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION);

        /*
         * The whole point of the method: one load-table call, and none of the
         * RESTCatalog construction (a GET /v1/config plus a second load-table)
         * that the two-stage path paid only to end up with a FileIO.
         */
        assertEquals(1, catalog.stageOneCalls);
        assertEquals(0, catalog.stageTwoCalls);
        assertEquals(1, catalog.scanFileIoCalls);
        assertEquals("bucket", catalog.lastScanFileIoProps.get("gopher.bucket"));
        assertNotNull(storage.io());
    }

    @Test
    public void resolveScanStorage_bucketComesFromTheCallersLocationNotTheCatalog() throws Exception {
        /*
         * Stage one names one bucket; the caller is about to read another (a
         * dropped-and-recreated table).  The FileIO must address the bucket of
         * the file actually being read -- the caller's location is the only
         * source of truth, and there is nothing left to cross-validate.
         */
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stageAt(METADATA_LOCATION)),
                Collections.<String>emptyList());

        catalog.resolveScanStorage(TableIdentifier.of("ns", "tbl"),
                "s3://new-generation/table/metadata/v1.metadata.json");

        assertEquals("new-generation", catalog.lastScanFileIoProps.get("gopher.bucket"));
    }

    @Test
    public void resolveScanStorage_vendedOffWithStaticKeysAsksTheCatalogNothing() throws Exception {
        Configuration conf = staticCredentialConfiguration();
        conf.set("vended_credentials", "false");
        TestCatalog catalog = testCatalog(conf,
                Collections.<IcebergRestCatalog.StageOneResult>emptyList(),
                Collections.<String>emptyList());

        IcebergRestCatalog.ScanStorage storage =
                catalog.resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION);

        /* Zero catalog requests: bucket from the location, keys from the
         * user mapping, envelope synthesized locally. */
        assertEquals(0, catalog.stageOneCalls);
        assertEquals(0, catalog.stageTwoCalls);
        assertEquals("bucket", catalog.lastScanFileIoProps.get("gopher.bucket"));
        assertEquals("static-ak", catalog.lastScanFileIoProps.get("gopher.access_key"));
        /* Static keys never expire; only the cache TTL bounds the entry. */
        assertTrue(storage.usableAt(Long.MAX_VALUE - 1L, 0L));

        Map<String, Object> envelope = storage.envelopeFor(METADATA_LOCATION);
        assertEquals(METADATA_LOCATION, envelope.get("metadata-location"));
        assertEquals("s3://bucket/table", envelope.get("table-location"));
        @SuppressWarnings("unchecked")
        Map<String, String> local = (Map<String, String>) envelope.get("local-data-access");
        assertEquals("static-ak", local.get("s3.access-key-id"));
    }

    @Test
    public void resolveScanStorage_vendedOffWithoutStaticKeysFailsClearly() {
        Configuration conf = baseConfiguration();
        conf.set("vended_credentials", "false");
        TestCatalog catalog = testCatalog(conf,
                Collections.<IcebergRestCatalog.StageOneResult>emptyList(),
                Collections.<String>emptyList());

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> catalog.resolveScanStorage(TableIdentifier.of("ns", "tbl"),
                        METADATA_LOCATION));
        assertTrue(error.getMessage().contains("No usable data-plane credentials"));
    }

    @Test
    public void tableRoot_isEverythingBeforeTheMetadataDirectory() {
        assertEquals("s3://bucket/table",
                IcebergRestCatalog.tableRootFromMetadataLocation(METADATA_LOCATION, "bucket"));
        assertEquals("s3://b",
                IcebergRestCatalog.tableRootFromMetadataLocation(
                        "s3://b/no-metadata-dir.json", "b"));
    }

    @Test
    public void loadTableMetadataOnly_answersFromStageOneAndRefusesStorage() throws Exception {
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(
                        stageWithMetadata(METADATA_LOCATION, someTableMetadata())),
                Collections.<String>emptyList());

        Table table = catalog.loadTableMetadataOnly(TableIdentifier.of("ns", "tbl"),
                null, Collections.<String, String>emptyMap());

        /* One catalog request, no RESTCatalog, no FileIO construction. */
        assertEquals(1, catalog.stageOneCalls);
        assertEquals(0, catalog.stageTwoCalls);
        assertEquals(0, catalog.scanFileIoCalls);

        /* The metadata answers, pinned to stage one's head.  (TableMetadata
         * normalizes the trailing slash off the location.) */
        assertEquals(METADATA_LOCATION, catalog.metadataLocation(table));
        assertEquals("s3://bucket/table", table.location());
        assertNotNull(table.schema());
        assertNotNull(catalog.catalogPropertiesEnvelope());

        /* Storage access is a caller bug and says so, instead of an NPE. */
        UnsupportedOperationException error = assertThrows(
                UnsupportedOperationException.class,
                () -> table.io().newInputFile(METADATA_LOCATION));
        assertTrue(error.getMessage().contains("metadata only"));
    }

    @Test
    public void createTableMetadataOnly_usesTheCreateResponseAndBuildsNoRestCatalog()
            throws Exception {
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.<IcebergRestCatalog.StageOneResult>emptyList(),
                Collections.<String>emptyList());
        catalog.createStage = stageWithMetadata(METADATA_LOCATION, someTableMetadata());

        Table table = catalog.createTableMetadataOnly(TableIdentifier.of("ns", "tbl"),
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                null, null, Collections.<String, String>emptyMap());

        assertEquals(1, catalog.createStageOneCalls);
        assertEquals(0, catalog.stageTwoCalls);
        assertEquals(METADATA_LOCATION, catalog.metadataLocation(table));
        assertNotNull(catalog.catalogPropertiesEnvelope());
        assertThrows(UnsupportedOperationException.class,
                () -> table.io().newInputFile(METADATA_LOCATION));
    }

    @Test
    public void resolveScanStorage_buildsTheFileIoTheRestCatalogWouldHave() throws Exception {
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stageAt(METADATA_LOCATION)),
                Collections.<String>emptyList());

        catalog.resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION);

        Map<String, String> props = catalog.lastScanFileIoProps;
        assertEquals("org.cbdb.iceberg.gopher.client.GopherFileIO",
                props.get("io-impl"));
        assertEquals("bucket", props.get("gopher.bucket"));
        assertEquals("static-ak", props.get("gopher.access_key"));
        assertEquals("static-sk", props.get("gopher.secret_key"));
    }

    @Test
    public void resolveScanStorage_envelopeCarriesTheStorageTheSegmentsNeed() throws Exception {
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stage(credentials("vended-ak", "vended-sk"), null)),
                Collections.<String>emptyList());

        Map<String, Object> envelope = catalog
                .resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION)
                .envelopeFor(null);

        assertEquals(TABLE_LOCATION, envelope.get("table-location"));
        assertEquals(METADATA_LOCATION, envelope.get("metadata-location"));
        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) envelope.get("config");
        assertEquals("vended-ak", config.get("s3.access-key-id"));
    }

    @Test
    public void envelope_withVendedCredentialsDoesNotShipTheStaticKeys() throws Exception {
        /*
         * The response vends its own credentials, so the segment will never
         * reach the static ones.  Shipping them regardless would hand every
         * segment a long-lived cross-table key on every scan -- the exact
         * exposure a deployment enables vending to avoid.
         */
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stage(credentials("vended-ak", "vended-sk"), null)),
                Collections.<String>emptyList());

        Map<String, Object> envelope = catalog
                .resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION)
                .envelopeFor(null);

        @SuppressWarnings("unchecked")
        Map<String, String> local = (Map<String, String>) envelope.get("local-data-access");
        assertFalse(local.containsKey("s3.access-key-id"),
                "static access key must not travel with a vended envelope");
        assertFalse(local.containsKey("s3.secret-access-key"),
                "static secret key must not travel with a vended envelope");
        /* Addressing is not a credential and the segment still needs it. */
        assertEquals("us-east-1", local.get("s3.region"));
    }

    @Test
    public void envelope_withAStorageCredentialsGroupAlsoWithholdsTheStaticKeys()
            throws Exception {
        /* Same rule for the other vended shape: a storage-credentials group. */
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stage(Collections.<String, String>emptyMap(),
                        Collections.singletonList(
                                storageCredential(TABLE_LOCATION, "grp-ak", "grp-sk")))),
                Collections.<String>emptyList());

        Map<String, Object> envelope = catalog
                .resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION)
                .envelopeFor(null);

        @SuppressWarnings("unchecked")
        Map<String, String> local = (Map<String, String>) envelope.get("local-data-access");
        assertFalse(local.containsKey("s3.access-key-id"));
    }

    @Test
    public void envelope_withoutVendedCredentialsStillCarriesTheStaticKeys() throws Exception {
        /*
         * Nothing else in the envelope answers for credentials, so these are
         * the only ones the segment has -- withholding them here would break
         * every static-key deployment.
         */
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stageAt(METADATA_LOCATION)),
                Collections.<String>emptyList());

        Map<String, Object> envelope = catalog
                .resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION)
                .envelopeFor(null);

        @SuppressWarnings("unchecked")
        Map<String, String> local = (Map<String, String>) envelope.get("local-data-access");
        assertEquals("static-ak", local.get("s3.access-key-id"));
        assertEquals("static-sk", local.get("s3.secret-access-key"));
    }

    @Test
    public void envelopeFor_namesTheMetadataAboutToBeRead() throws Exception {
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stageAt(METADATA_LOCATION)),
                Collections.<String>emptyList());
        IcebergRestCatalog.ScanStorage storage =
                catalog.resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION);
        String head = "s3://bucket/table/metadata/v9.metadata.json";

        /*
         * A cached entry was resolved against whatever head was current then.
         * The segments check their credential group covers the file they read,
         * so the envelope has to name the head this scan actually reads.
         */
        assertEquals(head, storage.envelopeFor(head).get("metadata-location"));
        /* and the cached envelope itself is not rewritten by handing one out */
        assertEquals(METADATA_LOCATION,
                storage.envelopeFor(null).get("metadata-location"));
    }

    @Test
    public void resolveScanStorage_readsTheVendedCredentialExpiry() throws Exception {
        Map<String, String> config = credentials("vended-ak", "vended-sk");
        config.put("s3.session-token-expires-at-ms", "4102444800000");
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stage(config, null)),
                Collections.<String>emptyList());

        IcebergRestCatalog.ScanStorage storage =
                catalog.resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION);

        assertEquals(4102444800000L, storage.credentialsExpireAtMs());
        assertTrue(storage.usableAt(4102444800000L - 120_000L, 60_000L));
        assertFalse(storage.usableAt(4102444800000L - 30_000L, 60_000L));
    }

    @Test
    public void resolveScanStorage_expiryOfAMatchedCredentialGroupWins() throws Exception {
        Map<String, Object> group = storageCredential(TABLE_LOCATION, "vended-ak", "vended-sk");
        @SuppressWarnings("unchecked")
        Map<String, String> groupConfig = (Map<String, String>) group.get("config");
        groupConfig.put("s3.session-token-expires-at-ms", "4102444800000");
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(
                        stage(Collections.<String, String>emptyMap(),
                                Collections.singletonList(group))),
                Collections.<String>emptyList());

        assertEquals(4102444800000L, catalog
                .resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION)
                .credentialsExpireAtMs());
    }

    @Test
    public void resolveScanStorage_treatsAnUnreadableExpiryAsUnsaid() throws Exception {
        Map<String, String> config = credentials("vended-ak", "vended-sk");
        config.put("s3.session-token-expires-at-ms", "not-a-number");
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stage(config, null)),
                Collections.<String>emptyList());

        IcebergRestCatalog.ScanStorage storage =
                catalog.resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION);

        /*
         * The credentials work; only the ability to expire them early is lost.
         * Failing the scan over an unreadable optional field would be worse.
         */
        assertEquals(0L, storage.credentialsExpireAtMs());
        assertTrue(storage.usableAt(Long.MAX_VALUE - 1L, 0L));
    }

    @Test
    public void scanStorage_withoutAnAdvertisedExpiryNeverExpiresItself() throws Exception {
        TestCatalog catalog = testCatalog(staticCredentialConfiguration(),
                Collections.singletonList(stageAt(METADATA_LOCATION)),
                Collections.<String>emptyList());

        /* Static keys from the user mapping do not expire; only the cache TTL
         * bounds how long they may be reused. */
        assertTrue(catalog.resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION)
                .usableAt(System.currentTimeMillis(), 60_000L));
    }

    @Test
    public void resolveScanStorage_isRejectedOnPolaris() {
        /*
         * Polaris keeps its single-stage load: one bucket per warehouse, known
         * before the table exists, so it never had the two-stage problem this
         * resolves and its FileIO already comes from vendedFileIoCache.
         */
        Configuration conf = new Configuration(false);
        conf.set("polaris_server_url", "http://polaris.example/api/catalog");
        conf.set("client_id", "client");
        conf.set("client_secret", "secret");
        TestPolarisCatalog catalog = new TestPolarisCatalog(conf);

        assertThrows(IllegalStateException.class,
                () -> catalog.resolveScanStorage(TableIdentifier.of("ns", "tbl"), METADATA_LOCATION));
    }

    @Test
    public void closeSuppressing_closesTheResource() {
        RecordingCloseable resource = new RecordingCloseable(null);

        IcebergRestCatalog.closeSuppressing(resource, new IllegalStateException("boom"));

        assertTrue(resource.closed);
    }

    @Test
    public void closeSuppressing_keepsTheOriginalFailureWhenCloseAlsoFails() {
        IOException closeFailure = new IOException("connection already gone");
        RecordingCloseable resource = new RecordingCloseable(closeFailure);
        IllegalStateException tokenFailure = new IllegalStateException("token fetch failed");

        /*
         * The point of the helper: the caller must still be told the token
         * fetch failed, not that closing the wreckage of it failed.
         */
        IcebergRestCatalog.closeSuppressing(resource, tokenFailure);

        assertEquals("token fetch failed", tokenFailure.getMessage());
        assertArrayEquals(new Throwable[] {closeFailure}, tokenFailure.getSuppressed());
    }

    @Test
    public void closeSuppressing_alsoSuppressesAnUncheckedCloseFailure() {
        RuntimeException closeFailure = new IllegalStateException("pool already shut down");
        RecordingCloseable resource = new RecordingCloseable(closeFailure);
        IllegalStateException tokenFailure = new IllegalStateException("token fetch failed");

        IcebergRestCatalog.closeSuppressing(resource, tokenFailure);

        assertArrayEquals(new Throwable[] {closeFailure}, tokenFailure.getSuppressed());
    }

    @Test
    public void closeSuppressing_alsoSuppressesAnErrorFromClose() {
        /*
         * An Error out of close() is not the more important of the two: it is
         * still the teardown of something already broken, and letting it out
         * would drop the failure that stopped the work.  Same rule the compiler
         * applies to try-with-resources.
         */
        Error closeFailure = new StackOverflowError("teardown recursed");
        RecordingCloseable resource = new RecordingCloseable(closeFailure);
        IllegalStateException tokenFailure = new IllegalStateException("token fetch failed");

        IcebergRestCatalog.closeSuppressing(resource, tokenFailure);

        assertEquals("token fetch failed", tokenFailure.getMessage());
        assertArrayEquals(new Throwable[] {closeFailure}, tokenFailure.getSuppressed());
    }

    private static final class RecordingCloseable implements Closeable {
        private final Throwable failure;
        boolean closed;

        RecordingCloseable(Throwable failure) {
            this.failure = failure;
        }

        @Override
        public void close() throws IOException {
            closed = true;
            if (failure instanceof IOException) {
                throw (IOException) failure;
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
        }
    }

    private static Configuration baseConfiguration() {
        Configuration conf = new Configuration(false);
        conf.set("url", "http://catalog.example/api/catalog");
        return conf;
    }

    private static Configuration staticCredentialConfiguration() {
        Configuration conf = baseConfiguration();
        conf.set("access_key_id", "static-ak");
        conf.set("secret_access_key", "static-sk");
        conf.set("region", "us-east-1");
        return conf;
    }

    private static IcebergRestCatalog newCatalog(Configuration conf) {
        return new IcebergRestCatalog("cat.ns.tbl", utilities(), conf,
                Collections.<String, String>emptyMap());
    }

    private static IcebergUtilities utilities() {
        IcebergUtilities utilities = Mockito.mock(IcebergUtilities.class);
        when(utilities.extractWarehouseFromTableName(any(String.class))).thenReturn("cat");
        when(utilities.composeCatalogProperties(any(Configuration.class)))
                .thenAnswer(invocation -> new HashMap<String, String>());
        return utilities;
    }

    private static Map<String, String> credentials(String accessKey, String secretKey) {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("s3.access-key-id", accessKey);
        config.put("s3.secret-access-key", secretKey);
        return config;
    }

    private static Map<String, Object> storageCredential(
            String prefix, String accessKey, String secretKey) {
        Map<String, Object> group = new LinkedHashMap<>();
        group.put("prefix", prefix);
        group.put("config", credentials(accessKey, secretKey));
        return group;
    }

    private static IcebergRestCatalog.StageOneResult stage(
            Map<String, String> config, List<Map<String, Object>> storage) {
        return new IcebergRestCatalog.StageOneResult(
                METADATA_LOCATION, TABLE_LOCATION, config, storage, null);
    }

    private static IcebergRestCatalog.StageOneResult stageAt(String metadataLocation) {
        return new IcebergRestCatalog.StageOneResult(metadataLocation, TABLE_LOCATION,
                Collections.<String, String>emptyMap(), null, null);
    }

    private static IcebergRestCatalog.StageOneResult stageWithMetadata(
            String metadataLocation, TableMetadata metadata) {
        return new IcebergRestCatalog.StageOneResult(metadataLocation, TABLE_LOCATION,
                Collections.<String, String>emptyMap(), null, metadata);
    }

    private static TableMetadata someTableMetadata() {
        return TableMetadata.newTableMetadata(
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                PartitionSpec.unpartitioned(), TABLE_LOCATION,
                Collections.<String, String>emptyMap());
    }

    private static Map<String, String> select(
            IcebergRestCatalog.StageOneResult stage, Configuration conf) {
        return IcebergRestCatalog.selectGopherProperties(stage, "bucket", conf,
                Collections.<String, String>emptyMap());
    }

    private static TestCatalog testCatalog(Configuration conf,
                                           List<IcebergRestCatalog.StageOneResult> stages,
                                           List<String> loadedMetadataLocations) {
        return new TestCatalog(conf, stages, loadedMetadataLocations);
    }

    /** Replaces HTTP phase one and RESTCatalog phase two with deterministic mocks. */
    private static class TestCatalog extends IcebergRestCatalog {
        private final Deque<StageOneResult> stages;
        private final Deque<String> loadedMetadataLocations;
        private final Map<Table, String> tableMetadataLocations = new IdentityHashMap<>();
        private int stageOneCalls;
        private int stageTwoCalls;
        private int scanFileIoCalls;
        private int createStageOneCalls;
        private StageOneResult createStage;
        private Map<String, String> lastScanFileIoProps;

        TestCatalog(Configuration conf, List<StageOneResult> stages,
                    List<String> loadedMetadataLocations) {
            super("cat.ns.tbl", utilities(), conf, Collections.<String, String>emptyMap());
            this.stages = new ArrayDeque<>(stages);
            this.loadedMetadataLocations = new ArrayDeque<>(loadedMetadataLocations);
        }

        /** Stands in for GopherFileIO, which needs the native client. */
        @Override
        protected FileIO newScanFileIO(Map<String, String> catalogProps) {
            scanFileIoCalls++;
            lastScanFileIoProps = catalogProps;
            return Mockito.mock(FileIO.class);
        }

        @Override
        protected StageOneResult fetchStageOne(TableIdentifier tableId) {
            stageOneCalls++;
            return stages.removeFirst();
        }

        @Override
        protected StageOneResult createStageOne(TableIdentifier tableId, Schema schema,
                                                PartitionSpec spec,
                                                Map<String, String> tableProps) {
            createStageOneCalls++;
            return createStage;
        }

        @Override
        protected RESTCatalog newRestCatalog() {
            stageTwoCalls++;
            RESTCatalog catalog = Mockito.mock(RESTCatalog.class);
            Table table = Mockito.mock(Table.class);
            tableMetadataLocations.put(table, loadedMetadataLocations.removeFirst());
            when(catalog.loadTable(any(TableIdentifier.class))).thenReturn(table);
            return catalog;
        }

        @Override
        protected String metadataLocation(Table table) {
            String pinned = tableMetadataLocations.get(table);
            return pinned != null ? pinned : super.metadataLocation(table);
        }
    }

    private static class TestPolarisCatalog extends IcebergPolarisCatalog {
        TestPolarisCatalog(Configuration conf) {
            super("cat.ns.tbl", utilities(), conf, Collections.<String, String>emptyMap());
        }

        @Override
        protected RESTCatalog newRestCatalog() {
            return Mockito.mock(RESTCatalog.class);
        }
    }
}
