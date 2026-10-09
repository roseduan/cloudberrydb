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

import cloud.elastic.dlagent.plugins.hudi.utilities.FilePathUtils;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import cloud.elastic.dlagent.plugins.iceberg.utilities.SensitiveDataRedactor;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Common Iceberg REST catalog implementation.
 *
 * <p>The generic {@code rest} type first fetches the load-table DTO without
 * constructing FileIO, then initializes GopherFileIO with the table bucket and
 * the response's vended credentials. Polaris uses the same catalog plumbing but
 * retains its historical single-stage writable load path.
 */
public class IcebergRestCatalog implements IcebergCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergRestCatalog.class);

    static final String AUTH_SIGV4 = "sigv4";
    static final String AUTH_OAUTH2 = "oauth2";
    static final String AUTH_NONE = "none";
    static final String DELEGATION_HEADER = "X-Iceberg-Access-Delegation";
    static final String DELEGATION_VALUE = "vended-credentials";
    static final int ENVELOPE_VERSION = 1;
    /**
     * When the vended credentials stop working, as the catalog reports it.
     * Both platforms this has been run against return it -- XSKY's S3 Tables
     * and Polaris -- and both mint credentials that live an hour.
     */
    static final String SESSION_TOKEN_EXPIRES_AT_MS = "s3.session-token-expires-at-ms";

    /** Polaris's realm selector; see the send site in {@link #authProperties()}. */
    static final String POLARIS_REALM_CONFIG_KEY = "polaris_server_realm";
    static final String POLARIS_REALM_HEADER = "Polaris-Realm";

    /**
     * What a caller reads when it wrongly asks a metadata-only table for
     * storage access.  An exception with directions, not an NPE three frames
     * later: the table was built by {@link #loadTableMetadataOnly} or
     * {@link #createTableMetadataOnly}, which deliberately request no storage
     * credentials from the catalog.
     */
    static final String METADATA_ONLY_MESSAGE =
            "table was loaded for metadata only (no storage credentials were requested); "
                    + "use the scan path if you need to read data";

    private final String catalogType;
    private final String catalogName;
    private final String catalogUrl;
    private final String prefix;
    protected final IcebergUtilities icebergUtilities;
    protected final Configuration configuration;
    private final Map<String, String> initialGopherProperties;

    private RESTCatalog restCatalog;
    private Map<String, Object> catalogPropertiesEnvelope;

    public IcebergRestCatalog(String warehouse, IcebergUtilities icebergUtilities,
                              Configuration configuration,
                              Map<String, String> gopherProperties) {
        this("rest", warehouse, icebergUtilities, configuration, gopherProperties,
                requireConfig(configuration, "url"));
    }

    protected IcebergRestCatalog(String catalogType, String warehouse,
                                 IcebergUtilities icebergUtilities,
                                 Configuration configuration,
                                 Map<String, String> gopherProperties,
                                 String catalogUrl) {
        this.catalogType = catalogType;
        this.icebergUtilities = icebergUtilities;
        this.configuration = configuration;
        this.catalogName = icebergUtilities.extractWarehouseFromTableName(warehouse);
        this.catalogUrl = catalogUrl;
        this.prefix = "rest".equals(catalogType)
                ? unescape(configuration.get("prefix", catalogName)) : null;
        this.initialGopherProperties = gopherProperties == null
                ? new HashMap<>() : new HashMap<>(gopherProperties);

        // Polaris must retain the old eager initialization and load path.
        if ("rest".equals(catalogType)) {
            // Validate authentication configuration before the first request.
            authProperties();
        } else {
            this.restCatalog = initializeCatalog(this.initialGopherProperties);
        }
    }

    protected Map<String, String> authProperties() {
        String method = unescape(configuration.get("auth_method", AUTH_NONE))
                .toLowerCase(Locale.ROOT);
        Map<String, String> props = new HashMap<>();
        switch (method) {
            case AUTH_SIGV4:
                props.put("rest.sigv4-enabled", "true");
                props.put("rest.signing-region", requireConfig(configuration, "region"));
                props.put("rest.signing-name", requireConfig(configuration, "signing_name"));
                props.put("rest.access-key-id", requireConfig(configuration, "access_key_id"));
                props.put("rest.secret-access-key", requireConfig(configuration, "secret_access_key"));
                break;
            case AUTH_OAUTH2:
                String clientId = requireConfig(configuration, "client_id");
                String clientSecret = requireConfig(configuration, "client_secret");
                props.put(OAuth2Properties.CREDENTIAL, clientId + ":" + clientSecret);
                props.put(OAuth2Properties.SCOPE,
                        unescape(configuration.get("scope", OAuth2Properties.CATALOG_SCOPE)));
                break;
            case AUTH_NONE:
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Iceberg REST auth_method: " + method
                                + "; expected sigv4, oauth2, or none");
        }
        /*
         * A Polaris deployment reached over the generic type still needs its
         * realm header: configured with a non-default realm, it answers the
         * OAuth token request with 404 MissingOrInvalidRealm when the header
         * is missing (issue #841).  Unlike IcebergPolarisCatalog, which
         * defaults the realm to "POLARIS", this sends nothing unless asked --
         * the generic type talks to platforms that have never heard of
         * realms, and inventing a header for them is not harmless.
         */
        String realm = configuration.get(POLARIS_REALM_CONFIG_KEY);
        if (!isBlank(realm)) {
            props.put("header." + POLARIS_REALM_HEADER, unescape(realm));
        }
        return props;
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                             String location, Map<String, String> tableProps) throws Exception {
        TableIdentifier tableId = icebergUtilities.getIcebergTableIdentifier(identifier.toString());
        if (!"rest".equals(catalogType)) {
            // Polaris: one bucket per warehouse, known from configuration before
            // the table exists, so the catalog can be built up front.
            RESTCatalog catalog = restCatalog;
            if (catalog == null) {
                catalog = initializeCatalog(staticGopherProperties(location));
                restCatalog = catalog;
            }
            return catalog.createTable(tableId, schema,
                    spec != null ? spec : PartitionSpec.unpartitioned(),
                    IcebergUtilities.stripInternalProperties(tableProps));
        }

        /*
         * A generic REST catalog allocates the table's bucket as part of creating
         * it, so the bucket cannot be known beforehand -- and GopherFileIO demands
         * one at construction time ("Bucket is required for object storage").
         * Create in two stages, mirroring loadTable: a bare REST call that needs
         * no FileIO (the catalog server writes the initial metadata itself), then
         * a catalog built on the bucket that call just returned.
         *
         * Note this never reuses a cached catalog: with one bucket per table, a
         * catalog built for an earlier table addresses the wrong storage.
         */
        StageOneResult stageOne = createStageOne(tableId, schema, spec, tableProps);
        Map<String, String> gopher = selectGopherProperties(
                stageOne, bucketFromLocation(stageOne.tableLocation),
                configuration, initialGopherProperties);
        RESTCatalog candidate = initializeCatalog(gopher);
        try {
            Table table = candidate.loadTable(tableId);
            closeRestCatalog(restCatalog);
            restCatalog = candidate;
            catalogPropertiesEnvelope = stageOne.envelope(configuration);
            return table;
        } catch (RuntimeException e) {
            closeRestCatalog(candidate);
            throw e;
        }
    }

    /**
     * Creates the table with a bare REST call and returns the storage information
     * the catalog assigned to it, in the same shape {@link #fetchStageOne} returns
     * for an existing table.
     */
    protected StageOneResult createStageOne(TableIdentifier tableId, Schema schema,
                                            PartitionSpec spec, Map<String, String> tableProps)
            throws Exception {
        CreateTableRequest request = CreateTableRequest.builder()
                .withName(tableId.name())
                .withSchema(schema)
                .withPartitionSpec(spec != null ? spec : PartitionSpec.unpartitioned())
                .setProperties(IcebergUtilities.stripInternalProperties(tableProps))
                .build();
        try (BareClient bare = newBareClient()) {
            ExtendedLoadTableResponse response = bare.client.post(
                    new ResourcePaths(prefix).tables(tableId.namespace()),
                    request,
                    ExtendedLoadTableResponse.class,
                    bare.headers,
                    ErrorHandlers.tableErrorHandler());
            response.validate();
            return new StageOneResult(response.metadataLocation(),
                    response.tableMetadata().location(), response.config(),
                    response.storageCredentials(), response.tableMetadata());
        }
    }

    @Override
    public Table createTableMetadataOnly(TableIdentifier identifier, Schema schema,
                                         PartitionSpec spec, String location,
                                         Map<String, String> tableProps) throws Exception {
        if (!"rest".equals(catalogType)) {
            return createTable(identifier, schema, spec, location, tableProps);
        }
        /*
         * The create response is a full load-table response: the catalog server
         * wrote the initial metadata itself and echoes it back.  Everything the
         * DDL response is built from is in it, so the second stage loadTable
         * runs -- GET /v1/config plus a re-load purely to obtain a FileIO --
         * has nothing left to contribute here.  Write paths that commit through
         * the created table (see IcebergMetadataFetcher.batchAppend) keep using
         * createTable, which still builds the real catalog.
         */
        TableIdentifier tableId = icebergUtilities.getIcebergTableIdentifier(identifier.toString());
        StageOneResult stageOne = createStageOne(tableId, schema, spec, tableProps);
        catalogPropertiesEnvelope = stageOne.envelope(configuration);
        return metadataOnlyTable(stageOne, tableId);
    }

    @Override
    public Table loadTableMetadataOnly(TableIdentifier tableId, String tableLocation,
                                       Map<String, String> tableProps) throws Exception {
        Preconditions.checkState(tableId != null);
        if (!"rest".equals(catalogType)) {
            return loadTable(tableId, tableLocation, tableProps);
        }
        /*
         * Head resolution and the exists-check relay metadata and never touch
         * storage, so stage one is the whole story: no RESTCatalog, no
         * GopherFileIO, no bucket.  loadTable keeps the two-stage path for the
         * callers that go on to read or commit through the returned table.
         */
        StageOneResult stageOne = fetchStageOne(tableId);
        catalogPropertiesEnvelope = stageOne.envelope(configuration);
        return metadataOnlyTable(stageOne, tableId);
    }

    /**
     * Wraps a stage-one response as a {@link Table} that answers every metadata
     * accessor and refuses storage access with {@link #METADATA_ONLY_MESSAGE}.
     */
    private Table metadataOnlyTable(StageOneResult stageOne, TableIdentifier tableId) {
        TableMetadata metadata = stageOne.tableMetadata;
        if (metadata == null) {
            throw new IllegalStateException(
                    "REST load-table response carried no table metadata for " + tableId);
        }
        /*
         * The DTO's TableMetadata may not carry its own file location (that is
         * a sibling response field); the controller reads it off the metadata,
         * so pin it here.
         */
        if (isBlank(metadata.metadataFileLocation())) {
            metadata = TableMetadata.buildFrom(metadata)
                    .withMetadataLocation(stageOne.metadataLocation)
                    .discardChanges()
                    .build();
        }
        return new BaseTable(new MetadataOnlyTableOperations(metadata),
                catalogName + "." + tableId);
    }

    /**
     * TableOperations over metadata this catalog already holds.  Read-only and
     * storage-free: refresh is a no-op (the metadata is pinned to what the
     * catalog answered), commits and storage access fail with directions.
     */
    private static final class MetadataOnlyTableOperations implements TableOperations {
        private final TableMetadata metadata;
        private final FileIO io = new MetadataOnlyFileIO();

        MetadataOnlyTableOperations(TableMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public TableMetadata current() {
            return metadata;
        }

        @Override
        public TableMetadata refresh() {
            return metadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata updated) {
            throw new UnsupportedOperationException(METADATA_ONLY_MESSAGE);
        }

        @Override
        public FileIO io() {
            return io;
        }

        @Override
        public String metadataFileLocation(String fileName) {
            throw new UnsupportedOperationException(METADATA_ONLY_MESSAGE);
        }

        @Override
        public LocationProvider locationProvider() {
            throw new UnsupportedOperationException(METADATA_ONLY_MESSAGE);
        }
    }

    /** A FileIO that names the caller's mistake instead of NPE-ing on it. */
    private static final class MetadataOnlyFileIO implements FileIO {
        @Override
        public InputFile newInputFile(String path) {
            throw new UnsupportedOperationException(METADATA_ONLY_MESSAGE);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException(METADATA_ONLY_MESSAGE);
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException(METADATA_ONLY_MESSAGE);
        }
    }

    @Override
    public Table loadTable(String tableName) throws Exception {
        return loadTable(icebergUtilities.getIcebergTableIdentifier(tableName), null, null);
    }

    @Override
    public Table loadTable(TableIdentifier tableId, String tableLocation,
                           Map<String, String> tableProps) throws Exception {
        Preconditions.checkState(tableId != null);
        if (!"rest".equals(catalogType)) {
            return loadPolarisTable(tableId);
        }

        String requestedMetadataLocation = tableProps == null
                ? null : tableProps.get("metadata_location");
        if ((requestedMetadataLocation == null || requestedMetadataLocation.isEmpty())
                && tableLocation != null && tableLocation.endsWith("metadata.json")) {
            requestedMetadataLocation = tableLocation;
        }

        final int maxAttempts = 3; // initial attempt plus at most two full retries
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            StageOneResult stageOne = fetchStageOne(tableId);
            String bucket = bucketFromLocation(stageOne.tableLocation);
            boolean readYourOwnWrites = requestedMetadataLocation != null
                    && !requestedMetadataLocation.isEmpty();
            if (readYourOwnWrites) {
                String requestedBucket = bucketFromLocation(requestedMetadataLocation);
                if (!bucket.equals(requestedBucket)) {
                    throw new IllegalStateException(
                            "Uncommitted metadata belongs to an older table generation: catalog bucket "
                                    + bucket + " does not match metadata bucket " + requestedBucket);
                }
                bucket = requestedBucket;
            }

            Map<String, String> gopher = selectGopherProperties(
                    stageOne, bucket, configuration, initialGopherProperties);
            RESTCatalog candidate = initializeCatalog(gopher);
            Table table;
            try {
                table = candidate.loadTable(tableId);
            } catch (RuntimeException e) {
                closeRestCatalog(candidate);
                // Do not retry with another storage-credential group.
                throw e;
            }
            String loadedMetadataLocation = metadataLocation(table);
            if (readYourOwnWrites || stageOne.metadataLocation.equals(loadedMetadataLocation)) {
                closeRestCatalog(restCatalog);
                restCatalog = candidate;
                catalogPropertiesEnvelope = stageOne.envelope(configuration);
                return table;
            }
            closeRestCatalog(candidate);
        }
        throw new IllegalStateException("table is being modified concurrently, retry");
    }

    private Table loadPolarisTable(TableIdentifier tableId) throws Exception {
        final int maxAttempts = 5;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                return restCatalog.loadTable(tableId);
            } catch (NoSuchTableException e) {
                throw e;
            } catch (NullPointerException | UncheckedIOException e) {
                if (attempt == maxAttempts - 1) {
                    throw e;
                }
                LOG.warn("Caught Exception during Iceberg table loading: {}: {}", tableId, e);
                sleep(500L);
            }
        }
        throw new Exception("Failed to load Iceberg table with id: " + tableId);
    }

    @Override
    public boolean dropTable(String tableName, boolean purge) {
        throw new UnsupportedOperationException(
                "Iceberg accessor does not support dropTable operation.");
    }

    @Override
    public void renameTable(String tableName, String newTableName) {
        throw new UnsupportedOperationException(
                "Iceberg accessor does not support renameTable operation.");
    }

    @Override
    public boolean createNamespace(String catalogName, String namespaceName,
                                   Map<String, String> properties) throws Exception {
        if (restCatalog == null) {
            restCatalog = initializeCatalog(staticGopherProperties(null));
        }
        restCatalog.createNamespace(Namespace.of(namespaceName),
                properties != null ? properties : new HashMap<String, String>());
        return true;
    }

    /**
     * Closes the catalog this request built, and with it its GopherFileIO.
     *
     * <p>This class never reuses a catalog across tables -- one bucket per
     * table makes that unsound -- so every request leaves one behind. Each
     * carries a GopherFileIO holding a unix socket to the gophermeta worker,
     * and gophermeta already consumes most of its file-descriptor budget at
     * startup, so the abandoned ones accumulate until it hits the limit. What
     * that looks like from SQL is not an error but a hang: gophermeta starts
     * failing its socket reads, and the query waits on a reply that never
     * comes, with no wait_event to show for it.
     */
    @Override
    public void close() {
        RESTCatalog catalog = restCatalog;

        restCatalog = null;
        closeRestCatalog(catalog);
    }

    public Map<String, Object> catalogPropertiesEnvelope() {
        return catalogPropertiesEnvelope == null ? null
                : deepCopyMap(catalogPropertiesEnvelope);
    }

    /**
     * Everything a scan of {@code tableId} needs from the catalog -- a FileIO
     * on the table's bucket plus the envelope the segments read their
     * credentials from -- in at most one catalog request.
     *
     * <p>The bucket has exactly one source: {@code metadataLocation}, the
     * metadata.json the caller is about to read.  It is not asked of the
     * catalog, because the caller already holds it -- every per-table bucket
     * is spelled out in the location string -- and because a bucket parsed
     * from the very file being read cannot disagree with it, which is what
     * made the old cached-bucket generation check necessary.
     *
     * <p>What the catalog is still asked for is credentials, and only when it
     * vends them: with {@code vended_credentials} off the static keys from the
     * user mapping are the credentials, and this call answers without touching
     * the catalog at all.
     *
     * <p>The returned FileIO outlives this catalog on purpose: it is cached
     * across requests, so it must not be registered for per-request release.
     */
    public ScanStorage resolveScanStorage(TableIdentifier tableId, String metadataLocation)
            throws Exception {
        Preconditions.checkState("rest".equals(catalogType),
                "resolveScanStorage is only valid for a generic REST catalog, not " + catalogType);
        String bucket = bucketFromLocation(metadataLocation);
        if (!vendedCredentialsEnabled()) {
            return staticScanStorage(metadataLocation, bucket);
        }
        StageOneResult stageOne = fetchStageOne(tableId);
        Map<String, String> props = catalogProperties(selectGopherProperties(
                stageOne, bucket, configuration, initialGopherProperties));
        return new ScanStorage(newScanFileIO(props), stageOne.envelope(configuration),
                vendedExpiryMs(stageOne));
    }

    /**
     * Scan storage built entirely from local configuration: bucket from the
     * caller's metadata location, credentials from the user mapping's static
     * keys.  Zero catalog requests -- with vending off nothing on this path is
     * the catalog's to answer.  The synthetic stage-one result reuses the
     * credential validation ("vended_credentials is off but no static keys")
     * and produces the same envelope shape the segments already consume; its
     * credentials never expire, so the cache TTL alone bounds the entry.
     */
    private ScanStorage staticScanStorage(String metadataLocation, String bucket)
            throws Exception {
        StageOneResult synthetic = new StageOneResult(metadataLocation,
                tableRootFromMetadataLocation(metadataLocation, bucket),
                Collections.<String, String>emptyMap(), null, null);
        Map<String, String> props = catalogProperties(selectGopherProperties(
                synthetic, bucket, configuration, initialGopherProperties));
        return new ScanStorage(newScanFileIO(props), synthetic.envelope(configuration), 0L);
    }

    /**
     * The table root a metadata location implies: everything before the
     * "/metadata/" directory Iceberg keeps its metadata files in, falling back
     * to the bucket root for a layout that lacks it.  Only used to fill the
     * envelope's table-location on the static-credential path, where no
     * catalog response supplies the real one.
     */
    @VisibleForTesting
    static String tableRootFromMetadataLocation(String metadataLocation, String bucket) {
        int metadataDir = metadataLocation.lastIndexOf("/metadata/");
        if (metadataDir > 0) {
            return metadataLocation.substring(0, metadataDir);
        }
        return "s3://" + bucket;
    }

    /**
     * Builds the FileIO a REST scan reads through, exactly as RESTCatalog
     * would: same implementation class, same properties, same Hadoop conf.
     * Separate method so tests can substitute one without native Gopher.
     */
    protected FileIO newScanFileIO(Map<String, String> catalogProps) {
        return CatalogUtil.loadFileIO(catalogProps.get(CatalogProperties.FILE_IO_IMPL),
                catalogProps, configuration);
    }

    /**
     * When the credentials in {@code stageOne} stop working, or 0 if the
     * catalog did not say. Unparseable is treated as unsaid rather than as an
     * error: the credentials themselves are fine, and the only thing lost is
     * the ability to cache them, which is the safe direction to fail.
     */
    private static long vendedExpiryMs(StageOneResult stageOne) {
        String raw = selectResponseConfig(stageOne).get(SESSION_TOKEN_EXPIRES_AT_MS);
        if (isBlank(raw)) {
            return 0L;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            LOG.warn("Ignoring unparseable {}: {}", SESSION_TOKEN_EXPIRES_AT_MS, raw);
            return 0L;
        }
    }

    /**
     * A REST table's storage as one resolution produced it.
     *
     * <p>Cached across requests, so it carries what a later request needs to
     * decide whether it is still usable: when the credentials behind it
     * expire.  It does not carry a bucket -- the bucket is derived from the
     * metadata location a caller is about to read, and the cache is keyed by
     * it, so an entry can never address a different bucket than its key names.
     */
    public static final class ScanStorage {
        private final FileIO io;
        private final Map<String, Object> envelope;
        private final long credentialsExpireAtMs;

        ScanStorage(FileIO io, Map<String, Object> envelope, long credentialsExpireAtMs) {
            this.io = io;
            this.envelope = envelope;
            this.credentialsExpireAtMs = credentialsExpireAtMs;
        }

        public FileIO io() {
            return io;
        }

        long credentialsExpireAtMs() {
            return credentialsExpireAtMs;
        }

        /**
         * True if the credentials will still be good {@code marginMs} from
         * {@code nowMs}. A catalog that advertises no expiry is taken at its
         * word -- static keys from the user mapping do not expire at all, and
         * a vending catalog that keeps its expiry to itself gives us nothing
         * better to go on than the configured TTL.
         */
        public boolean usableAt(long nowMs, long marginMs) {
            return credentialsExpireAtMs <= 0L || nowMs + marginMs < credentialsExpireAtMs;
        }

        /**
         * The envelope to send to the segments for a scan of
         * {@code metadataLocation}.
         *
         * <p>The metadata location is overwritten rather than kept as stage
         * one found it: a cached envelope was built for whatever head was
         * current when it was resolved, and the segments use that field to
         * check the credential group they were given actually covers the file
         * being read. Naming the head that is really about to be read keeps
         * that check meaningful instead of re-verifying a stale one.
         */
        public Map<String, Object> envelopeFor(String metadataLocation) {
            Map<String, Object> copy = deepCopyMap(envelope);
            if (!isBlank(metadataLocation)) {
                copy.put("metadata-location", metadataLocation);
            }
            return copy;
        }
    }

    @VisibleForTesting
    Map<String, String> catalogAuthProperties() {
        return new HashMap<>(authProperties());
    }

    /** Test hook: the properties a catalog would be initialized with. */
    Map<String, String> catalogRuntimeProperties() {
        return catalogProperties(Collections.<String, String>emptyMap());
    }

    protected StageOneResult fetchStageOne(TableIdentifier tableId) throws Exception {
        try (BareClient bare = newBareClient()) {
            ExtendedLoadTableResponse response = bare.client.get(
                    new ResourcePaths(prefix).table(tableId),
                    Collections.<String, String>emptyMap(),
                    ExtendedLoadTableResponse.class,
                    bare.headers,
                    ErrorHandlers.tableErrorHandler());
            response.validate();
            return new StageOneResult(response.metadataLocation(),
                    response.tableMetadata().location(), response.config(),
                    response.storageCredentials(), response.tableMetadata());
        }
    }

    /**
     * A REST client carrying this catalog's auth, with no FileIO behind it.
     *
     * <p>Both stages that must run before the table's bucket is known use this:
     * loading an existing table's storage information, and creating a table.
     * Building the real catalog instead would construct GopherFileIO, which
     * cannot exist without the bucket these calls are what supply.
     */
    private static final class BareClient implements AutoCloseable {
        final HTTPClient client;
        final Map<String, String> headers;

        BareClient(HTTPClient client, Map<String, String> headers) {
            this.client = client;
            this.headers = headers;
        }

        @Override
        public void close() throws IOException {
            client.close();
        }
    }

    private BareClient newBareClient() throws Exception {
        Map<String, String> catalogProps = catalogProperties(Collections.<String, String>emptyMap());
        HTTPClient.Builder builder = HTTPClient.builder(catalogProps).uri(catalogUrl);
        if (vendedCredentialsEnabled()) {
            builder.withHeader(DELEGATION_HEADER, DELEGATION_VALUE);
        }
        for (Map.Entry<String, String> entry : catalogProps.entrySet()) {
            if (entry.getKey().startsWith("header.")) {
                builder.withHeader(entry.getKey().substring("header.".length()), entry.getValue());
            }
        }

        HTTPClient client = builder.build();
        try {
            Map<String, String> headers = new HashMap<>();
            if (catalogProps.containsKey(OAuth2Properties.CREDENTIAL)) {
                OAuthTokenResponse token = OAuth2Util.fetchToken(
                        client, Collections.<String, String>emptyMap(),
                        catalogProps.get(OAuth2Properties.CREDENTIAL),
                        catalogProps.get(OAuth2Properties.SCOPE));
                headers.putAll(OAuth2Util.authHeaders(token.token()));
            }
            return new BareClient(client, headers);
        } catch (RuntimeException | Error e) {
            closeSuppressing(client, e);
            throw e;
        }
    }

    /**
     * Closes a resource that is being abandoned because {@code failure} happened.
     *
     * <p>The client is half-built and has to go, but closing it must not become
     * the story: the token fetch failing is what the caller needs to hear, and
     * a close that fails while tearing down that same broken connection would
     * otherwise replace it. Attaching it as suppressed keeps both in the stack
     * trace, which is what try-with-resources does for the cases that can use
     * it -- this one cannot, since the client outlives the block on success.
     *
     * <p>Catches Throwable rather than the checked-and-unchecked pair, matching
     * what the compiler generates for try-with-resources (JLS 14.20.3.1). An
     * Error thrown by close() is not the more important of the two: letting it
     * out would discard the failure that stopped the work, which is the exact
     * masking this method exists to stop. An Error raised by the work itself
     * still propagates -- only the teardown's is demoted.
     */
    static void closeSuppressing(Closeable resource, Throwable failure) {
        try {
            resource.close();
        } catch (Throwable closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    protected RESTCatalog newRestCatalog() {
        return new RESTCatalog();
    }

    protected String metadataLocation(Table table) {
        return ((HasTableOperations) table).operations().current().metadataFileLocation();
    }

    protected void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            /*
             * Keep the historical Polaris behaviour of not aborting the retry,
             * but do not eat the signal: Thread.sleep clears the interrupted
             * flag on the way out, so a Tomcat worker interrupted here would
             * carry no trace of it into the rest of the request or into the
             * next request the pool hands it.  Same pattern as
             * IcebergServiceImpl.retryRestCommit.
             */
            Thread.currentThread().interrupt();
        }
    }

    private RESTCatalog initializeCatalog(Map<String, String> gopherProperties) {
        RESTCatalog catalog = newRestCatalog();
        Map<String, String> props = catalogProperties(gopherProperties);
        LOG.info("{} catalog properties: {}", catalogType,
                SensitiveDataRedactor.redact(props));
        CatalogUtil.configureHadoopConf(catalog, configuration);
        catalog.initialize(catalogType, props);
        return catalog;
    }

    /**
     * Should we ask the catalog to vend data-plane credentials?
     *
     * <p>Default yes: per-table vended keys are the point of a catalog that
     * gives every table its own bucket. Operators turn it off when the
     * catalog's vended credentials cannot be used (e.g. rejected by its own
     * object store), in which case the segment falls back to the static keys
     * configured on the user mapping. That is weaker isolation, so it is an
     * explicit opt-out and never an automatic fallback.
     */
    private boolean vendedCredentialsEnabled() {
        return configuration.getBoolean(
                cloud.elastic.dlagent.constants.IcebergConfigConstants
                        .ICEBERG_CATALOG_CONFIG.VENDED_CREDENTIALS, true);
    }

    private Map<String, String> catalogProperties(Map<String, String> gopherProperties) {
        Map<String, String> props = icebergUtilities.composeCatalogProperties(configuration);
        if (gopherProperties != null) {
            props.putAll(gopherProperties);
        }
        if ("rest".equals(catalogType)
                && gopherProperties != null
                && gopherProperties.containsKey("gopher.bucket")) {
            props.put(CatalogProperties.FILE_IO_IMPL,
                    "org.cbdb.iceberg.gopher.client.GopherFileIO");
        }
        if ("rest".equals(catalogType) && vendedCredentialsEnabled()) {
            props.put("header." + DELEGATION_HEADER, DELEGATION_VALUE);
        }
        props.put(CatalogProperties.URI, catalogUrl);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, catalogName);
        if (prefix != null && !prefix.isEmpty()) {
            props.put("prefix", prefix);
        }
        props.putAll(authProperties());
        return props;
    }

    private Map<String, String> staticGopherProperties(String location) {
        Map<String, String> props = new HashMap<>(initialGopherProperties);
        String bucketLocation = firstNonBlank(location,
                configuration.get("warehouse_location_prefix"));
        if (!isBlank(bucketLocation)) {
            props.put("gopher.bucket", bucketFromLocation(bucketLocation));
        }
        String accessKey = configuration.get("access_key_id");
        String secretKey = configuration.get("secret_access_key");
        if (!isBlank(accessKey)) {
            props.put("gopher.access_key", unescape(accessKey));
        }
        if (!isBlank(secretKey)) {
            props.put("gopher.secret_key", unescape(secretKey));
        }
        applyLocalDataAccess(props, configuration.get("region"),
                configuration.get("endpoint"), configuration.get("path_style_access"));
        return props;
    }

    @VisibleForTesting
    static Map<String, String> selectGopherProperties(
            StageOneResult stageOne, String bucket, Configuration configuration,
            Map<String, String> initialGopherProperties) {
        Map<String, String> props = initialGopherProperties == null
                ? new HashMap<String, String>() : new HashMap<>(initialGopherProperties);
        Map<String, String> responseConfig = selectResponseConfig(stageOne);

        boolean responseHasCredentialKey = containsAnyCredentialKey(responseConfig);
        String accessKey;
        String secretKey;
        String sessionToken = null;
        if (responseHasCredentialKey) {
            accessKey = responseConfig.get("s3.access-key-id");
            secretKey = responseConfig.get("s3.secret-access-key");
            sessionToken = responseConfig.get("s3.session-token");
            List<String> missing = new ArrayList<>();
            if (isBlank(accessKey)) {
                missing.add("s3.access-key-id");
            }
            if (isBlank(secretKey)) {
                missing.add("s3.secret-access-key");
            }
            if (!missing.isEmpty()) {
                throw new IllegalArgumentException(
                        "Incomplete vended data-plane credentials; missing " + missing);
            }
        } else {
            accessKey = configuration.get("access_key_id");
            secretKey = configuration.get("secret_access_key");
            if (isBlank(accessKey) && isBlank(secretKey)) {
                throw new IllegalArgumentException(
                        "无可用数据面凭证 (No usable data-plane credentials)");
            }
            List<String> missing = new ArrayList<>();
            if (isBlank(accessKey)) {
                missing.add("access_key_id");
            }
            if (isBlank(secretKey)) {
                missing.add("secret_access_key");
            }
            if (!missing.isEmpty()) {
                throw new IllegalArgumentException(
                        "Incomplete static data-plane credentials; missing " + missing);
            }
        }

        props.put("gopher.ufs_type", "s3");
        props.put("gopher.bucket", bucket);
        props.put("gopher.access_key", unescape(accessKey));
        props.put("gopher.secret_key", unescape(secretKey));
        if (!isBlank(sessionToken)) {
            props.put("gopher.session_token", unescape(sessionToken));
        } else {
            props.remove("gopher.session_token");
        }
        String region = firstNonBlank(responseConfig.get("s3.region"),
                responseConfig.get("client.region"), stageOne.config.get("s3.region"),
                stageOne.config.get("client.region"), configuration.get("region"));
        applyLocalDataAccess(props, region, configuration.get("endpoint"),
                configuration.get("path_style_access"));
        return props;
    }

    private static Map<String, String> selectResponseConfig(StageOneResult stageOne) {
        /*
         * Absent and empty mean the same thing -- the catalog vended nothing --
         * and both are legal responses.  Treating "storage-credentials": [] as
         * a populated list instead sends the caller looking for a prefix match
         * that cannot exist, and reports the miss as the error.
         */
        if (stageOne.storageCredentials == null || stageOne.storageCredentials.isEmpty()) {
            return stageOne.config;
        }
        Map<String, String> selected = null;
        String selectedPrefix = null;
        for (Map<String, Object> candidate : stageOne.storageCredentials) {
            String candidatePrefix = stringValue(candidate.get("prefix"));
            if (candidatePrefix != null && stageOne.tableLocation.startsWith(candidatePrefix)
                    && (selectedPrefix == null || candidatePrefix.length() > selectedPrefix.length())) {
                selectedPrefix = candidatePrefix;
                selected = stringMap(candidate.get("config"));
            }
        }
        if (selected == null) {
            throw new IllegalArgumentException(
                    "No storage-credentials prefix matches table-location "
                            + stageOne.tableLocation);
        }
        if (!stageOne.metadataLocation.startsWith(selectedPrefix)) {
            throw new IllegalArgumentException(
                    "Selected storage-credentials prefix does not cover metadata-location "
                            + stageOne.metadataLocation);
        }
        return selected;
    }

    private static void applyLocalDataAccess(Map<String, String> props, String region,
                                             String endpoint, String pathStyleAccess) {
        if (!isBlank(region)) {
            props.put("gopher.region", unescape(region));
        }
        if (!isBlank(endpoint)) {
            String unescaped = unescape(endpoint);
            props.put("gopher.useHttps", Boolean.toString(unescaped.startsWith("https://")));
            props.put("gopher.endpoint", unescaped.replaceFirst("^https?://", ""));
        }
        if (!isBlank(pathStyleAccess)) {
            boolean pathStyle = Boolean.parseBoolean(pathStyleAccess);
            props.put("gopher.path_style_access", Boolean.toString(pathStyle));
            props.put("gopher.useVirtualHost", Boolean.toString(!pathStyle));
        }
    }

    static String bucketFromLocation(String location) {
        if (isBlank(location)) {
            throw new IllegalArgumentException("Iceberg location is empty; cannot determine bucket");
        }
        try {
            URI uri = new URI(location);
            String bucket = uri.getHost();
            if (isBlank(bucket)) {
                bucket = uri.getAuthority();
            }
            if (isBlank(bucket)) {
                throw new IllegalArgumentException(
                        "Iceberg location has no bucket: " + location);
            }
            return bucket;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid Iceberg location URI: " + location, e);
        }
    }

    protected static String requireConfig(Configuration configuration, String key) {
        String value = configuration == null ? null : configuration.get(key);
        if (isBlank(value)) {
            throw new IllegalArgumentException(
                    "Missing required Iceberg REST catalog configuration: " + key);
        }
        return unescape(value);
    }

    private static boolean containsAnyCredentialKey(Map<String, String> config) {
        return config.containsKey("s3.access-key-id")
                || config.containsKey("s3.secret-access-key")
                || config.containsKey("s3.session-token");
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (!isBlank(value)) {
                return value;
            }
        }
        return null;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String unescape(String value) {
        return value == null ? null : FilePathUtils.unescapeString(value);
    }

    private static String stringValue(Object value) {
        return value == null ? null : value.toString();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> stringMap(Object value) {
        if (!(value instanceof Map)) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            if (entry.getValue() != null) {
                result.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            }
        }
        return result;
    }

    private static void closeRestCatalog(RESTCatalog catalog) {
        if (catalog != null) {
            try {
                catalog.close();
            } catch (Exception e) {
                /*
                 * Exception, not IOException: RESTCatalog.close() tears down an
                 * HTTP client and a FileIO, either of which can fail unchecked.
                 * Both callers run this from a catch block and then rethrow the
                 * failure that brought them there -- an unchecked close would
                 * replace that failure with itself, which is how the reason a
                 * catalog was abandoned goes missing.
                 */
                LOG.warn("Failed to close superseded Iceberg REST catalog", e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> deepCopyMap(Map<String, Object> source) {
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                value = deepCopyMap((Map<String, Object>) value);
            } else if (value instanceof List) {
                List<Object> list = new ArrayList<>();
                for (Object item : (List<?>) value) {
                    list.add(item instanceof Map
                            ? deepCopyMap((Map<String, Object>) item) : item);
                }
                value = list;
            }
            copy.put(entry.getKey(), value);
        }
        return copy;
    }

    /** Immutable data retained from the phase-one load-table response. */
    static class StageOneResult {
        final String metadataLocation;
        final String tableLocation;
        final Map<String, String> config;
        final List<Map<String, Object>> storageCredentials;
        /**
         * The response's full TableMetadata, kept so the metadata-only paths
         * can answer without a second load.  Null when this result was
         * synthesized locally (the static-credential scan bypass), which never
         * feeds a metadata-only table.
         */
        final TableMetadata tableMetadata;

        StageOneResult(String metadataLocation, String tableLocation,
                       Map<String, String> config,
                       List<Map<String, Object>> storageCredentials,
                       TableMetadata tableMetadata) {
            if (isBlank(metadataLocation)) {
                throw new IllegalArgumentException("REST load-table response has no metadata-location");
            }
            if (isBlank(tableLocation)) {
                throw new IllegalArgumentException("REST load-table response has no table-location");
            }
            this.metadataLocation = metadataLocation;
            this.tableLocation = tableLocation;
            this.config = config == null ? Collections.<String, String>emptyMap()
                    : new LinkedHashMap<>(config);
            this.storageCredentials = storageCredentials == null ? null
                    : new ArrayList<>(storageCredentials);
            this.tableMetadata = tableMetadata;
        }

        Map<String, Object> envelope(Configuration configuration) {
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("envelope-version", ENVELOPE_VERSION);
            envelope.put("table-location", tableLocation);
            envelope.put("metadata-location", metadataLocation);
            if (!config.isEmpty()) {
                envelope.put("config", new LinkedHashMap<>(config));
            }
            if (storageCredentials != null) {
                envelope.put("storage-credentials", new ArrayList<>(storageCredentials));
            }
            Map<String, String> local = new LinkedHashMap<>();
            putIfNotBlank(local, "s3.endpoint", configuration.get("endpoint"));
            putIfNotBlank(local, "s3.region", configuration.get("region"));
            putIfNotBlank(local, "s3.path-style-access", configuration.get("path_style_access"));
            /*
             * The static keys go in only when nothing else in this envelope
             * can answer for credentials.  The segment picks its credentials
             * from storage-credentials, then config, then here, and treats a
             * group that is present but unusable as a hard error rather than
             * falling through -- so whenever either of the first two is
             * populated, these keys are unreachable.  Shipping them anyway
             * would broadcast a long-lived, cross-table credential to every
             * segment on every scan, which is precisely what a deployment
             * asking for per-table vended credentials is trying to avoid.
             *
             * Addressing (endpoint / region / path style) is not a credential
             * and stays unconditional: the segment needs it to reach the
             * bucket no matter whose keys open it.
             */
            if (!carriesCredentials()) {
                putIfNotBlank(local, "s3.access-key-id", configuration.get("access_key_id"));
                putIfNotBlank(local, "s3.secret-access-key", configuration.get("secret_access_key"));
            }
            if (!local.isEmpty()) {
                envelope.put("local-data-access", local);
            }
            return envelope;
        }

        /**
         * Does this response carry data-plane credentials of its own -- a
         * vended storage-credentials group, or credentials inlined in config?
         */
        boolean carriesCredentials() {
            return (storageCredentials != null && !storageCredentials.isEmpty())
                    || containsAnyCredentialKey(config);
        }

        private static void putIfNotBlank(Map<String, String> target,
                                          String key, String value) {
            if (!isBlank(value)) {
                target.put(key, unescape(value));
            }
        }
    }

    /**
     * Iceberg 1.3's DTO predates storage-credentials. Extending it lets the
     * same Iceberg REST ObjectMapper retain that forward-compatible field
     * while still deserializing and validating the official load-table DTO.
     */
    public static class ExtendedLoadTableResponse extends LoadTableResponse {
        @JsonProperty("storage-credentials")
        private List<Map<String, Object>> storageCredentials;

        public ExtendedLoadTableResponse() {
        }

        public List<Map<String, Object>> storageCredentials() {
            return storageCredentials;
        }
    }
}
