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
import cloud.elastic.dlagent.api.error.UnsupportedTypeException;
import cloud.elastic.dlagent.api.model.Metadata;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.api.security.SecureLogin;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import cloud.elastic.dlagent.plugins.hudi.utilities.FilePathUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
public class IcebergCatalogWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapper.class);

    private final Cache<String, IcebergHiveCatalog> catalogCache;

    /**
     * issue #393: per-table FileIO cache for credential-vending / per-request
     * catalogs (Polaris/REST, and hadoop/s3/builtin which build a fresh catalog
     * per request).  Lets a scan reuse the vended FileIO (temporary S3
     * credentials) within the TTL instead of re-resolving it via loadTable.
     * Config-based Hive bypasses this -- it returns its shared FileIO directly
     * from IcebergCatalog.io(), which must never be cached/closed here.
     */
    private final Cache<String, FileIO> vendedFileIoCache;

    /**
     * The same idea as {@link #vendedFileIoCache}, for the generic REST
     * catalog, which needs one thing more than a FileIO: the storage envelope
     * its segments read their credentials from. They have no volume to read
     * them from, so the envelope travels with the fragments, and a FileIO on
     * its own is not enough to answer a scan. Caching the pair is what lets a
     * REST scan skip the catalog entirely, the way a Polaris scan already does.
     */
    private final Cache<String, IcebergRestCatalog.ScanStorage> restScanStorageCache;

    /**
     * How long before the vended credentials expire we stop handing out a
     * cached entry. Covers the flight time of the fragments to the segments
     * and the scan they then run, so a scan that starts on a cache hit does
     * not find its credentials rejected halfway through.
     */
    private static final long CREDENTIAL_EXPIRY_MARGIN_MS = 60_000L;

    private IcebergUtilities icebergUtilities;
    private SecureLogin secureLogin;

    /**
     * Creates the wrapper with the hive catalog cache.  Entries idle for
     * longer than the configured TTL (dlagent.iceberg.hive-catalog-cache-ttl)
     * are evicted, complementing the connection-failure invalidation done by
     * {@link InvalidatingHiveCatalog}: the TTL reclaims idle catalogs and
     * heals configuration drift, the proxy reacts to runtime failures.
     */
    @Autowired
    public IcebergCatalogWrapper (DlServerProperties serverProperties) {
        Duration ttl = serverProperties.getIceberg().getHiveCatalogCacheTtl();
        if (ttl.isZero() || ttl.isNegative()) {
            /*
             * Guava rejects a negative TTL at build time but silently accepts
             * zero, which evicts every entry on its next access -- the cache
             * degenerates to rebuilding the HMS catalog (and its client pool)
             * on every request.  Fail startup with a clear message instead.
             */
            throw new IllegalArgumentException(
                    "dlagent.iceberg.hive-catalog-cache-ttl must be positive, got: " + ttl);
        }
        LOG.info("Creating iceberg catalogCache with idle TTL {} ...", ttl);
        catalogCache = CacheBuilder.newBuilder()
                .expireAfterAccess(ttl.toMillis(), TimeUnit.MILLISECONDS)
                .removalListener((RemovalListener<String, IcebergHiveCatalog>) notification -> {
                    LOG.info("Removed iceberg catalogCache entry key={} cause={}",
                            notification.getKey(),
                            notification.getCause());
                    IcebergHiveCatalog evicted = notification.getValue();
                    if (evicted != null) {
                        try {
                            evicted.close();
                        } catch (Exception e) {
                            LOG.warn("Error closing evicted IcebergHiveCatalog for key={}",
                                    notification.getKey(), e);
                        }
                    }
                })
                .build();

        Duration fileIoTtl = serverProperties.getIceberg().getFileIoCacheTtl();
        if (fileIoTtl.isZero() || fileIoTtl.isNegative()) {
            throw new IllegalArgumentException(
                    "dlagent.iceberg.file-io-cache-ttl must be positive, got: " + fileIoTtl);
        }
        LOG.info("Creating iceberg vendedFileIoCache with TTL {} ...", fileIoTtl);
        vendedFileIoCache = CacheBuilder.newBuilder()
                .expireAfterWrite(fileIoTtl.toMillis(), TimeUnit.MILLISECONDS)
                .removalListener((RemovalListener<String, FileIO>) notification -> {
                    /*
                     * Deliberately do NOT close the evicted FileIO here.  This
                     * wrapper is a singleton, so getScanFileIO() hands the same
                     * cached FileIO to every concurrent scan; because eviction is
                     * driven by a hard expireAfterWrite TTL (not idle time), an
                     * in-flight scan on one thread can still be reading through a
                     * FileIO that another thread's access has just expired.
                     * Closing it would yank the FileIO out from under that scan
                     * (use-after-close).  Instead let the entry become
                     * unreachable and GC reclaim it once the last scan releases
                     * its reference -- the same lifecycle the scan path relied on
                     * before this cache existed (getFragmentsByUncommittedMetadata
                     * never closed its per-scan FileIO either).
                     */
                    LOG.debug("Evicted vendedFileIoCache entry key={} cause={}",
                            notification.getKey(), notification.getCause());
                })
                .build();

        /*
         * Shares the FileIO cache's TTL, and for the same reason: it holds a
         * FileIO built on credentials that expire.  The TTL is the upper bound
         * -- how long a revoked authorization may go unnoticed -- while the
         * lower bound comes from the credentials themselves, which each entry
         * carries and getRestScanStorage checks.  The eviction listener does
         * not close either, exactly as above.
         */
        LOG.info("Creating iceberg restScanStorageCache with TTL {} ...", fileIoTtl);
        restScanStorageCache = CacheBuilder.newBuilder()
                .expireAfterWrite(fileIoTtl.toMillis(), TimeUnit.MILLISECONDS)
                .removalListener((RemovalListener<String, IcebergRestCatalog.ScanStorage>) notification ->
                        LOG.debug("Evicted restScanStorageCache entry key={} cause={}",
                                notification.getKey(), notification.getCause()))
                .build();
    }

    /**
     * Returns the FileIO to use for scanning {@code context}'s table (issue
     * #393), avoiding the loadTable()/external-catalog round-trip whose only
     * purpose is FileIO where possible.
     *
     * <p>Config-based catalogs (Hive) hand out their shared, already-built
     * FileIO directly.  Credential-vending / per-request catalogs
     * (Polaris/REST, hadoop/s3, builtin) have no shareable config FileIO, so
     * their FileIO is cached per connection+table with a TTL and re-resolved
     * only when it expires.
     */
    public FileIO getScanFileIO(RequestContext context) throws Exception {
        /*
         * Config-based catalogs (Hive) expose a shared, already-built FileIO via
         * io(); hand it out directly.  Hive is the only catalog type that
         * overrides io() to non-null and constructing it is cheap (the
         * underlying HMS catalog is itself cached), so gate on the catalog type
         * -- probing io() generically would mean building the (expensive)
         * vending catalog just to discover io() == null on every cache hit.
         */
        if ("hive".equals(context.getCatalogType())) {
            FileIO configIo = getIcebergCatalog(context).io();
            if (configIo != null) {
                return configIo;
            }
        }

        /*
         * Credential-vending / per-request catalogs (Polaris/REST, s3, hadoop,
         * builtin) have no shareable config FileIO.  Cache the vended FileIO per
         * connection+table and build the catalog only inside the loader, on a
         * miss: IcebergPolarisCatalog's constructor runs RESTCatalog.initialize
         * (a /v1/config + OAuth2 round-trip), so constructing it on every cache
         * hit would defeat the cache for Polaris tables at high scan rates.
         */
        String key = formCatalogCacheKey(context) + "|" + context.getDataSource();
        try {
            /*
             * Built untracked, on purpose.  The FileIO produced here is cached
             * across requests, so the catalog behind it is not a per-request
             * catalog: registering it would have releaseRequestCatalogs() close
             * it -- and its FileIO with it -- at the end of the request that
             * happened to miss the cache, handing every later scan a closed
             * FileIO.  Its lifetime is the cache entry's, released the way the
             * eviction listener above describes.
             */
            return vendedFileIoCache.get(key,
                    () -> newIcebergCatalog(context).loadTable(context.getDataSource()).io());
        } catch (ExecutionException | UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new DlRuntimeException(
                    "Failed to resolve FileIO for " + context.getDataSource(), e);
        }
    }

    /**
     * Returns the storage a generic REST scan of {@code context}'s table needs
     * -- a FileIO on the table's bucket and the envelope for the segments --
     * resolving it from the catalog only when the cached one cannot be used.
     *
     * <p>This is the REST equivalent of {@link #getScanFileIO}, and exists
     * separately because a REST scan needs the envelope too: the segments have
     * no volume to read credentials from, so a bare FileIO leaves them unable
     * to read anything. That is why REST could not simply use the FileIO cache.
     *
     * <p>The bucket the storage addresses comes from {@code metadataLocation}
     * -- the metadata.json about to be read names its own bucket -- and is
     * part of the cache key. That is the whole generation story: a table
     * dropped and recreated under the same name lives in a new bucket, so its
     * new head simply keys a new entry, and the old generation's entry ages
     * out untouched instead of being validated against.
     *
     * <p>A cached entry is used only if its credentials outlast
     * {@link #CREDENTIAL_EXPIRY_MARGIN_MS}.
     *
     * @param metadataLocation the metadata.json about to be read; required,
     *                         because the bucket has no other source
     */
    public IcebergRestCatalog.ScanStorage getRestScanStorage(
            RequestContext context, String metadataLocation) throws Exception {
        if (StringUtils.isBlank(metadataLocation)) {
            throw new DlRuntimeException(
                    "REST scan storage needs the metadata location about to be read; none was given for "
                            + context.getDataSource());
        }
        String bucket = IcebergRestCatalog.bucketFromLocation(metadataLocation);
        String key = formRestScanCacheKey(context) + "|" + bucket;

        IcebergRestCatalog.ScanStorage storage = restScanStorageCache.getIfPresent(key);
        if (storage != null
                && storage.usableAt(System.currentTimeMillis(), CREDENTIAL_EXPIRY_MARGIN_MS)) {
            return storage;
        }

        /*
         * Removing the unusable entry is what makes the reload happen at all,
         * not merely an optimisation: get(key, Callable) runs the loader only
         * when the key is absent, so with the stale value still in place every
         * caller would get that value straight back and nothing would ever be
         * refreshed.  Once the key is gone the loader does run, and Guava's
         * per-key lock leaves exactly one caller executing it while the rest
         * wait for its result.
         */
        restScanStorageCache.invalidate(key);
        try {
            /*
             * Deliberately returned even when its credentials expire inside the
             * margin: they are valid now, and this scan is about to use them
             * immediately.  A catalog that vends credentials that short simply
             * gets no caching -- the next request finds the entry unusable and
             * resolves again -- which is the behaviour it had before this cache.
             */
            return restScanStorageCache.get(key,
                    () -> resolveRestScanStorage(context, metadataLocation));
        } catch (ExecutionException | UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new DlRuntimeException(
                    "Failed to resolve REST scan storage for " + context.getDataSource(), e);
        }
    }

    /**
     * Resolves a REST table's storage from the catalog (or, with credential
     * vending off, entirely from local configuration).
     *
     * <p>Built untracked, like {@link #getScanFileIO}'s loader and for the same
     * reason: what comes back is cached across requests, so registering it
     * would have {@link #releaseRequestCatalogs()} close it at the end of
     * whichever request missed the cache.  Nothing is leaked by skipping that
     * -- a generic REST catalog builds no RESTCatalog until something asks it
     * to load a table, and this path never does.
     */
    private IcebergRestCatalog.ScanStorage resolveRestScanStorage(
            RequestContext context, String metadataLocation) throws Exception {
        IcebergCatalog catalog = newIcebergCatalog(context);
        if (!(catalog instanceof IcebergRestCatalog)) {
            throw new DlRuntimeException("Catalog type " + context.getCatalogType()
                    + " does not resolve REST scan storage");
        }
        return ((IcebergRestCatalog) catalog).resolveScanStorage(
                icebergUtilities.getIcebergTableIdentifier(context.getDataSource()),
                metadataLocation);
    }

    /**
     * Everything a REST catalog reads off the FOREIGN SERVER / FOREIGN CATALOG
     * to decide where it connects and as whom.
     *
     * <p>{@link #CONNECTION_CONFIG_KEYS} covers the Hive metastore connection
     * and nothing else, so on its own it would let an {@code ALTER SERVER} that
     * repoints the catalog or rotates its keys go unnoticed for a whole TTL --
     * a regression against the behaviour this cache replaced, where every REST
     * scan rebuilt its catalog from the request's configuration and therefore
     * picked such a change up immediately.  Listing them explicitly rather than
     * hashing the whole Configuration for the reason given on
     * CONNECTION_CONFIG_KEYS: it also carries request-scoped properties, which
     * would fragment the cache per request and defeat it entirely.
     */
    private static final String[] REST_CONNECTION_CONFIG_KEYS = {
            "url",
            "polaris_server_url",
            "prefix",
            "warehouse_location_prefix",
            "auth_method",
            "region",
            "signing_name",
            "access_key_id",
            "secret_access_key",
            "client_id",
            "client_secret",
            "scope",
            "endpoint",
            "path_style_access",
            cloud.elastic.dlagent.constants.IcebergConfigConstants
                    .ICEBERG_CATALOG_CONFIG.VENDED_CREDENTIALS,
            cloud.elastic.dlagent.constants.IcebergConfigConstants
                    .ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_REALM,
    };

    /*
     * Adding a REST catalog option means registering it in two fixed lists,
     * and neither one fails loudly when you forget: omit it from
     * IcebergRestController.setRestInline() and the option never reaches the
     * agent at all; omit it from the array above and two catalogs that differ
     * only by that option hash to the same key, so the second silently reuses
     * the first one's storage -- for polaris_server_realm, credentials vended
     * under another tenant's realm.  Both mistakes were made on this option.
     */

    /**
     * Cache key for {@link #getRestScanStorage}: the catalog connection, the
     * REST settings above, and the table -- one bucket per table makes the
     * last one load-bearing rather than merely a refinement.
     */
    private String formRestScanCacheKey(RequestContext context) {
        Configuration configuration = context.getConfiguration();
        StringBuilder restInfo = new StringBuilder();
        for (String key : REST_CONNECTION_CONFIG_KEYS) {
            appendKeyValue(restInfo, key, configuration == null ? null : configuration.get(key));
        }
        return formCatalogCacheKey(context)
                + "|" + Hashing.sha256()
                        .hashString(restInfo, StandardCharsets.UTF_8).toString()
                + "|" + context.getDataSource();
    }

    @VisibleForTesting
    Cache<String, IcebergRestCatalog.ScanStorage> getRestScanStorageCache() {
        return restScanStorageCache;
    }

    /**
     * Sets the {@link IcebergUtilities} object
     *
     * @param icebergUtilities the iceberg utilities object
     */
    @Autowired
    public void setIcebergUtilities(IcebergUtilities icebergUtilities) {
        this.icebergUtilities = icebergUtilities;
    }

    /**
     * Sets the {@link SecureLogin} object
     *
     * @param secureLogin the secure login object
     */
    @Autowired
    public void setSecureLogin(SecureLogin secureLogin) {
        this.secureLogin = secureLogin;
    }

    /**
     * Catalogs handed out during the request running on this thread.
     *
     * <p>Every branch of {@link #getIcebergCatalog} except hive builds a fresh
     * catalog per call, and several of them (rest and polaris in particular)
     * hold a FileIO with an open connection behind it. No caller closed them,
     * so each request leaked one for the life of the agent process. Rather
     * than push try-with-resources onto the fifteen-odd call sites -- and rely
     * on every future one remembering -- the wrapper records what it hands out
     * and {@link IcebergCatalogReleaseFilter} closes the lot when the request
     * ends. That is the correct boundary anyway: the {@code Table} a caller
     * gets back reads through the catalog's FileIO, so the catalog must
     * outlive the whole call chain and nothing needs it afterwards.
     */
    private static final ThreadLocal<List<IcebergCatalog>> REQUEST_CATALOGS =
            ThreadLocal.withInitial(ArrayList::new);

    /**
     * Closes and forgets every catalog handed out during the current request.
     *
     * <p>Called from a {@code finally}, so one catalog failing to close must
     * not stop the others: each failure is logged and the loop continues.
     * Idempotent -- a request that asked for no catalog closes nothing.
     */
    public void releaseRequestCatalogs() {
        List<IcebergCatalog> catalogs = REQUEST_CATALOGS.get();

        try {
            for (IcebergCatalog catalog : catalogs) {
                try {
                    catalog.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close per-request iceberg catalog {}",
                            catalog.getClass().getSimpleName(), e);
                }
            }
        } finally {
            /*
             * Remove rather than clear: Tomcat reuses worker threads, and a
             * retained ThreadLocal keeps its (classloader-referencing) value
             * alive for the life of the pool.
             */
            REQUEST_CATALOGS.remove();
        }
    }

    private IcebergCatalog trackForRequest(IcebergCatalog catalog) {
        REQUEST_CATALOGS.get().add(catalog);
        return catalog;
    }

    /**
     * Returns the corresponding catalog implementation.
     */
    public IcebergCatalog getIcebergCatalog(RequestContext context) throws Exception {
        return trackForRequest(newIcebergCatalog(context));
    }

    /**
     * Builds a catalog without registering it for per-request release.
     *
     * <p>Protected rather than private so tests can substitute a catalog whose
     * HTTP and native-storage edges are stubbed, the same seam
     * {@link IcebergRestCatalog} offers for its own two.
     */
    protected IcebergCatalog newIcebergCatalog(RequestContext context) throws Exception {
        String catalogType = context.getCatalogType();
        switch (catalogType) {
            case "s3":
                return new IcebergS3Catalog(FilePathUtils.unescapeString(context.getPath()),
                    icebergUtilities, context.getConfiguration(), context.getGopherProperties());
            case "hadoop":
                return new IcebergHadoopCatalog(FilePathUtils.unescapeString(context.getPath()),
                        icebergUtilities, context.getConfiguration(), context.getGopherProperties());
            case "hive":
                // Wrap the cached IcebergHiveCatalog in a proxy that invalidates this
                // cache (and the layer-2 DlCachedClientPool) on connection-class
                // failures, so subsequent requests rebuild the catalog instead of
                // reusing a broken one.  Service-layer and fetcher-layer callers
                // need no changes -- the proxy intercepts every catalog method.
                return new InvalidatingHiveCatalog(getHiveCatalog(context), this, context);
            case "polaris":
                return new IcebergPolarisCatalog(
                    context.getDataSource(),
                    icebergUtilities,
                    context.getConfiguration(),
                    context.getGopherProperties());
            case "rest":
                return new IcebergRestCatalog(
                    context.getDataSource(),
                    icebergUtilities,
                    context.getConfiguration(),
                    context.getGopherProperties());
            case "builtin":
                return new IcebergBuildInCatalog(context.getPath(),
                    icebergUtilities,
                    context.getConfiguration(),
                    secureLogin,
                    context.getServerName(),
                    context.getConfig(),
                    context.getGopherProperties(),
                    context.getBuildInCatalogProperties());
            default:
                throw new DlRuntimeException("Unexpected catalog type: " + catalogType);
        }
    }

    /**
     * Hadoop/Hive configuration keys that define the metastore connection
     * and its authentication.  Deliberately a fixed list: the per-request
     * Configuration also carries request-scoped properties (e.g. the
     * dlagent.session.user synthetic property), and hashing the whole
     * Configuration would fragment the cache per request.
     */
    private static final String[] CONNECTION_CONFIG_KEYS = {
            HiveConf.ConfVars.METASTOREURIS.varname,
            "hadoop.security.authentication",
            "hive.metastore.sasl.enabled",
            "hive.metastore.kerberos.principal",
            SecureLogin.CONFIG_KEY_SERVICE_PRINCIPAL,
            SecureLogin.CONFIG_KEY_SERVICE_KEYTAB,
    };

    /**
     * Forms the hive catalog cache key by hashing the full connection
     * definition: server name, server config directory, warehouse path,
     * metastore URI, authentication settings and all gopher (storage)
     * properties such as endpoint/bucket/credentials/region.  Any change to
     * the connection configuration therefore maps to a new cache entry, so a
     * reconfigured server picks up its new settings without an agent restart
     * (the stale entry simply idles out via the cache TTL).  Hashing also
     * keeps credentials out of the log lines that print the key.
     */
    @VisibleForTesting
    String formCatalogCacheKey(RequestContext context) {
        Configuration configuration = context.getConfiguration();
        StringBuilder connectionInfo = new StringBuilder();
        appendKeyValue(connectionInfo, "serverName", context.getServerName());
        appendKeyValue(connectionInfo, "config", context.getConfig());
        appendKeyValue(connectionInfo, "warehouse", context.getPath());
        for (String key : CONNECTION_CONFIG_KEYS) {
            appendKeyValue(connectionInfo, key, configuration == null ? null : configuration.get(key));
        }
        // TreeMap orders the storage properties so the hash is stable.
        Map<String, String> gopherProperties = context.getGopherProperties();
        if (gopherProperties != null) {
            for (Map.Entry<String, String> entry : new TreeMap<>(gopherProperties).entrySet()) {
                appendKeyValue(connectionInfo, entry.getKey(), entry.getValue());
            }
        }
        return Hashing.sha256()
                .hashString(connectionInfo, StandardCharsets.UTF_8)
                .toString();
    }

    private static void appendKeyValue(StringBuilder sb, String key, String value) {
        // '\n' cannot appear in these one-line settings, so it unambiguously
        // delimits the key=value pairs; null and "" hash differently.
        sb.append(key).append('=').append(value).append('\n');
    }

    @VisibleForTesting
    Cache<String, IcebergHiveCatalog> getCatalogCache() {
        return catalogCache;
    }

    private IcebergHiveCatalog getHiveCatalog(RequestContext context) throws IOException {
        final String cacheKey = formCatalogCacheKey(context);

        try {
            return catalogCache.get(cacheKey, () -> {
                        LOG.debug("Caching hive catalog with key={}", cacheKey);

                        IcebergHiveCatalog hiveCatalog = new IcebergHiveCatalog(context.getPath(),
                                icebergUtilities,
                                context.getConfiguration(),
                                secureLogin,
                                context.getServerName(),
                                context.getConfig(),
                                context.getGopherProperties());

                        LOG.info("Returning hive catalog for {} [user={}, table={}.{}, resource={}, path={}, " +
                                        "profile={}, predicate {}available]",
                                cacheKey,
                                context.getUser(),
                                context.getSchemaName(),
                                context.getTableName(),
                                context.getDataSource(),
                                context.getPath(),
                                context.getProfile(),
                                context.hasFilter() ? "" : "un");

                        return hiveCatalog;
                    });
        } catch (UncheckedExecutionException | ExecutionException e) {
            // Unwrap the error
            Exception exception = e.getCause() != null ? (Exception) e.getCause() : e;
            if (exception instanceof IOException)
                throw (IOException) exception;
            throw new IOException(exception);
        }
    }

    /**
     * Drop the cached hive catalog for the given context.  Triggered by
     * {@link InvalidatingHiveCatalog} when a catalog call surfaces a
     * connection-class failure.  Idempotent: invalidating a missing key is a
     * no-op.  The Guava removal listener will synchronously call
     * {@link IcebergHiveCatalog#close()} on the evicted value, which in turn
     * evicts the layer-2 {@code DlCachedClientPool} entry tied to the same
     * metastore URI.
     */
    public void invalidateHiveCatalog(RequestContext context) {
        String key = formCatalogCacheKey(context);
        LOG.info("Invalidating iceberg catalog cache for key={}", key);
        catalogCache.invalidate(key);
    }

    /**
     * True iff {@code t}'s cause chain contains an exception that indicates a
     * connection-class failure between dlagent and the Hive metastore.  We
     * deliberately keep this narrow so that schema / table / namespace errors
     * (NoSuchTable, AlreadyExists, ValidationException, ...) do not cause
     * cache thrash.  Match types:
     *
     * <ul>
     *   <li>{@code TTransportException}, {@code ConnectException},
     *       {@code UnknownHostException}, {@code SocketTimeoutException}
     *       -- thrift/socket transport gave up</li>
     *   <li>{@code ServiceUnavailableException} -- iceberg's own connect
     *       wrapper</li>
     *   <li>{@code RuntimeMetaException} only when its message explicitly says
     *       "Failed to connect" / "Failed to reconnect" -- {@code DlHiveClientPool#newClient}
     *       wraps real connection errors that way; other RuntimeMetaException
     *       paths cover non-connection HMS errors</li>
     * </ul>
     */
    public static boolean isConnectionFailure(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof org.apache.thrift.transport.TTransportException) return true;
            if (c instanceof java.net.ConnectException) return true;
            if (c instanceof java.net.UnknownHostException) return true;
            if (c instanceof java.net.SocketTimeoutException) return true;
            if (c instanceof org.apache.iceberg.exceptions.ServiceUnavailableException) return true;
            if (c instanceof org.apache.iceberg.hive.RuntimeMetaException) {
                String msg = c.getMessage();
                if (msg != null
                        && (msg.contains("Failed to connect") || msg.contains("Failed to reconnect"))) {
                    return true;
                }
            }
        }
        return false;
    }
}
