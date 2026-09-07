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
            return vendedFileIoCache.get(key,
                    () -> getIcebergCatalog(context).loadTable(context.getDataSource()).io());
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
     * Returns the corresponding catalog implementation.
     */
    public IcebergCatalog getIcebergCatalog(RequestContext context) throws Exception {
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
