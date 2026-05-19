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

import cloud.elastic.dlagent.api.error.DlRuntimeException;
import cloud.elastic.dlagent.api.error.UnsupportedTypeException;
import cloud.elastic.dlagent.api.model.Metadata;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.api.security.SecureLogin;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import cloud.elastic.dlagent.plugins.hudi.utilities.FilePathUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

@Component
public class IcebergCatalogWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapper.class);

    private final Cache<String, IcebergHiveCatalog> catalogCache;

    private IcebergUtilities icebergUtilities;
    private SecureLogin secureLogin;

    public IcebergCatalogWrapper () {
        LOG.info("Creating iceberg catalogCache ...");
        catalogCache = CacheBuilder.newBuilder()
                .removalListener((RemovalListener<String, IcebergHiveCatalog>) notification ->
                        LOG.debug("Removed iceberg catalogCache entry for key {} with cause {}",
                                notification.getKey(),
                                notification.getCause().toString()))
                .build();
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
                return getHiveCatalog(context);
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

    private String formCatalogCacheKey(RequestContext context) {
        return String.format("%s:%s", context.getServerName(),
                context.getConfiguration().get(HiveConf.ConfVars.METASTOREURIS.varname));
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
}
