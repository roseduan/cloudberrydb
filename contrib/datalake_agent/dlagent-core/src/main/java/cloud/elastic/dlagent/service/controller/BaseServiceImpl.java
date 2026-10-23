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

package cloud.elastic.dlagent.service.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

import com.google.common.io.CountingOutputStream;

import cloud.elastic.dlagent.api.model.ConfigurationFactory;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.service.MetricsReporter;
import cloud.elastic.dlagent.service.bridge.Bridge;
import cloud.elastic.dlagent.service.bridge.BridgeFactory;
import cloud.elastic.dlagent.service.security.SecurityService;
import cloud.elastic.dlagent.transport.ClientDisconnectPredicate;


import java.io.DataOutputStream;
import java.io.OutputStream;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.time.Instant;

/**
 * Base abstract implementation of the Service class, provides means to execute an operation
 * using provided request context and the identity determined by the security service.
 */
@Slf4j
public abstract class BaseServiceImpl<T> extends DlErrorReporter<T> {

    protected final MetricsReporter metricsReporter;
    private final String serviceName;
    private final ConfigurationFactory configurationFactory;
    private final BridgeFactory bridgeFactory;
    private final SecurityService securityService;
    private final ClientDisconnectPredicate clientDisconnectPredicate;

    /**
     * Creates a new instance of the service with auto-wired dependencies.
     *
     * @param serviceName               name of the service
     * @param configurationFactory      configuration factory
     * @param bridgeFactory             bridge factory
     * @param securityService           security service
     * @param metricsReporter           metrics reporter service
     * @param clientDisconnectPredicate transport-specific disconnect test
     */
    protected BaseServiceImpl(String serviceName,
                              ConfigurationFactory configurationFactory,
                              BridgeFactory bridgeFactory,
                              SecurityService securityService,
                              MetricsReporter metricsReporter,
                              ClientDisconnectPredicate clientDisconnectPredicate) {
        super(clientDisconnectPredicate);
        this.clientDisconnectPredicate = clientDisconnectPredicate;
        this.serviceName = serviceName;
        this.configurationFactory = configurationFactory;
        this.bridgeFactory = bridgeFactory;
        this.securityService = securityService;
        this.metricsReporter = metricsReporter;
    }

    /**
     * Executes an action with the identity determined by the dlagent security service.
     *
     * @param context request context
     * @param action  action to execute
     * @return operation statistics
     */
    protected OperationStats processData(RequestContext context, PrivilegedAction<OperationResult> action) throws Exception {
        log.debug("{} service is called for resource {} using profile {}",
                serviceName, context.getDataSource(), context.getProfile());

        // initialize iceberg configuration
        configurationFactory.initIcebergConfigFormJson(context);

        // Gopher filesystem is an Iceberg-specific access path. For non-Iceberg
        // profiles (hive, hudi, ...) override the process-wide gopher.enabled=true
        // (set in application.properties by datalake_proxy) so Hadoop FS access
        // falls back to the standard filesystem implementations instead of
        // org.cbdb.iceberg.gopher.fs.GopherFileSystem.
        String profile = context.getProfile();
        if (profile == null || !profile.toLowerCase().contains("iceberg")) {
            context.addOption("gopher.enabled", "false");
        }

        // initialize the configuration for this request
        Configuration configuration = configurationFactory.
                initConfiguration(context.getCatalogType(),
                        context.getConfig(),
                        context.getServerName(),
                        context.getUser(),
                        context.getPath(),
                        context.getOptions());
        context.setConfiguration(configuration);

        Instant startTime = Instant.now();

        // execute processing action with a proper identity
        OperationResult result = securityService.doAs(context, action);

        // obtain results after executing the action
        OperationStats stats = result.getStats();
        Exception exception = result.getException();
        String status = (exception == null) ? "Completed" :
                clientDisconnectPredicate.isClientDisconnect(exception) ? "Aborted" : "Failed";

        // log action status and stats
        long durationMs = Duration.between(startTime, Instant.now()).toMillis();

        log.info("{} {} operation [{} ms]{}",
                status,
                stats.getOperation().name().toLowerCase(),
                durationMs,
                (exception == null) ? "" : " for " + result.getSourceName());

        // re-throw the exception if the operation failed
        if (exception != null) {
            throw exception;
        }

        // return operation stats
        return stats;
    }

    /**
     * Returns a new Bridge instance based on the current context.
     *
     * @param context request context
     * @return an instance of the bridge to use
     */
    protected Bridge getBridge(RequestContext context) {
        return bridgeFactory.getBridge(context);
    }

    protected OperationResult writeStream(RequestContext context, OutputStream outputStream) {
        OperationStats queryStats = new OperationStats(metricsReporter, context);
        OperationResult queryResult = new OperationResult();

        // dataStream (and outputStream as the result) will close automatically at the end of the try block
        CountingOutputStream countingOutputStream = new CountingOutputStream(outputStream);
        String sourceName = context.getDataSource();
        try {
            processRequest(countingOutputStream, context, queryStats);
        } catch (Exception e) {
            // the exception is not re-thrown but passed to the caller in the queryResult so that
            // the caller has a chance to inspect / report query stats before re-throwing the exception
            queryResult.setException(e);
            queryResult.setSourceName(sourceName);
        } finally {
            queryResult.setStats(queryStats);
        }

        return queryResult;
    }

    protected void processRequest(CountingOutputStream countingOutputStream,
                                RequestContext context,
                                OperationStats queryStats) throws Exception {
        DataOutputStream dos = new DataOutputStream(countingOutputStream);
        boolean success = false;
        Instant startTime = Instant.now();
        Bridge bridge = null;

        try {
            bridge = getBridge(context);
            log.debug("Starting processing request: methodName {} resource {} tableName {}",
                    context.getMethod(), context.getDataSource(), context.getTableName());
            bridge.open();

            call(bridge, context, dos, queryStats);
            success = true;
        } finally {
            if (bridge != null) {
                try {
                    bridge.close();
                } catch (Exception e) {
                    log.warn("Ignoring error encountered during bridge.close()", e);
                }
            }

            Duration duration = Duration.between(startTime, Instant.now());
            queryStats.reportCompletedRpcCount();
            log.debug("Finished processing request: methodName {} resource {} tableName in {} ms.",
                    context.getMethod(), context.getDataSource(), duration.toMillis());
            metricsReporter.reportTimer(queryStats.getOperation().getMetric(), duration, context, success);
        }
    }

    protected abstract void call(Bridge bridge, RequestContext context, DataOutputStream dos, OperationStats queryStats) throws Exception;

}
