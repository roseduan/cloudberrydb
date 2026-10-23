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
import cloud.elastic.dlagent.api.model.ConfigurationFactory;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.api.model.ServiceMethodConstants;
import cloud.elastic.dlagent.service.MetricsReporter;
import cloud.elastic.dlagent.service.bridge.Bridge;
import cloud.elastic.dlagent.service.bridge.BridgeFactory;
import cloud.elastic.dlagent.service.security.SecurityService;
import cloud.elastic.dlagent.transport.ClientDisconnectPredicate;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.DataOutputStream;
import java.io.OutputStream;

/**
 * Implementation of the ReadService.
 */
@Named
@Singleton
@Slf4j
public class ReadServiceImpl extends BaseServiceImpl<OperationStats> implements ReadService {
    /**
     * Creates a new instance.
     *
     * @param configurationFactory      configuration factory
     * @param bridgeFactory             bridge factory
     * @param securityService           security service
     * @param metricsReporter           metrics reporter service
     * @param clientDisconnectPredicate transport-specific disconnect test
     */
    @Inject
    public ReadServiceImpl(ConfigurationFactory configurationFactory,
                           BridgeFactory bridgeFactory,
                           SecurityService securityService,
                           MetricsReporter metricsReporter,
                           ClientDisconnectPredicate clientDisconnectPredicate) {
        super("Read", configurationFactory, bridgeFactory, securityService, metricsReporter,
                clientDisconnectPredicate);
    }

    @Override
    public void readData(RequestContext context, OutputStream outputStream) {
        // wrapping the invocation of processData(..) with the error reporting logic
        // since any exception thrown from it must be logged, as this method is called asynchronously
        // and is the last opportunity to log the exception while having MDC logging context defined
        invokeWithErrorHandling(() -> processData(context, () -> writeStream(context, outputStream)));
    }

    protected void call(Bridge bridge, RequestContext context, DataOutputStream dos, OperationStats queryStats) throws Exception{
        switch (context.getMethod()) {
            case ServiceMethodConstants.GET_PARTITIONS:
                queryStats.setOperation(OperationStats.Operation.PARTITION_GET);
                bridge.getPartitions(context.getTableName()).write(dos);
                break;
            case ServiceMethodConstants.GET_FRAGMENTS:
                queryStats.setOperation(OperationStats.Operation.FRAGMENT_GET);
                bridge.getFragments(context.getTableName()).write(dos);
                break;
            case ServiceMethodConstants.GET_SCHEMA:
                queryStats.setOperation(OperationStats.Operation.METADATA_GET);
                bridge.getSchema(context.getTableName()).write(dos);
                break;
            case ServiceMethodConstants.BATCH_APPEND:
                queryStats.setOperation(OperationStats.Operation.BATCH_APPEND);
                bridge.batchAppend().write(dos);
                break;
            case ServiceMethodConstants.GET_OR_CREATE_SCHEMA:
                queryStats.setOperation(OperationStats.Operation.SCHEMA_GET_OR_CREATE);
                bridge.getOrCreateSchema().write(dos);
                break;
            case ServiceMethodConstants.ROW_UPDATE:
                queryStats.setOperation(OperationStats.Operation.ROW_UPDATE);
                bridge.rowUpdate().write(dos);
                break;
            case ServiceMethodConstants.GET_CURRENT_SNAPSHOT_SUMMARY:
                queryStats.setOperation(OperationStats.Operation.CURRENT_SNAPSHOT_SUMMARY_GET);
                bridge.getCurrentSnapshotSummary().write(dos);
                break;
            default:
                throw new UnsupportedOperationException("unknown method:\""+ context.getMethod() + "\".");
        }
    }
}
