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


package cloud.elastic.dlagent.service.bridge;

import cloud.elastic.dlagent.api.io.Writable;
import cloud.elastic.dlagent.api.model.MetadataFetcher;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.service.utilities.BasePluginFactory;
import cloud.elastic.dlagent.service.utilities.GSSFailureHandler;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import cloud.elastic.dlagent.api.io.BufferWritable;

import java.nio.charset.StandardCharsets;

/**
 * ReadBridge class creates appropriate accessor and resolver. It will then
 * create the correct output conversion class (e.g. Text or GPDBWritable) and
 * get records from accessor, let resolver deserialize them and serialize them
 * again using the output conversion class. <br>
 * The class handles BadRecordException and other exception type and marks the
 * record as invalid for GPDB.
 */
public class ReadBridge extends BaseBridge {
    private MetadataFetcher accessor;

    public ReadBridge(BasePluginFactory pluginFactory, RequestContext context, GSSFailureHandler failureHandler) {
        super(pluginFactory, context, failureHandler);
        String accessorClassName = context.getMetadataFetcher();
        LOG.debug("Creating accessor '{}'", accessorClassName);

        this.accessor = pluginFactory.getPlugin(context, accessorClassName);
    }

    /**
     * A function that is called by the failure handler before a new retry attempt after a failure.
     * It re-creates the accessor from the factory in case the accessor implementation is not idempotent.
     */
    protected void beforeRetryCallback() {
        String accessorClassName = context.getMetadataFetcher();
        LOG.debug("Creating accessor '{}'", accessorClassName);
        this.accessor = pluginFactory.getPlugin(context, accessorClassName);
    }

    /**
     * Accesses the underlying data source.
     */
    @Override
    public boolean open() throws Exception {
        // using lambda and not a method reference accessor::openForRead as the accessor will be changed by the retry function
        return failureHandler.execute(context.getConfiguration(), "open", () -> accessor.open(), this::beforeRetryCallback);
    }

    @Override
    public Writable getFragments(String pattern) throws Exception {
        // we checked before that outputQueue is empty, so we can override it.
        return makeOutput(accessor.getFragments(pattern));
    }

    @Override
    public Writable getPartitions(String pattern) throws Exception {
        // we checked before that outputQueue is empty, so we can override it.
        return makeOutput(accessor.getPartitions(pattern));
    }

    @Override
    public Writable getSchema(String pattern) throws Exception {
        // we checked before that outputQueue is empty, so we can override it.
        return makeOutput(accessor.getSchema(pattern));
    }

    @Override
    public Writable batchAppend() throws Exception {
        return makeOutput(accessor.batchAppend());
    }

    @Override
    public Writable getOrCreateSchema() throws Exception {
        return makeOutput(accessor.getOrCreateSchema());
    }

    @Override
    public Writable rowUpdate() throws Exception {
        return makeOutput(accessor.rowUpdate());
    }

    @Override
    public Writable getCurrentSnapshotSummary() throws Exception {
        return makeOutput(accessor.getCurrentSnapshotSummary());
    }


    /**
     * Close the underlying resource
     */
    public void close() throws Exception {
        try {
            accessor.close();
        } catch (Exception e) {
            LOG.error("Failed to close bridge resources: {}", e.getMessage());
            throw e;
        }
    }

}
