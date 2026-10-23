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

import cloud.elastic.dlagent.api.io.BufferWritable;
import cloud.elastic.dlagent.api.io.Writable;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.service.utilities.BasePluginFactory;
import cloud.elastic.dlagent.service.utilities.GSSFailureHandler;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Abstract class representing the bridge that provides to subclasses logger and accessor and
 * resolver instances obtained from the factories.
 */
public abstract class BaseBridge implements Bridge {
    private static final String DEFAULT_RESPONSE = "{}";
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected BasePluginFactory pluginFactory;
    protected RequestContext context;
    protected GSSFailureHandler failureHandler;

    /**
     * Creates a new instance of the bridge.
     *
     * @param pluginFactory plugin factory
     * @param context request context
     * @param failureHandler failure handler
     */
    public BaseBridge(BasePluginFactory pluginFactory, RequestContext context, GSSFailureHandler failureHandler) {
        this.pluginFactory = pluginFactory;
        this.context = context;
        this.failureHandler = failureHandler;
    }

    protected Writable makeOutput(Object value) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.USE_ANNOTATIONS, true); // enable annotations for serialization
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY); // ignore empty fields

        if (value == null) {
            byte[] output = DEFAULT_RESPONSE.getBytes(StandardCharsets.UTF_8);
            return new BufferWritable(output, output.length);
        }

        byte[] output = mapper.writeValueAsBytes(value);
        return new BufferWritable(output, output.length);
    }
}
