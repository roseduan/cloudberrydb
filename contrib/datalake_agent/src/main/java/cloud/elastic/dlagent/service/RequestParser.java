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

package cloud.elastic.dlagent.service;

import cloud.elastic.dlagent.api.model.RequestContext;

/**
 * Parser for incoming requests responsible for extracting request parameters.
 *
 * @param <T> type of request
 */
public interface RequestParser<T> {

    /**
     * Parses the request and constructs RequestContext instance
     * @param request request data
     * @return parsed information as an instance of RequestContext
     */
    RequestContext parseRequest(T request);

    /**
     * Parses a given request into request context, additionally taking the
     * request body (the legacy dlproxy protocol carries the iceberg config
     * JSON there).
     *
     * @param request     the request
     * @param requestBody the request body, may be null
     * @return parsed request context
     */
    default RequestContext parseRequest(T request, String requestBody) {
        return parseRequest(request);
    }
}
