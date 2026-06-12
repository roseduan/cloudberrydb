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

package cloud.elastic.dlagent.service.rest;

import lombok.extern.slf4j.Slf4j;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.service.RequestParser;
import cloud.elastic.dlagent.service.controller.DlErrorReporter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;

import javax.servlet.http.HttpServletRequest;

/**
 * Base class for dlagent REST resources that provides unified error logging and wrapping.
 * All exceptions will be first logged with the proper MDC context and then wrapped into a DlRuntimeException
 * so that the ErrorHandler can process them and not re-throw to the container
 * where they would've been logged again, but without the MDC context.
 *
 * @param <T> type of ResponseEntity that a resource will produce.
 */
@Slf4j
public abstract class DlBaseResource<T> extends DlErrorReporter<T> {
    private final RequestParser<MultiValueMap<String, String>> parser;

    /**
     * Creates a new instance of the resource.
     *
     * @param parser      request parser
     */
    protected DlBaseResource(RequestParser<MultiValueMap<String, String>> parser) {
        this.parser = parser;
    }

    /**
     * Gets the request parser.
     *
     * @return the request parser
     */
    protected RequestParser<MultiValueMap<String, String>> getParser() {
        return parser;
    }

    /**
     * Parses the incoming httpServletRequest and produces a response, wrapping and logging an error, if any.
     *
     * @param headers            http servlet request headers
     * @param httpServletRequest http servlet request
     * @return response entity to give to container
     */
    protected ResponseEntity<T> processRequest(final MultiValueMap<String, String> headers,
                                               final HttpServletRequest httpServletRequest) {
        return processRequest(headers, httpServletRequest, null);
    }

    /**
     * Variant that also accepts a request body.  The legacy dlproxy protocol
     * carries the iceberg config JSON (gopher.* connection keys emitted by
     * contrib/datalake_fdw/src/dlproxy/icebergConfig.c) in the body; the
     * parser stores it in RequestContext.icebergConfigJsonString.
     *
     * @param headers            http servlet request headers
     * @param httpServletRequest http servlet request
     * @param requestBody        request body, may be null
     * @return response entity to give to container
     */
    protected ResponseEntity<T> processRequest(final MultiValueMap<String, String> headers,
                                               final HttpServletRequest httpServletRequest,
                                               final String requestBody) {
        // use the request processing algorithm as a lambda for the invoking and error handling logic
        T response = this.invokeWithErrorHandling(
                () -> {
                    RequestContext context = parser.parseRequest(headers, requestBody);
                    return produceResponse(context, httpServletRequest);
                }
        );

        // return the response entity, if it is StreamingResponseBody, then the response will be streamed asynchronously
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    /**
     * Produces response of type T by processing a given request.
     *
     * @param context dlagent request context
     * @param request HTTP servlet request
     * @return the response that can be placed in the ResponseEntity and given to the container
     * @throws Exception if operation fails
     */
    protected abstract T produceResponse(RequestContext context, HttpServletRequest request) throws Exception;
}
