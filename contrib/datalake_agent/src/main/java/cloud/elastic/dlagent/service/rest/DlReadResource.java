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

import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.service.RequestParser;
import cloud.elastic.dlagent.service.controller.ReadService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import javax.servlet.http.HttpServletRequest;

/**
 * dlagent REST endpoint for read data requests.
 */
@RestController
@RequestMapping("/dlproxy")
public class DlReadResource extends DlBaseResource<StreamingResponseBody> {

    private final ReadService readService;

    /**
     * Creates a new instance of the resource with Request parser and read service implementation.
     *
     * @param parser      http request parser
     * @param readService read service implementation
     */
    public DlReadResource(RequestParser<MultiValueMap<String, String>> parser,
                          ReadService readService) {
        super(parser);
        this.readService = readService;
    }

    /**
     * REST endpoint for read data requests.
     *
     * @param headers http headers from request that carry all parameters
     * @return response object containing stream that will output records
     */
    @GetMapping(value = "/read", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> read(@RequestHeader MultiValueMap<String, String> headers,
                                                      HttpServletRequest request) {
        return processRequest(headers, request);
    }

    /**
     * REST endpoint for read-class requests (e.g. getFragments) that carry
     * the legacy dlproxy iceberg config JSON in the request body.  A GET
     * cannot carry a body, so the C side POSTs whenever it has a config
     * payload (dlproxy/protocol.c, dlproxy/iceberg.c); without this mapping
     * those requests bounced off Spring with a 404 and the agent never saw
     * the gopher.* connection keys (issue #844).
     *
     * @param requestBody iceberg config JSON, may be absent
     * @param headers     http headers from request that carry all parameters
     * @param request     HTTP servlet request
     * @return response object containing stream that will output records
     */
    @PostMapping(value = "/read", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE,
                 consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<StreamingResponseBody> readWithConfig(@RequestBody(required = false) String requestBody,
                                                                @RequestHeader MultiValueMap<String, String> headers,
                                                                HttpServletRequest request) {
        return processRequest(headers, request, requestBody);
    }

    /**
     * REST endpoint for write data requests.
     *
     * @param fileListRequest JSON request body containing file list
     * @param headers http headers from request that carry other parameters
     * @param request HTTP servlet request
     * @return response object containing stream that will output records
     */
    @PostMapping(value = "/write", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<StreamingResponseBody> write(@RequestBody(required = false) FileListRequest fileListRequest,
                                                        @RequestHeader MultiValueMap<String, String> headers,
                                                        HttpServletRequest request) {
        return processRequestWithFileList(fileListRequest, headers, request);
    }

    /**
     * Process write request with file list from JSON body.
     * This method extends the base processRequest to handle file lists directly from POST body.
     *
     * @param fileListRequest JSON request body containing file list
     * @param headers http headers
     * @param httpServletRequest HTTP servlet request
     * @return response entity
     */
    private ResponseEntity<StreamingResponseBody> processRequestWithFileList(
            final FileListRequest fileListRequest,
            final MultiValueMap<String, String> headers,
            final HttpServletRequest httpServletRequest) {

        // use the request processing algorithm as a lambda for the invoking and error handling logic
        StreamingResponseBody response = this.invokeWithErrorHandling(
                () -> {
                    RequestContext context = getParser().parseRequest(headers);

                    // Add file list from request body to context
                    if (fileListRequest != null && fileListRequest.getFiles() != null) {
                        context.setFileList(fileListRequest.getFiles());
                    }

                    return produceResponse(context, httpServletRequest);
                }
        );

        // return the response entity
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @Override
    protected StreamingResponseBody produceResponse(RequestContext context, HttpServletRequest request) {
        // return a lambda that will be executed asynchronously
        return os -> readService.readData(context, os);
    }
}
