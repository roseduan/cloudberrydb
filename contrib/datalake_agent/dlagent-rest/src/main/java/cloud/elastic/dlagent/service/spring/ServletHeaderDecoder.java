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

package cloud.elastic.dlagent.service.spring;

import cloud.elastic.dlagent.service.HttpHeaderDecoder;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.util.Collections;

/**
 * The two {@code HttpServletRequest} overloads that used to sit on
 * {@link HttpHeaderDecoder}. They moved here because {@code javax.servlet} is
 * a transport type that dlagent-core may not reference, and because both
 * callers -- {@code DlWebMvcTagsContributor} and
 * {@code DlContextMdcLogEnhancerFilter} -- are servlet-side classes.
 * <p>
 * Composition rather than inheritance on purpose: a subclass would make two
 * beans assignable to {@code HttpHeaderDecoder} and turn every by-type
 * injection of the core decoder into an ambiguity error.
 * <p>
 * Both methods delegate with the exact same arguments the removed overloads
 * built internally, so the decoded values are unchanged. Note the
 * {@code false} in {@link #areHeadersEncoded}: passing "not encoded" makes
 * {@code getHeaderValue} skip URL-decoding and return the flattened value,
 * which is what the old code did by calling the private {@code flatten}
 * directly.
 */
@Component
public class ServletHeaderDecoder {

    private final HttpHeaderDecoder delegate;

    /**
     * Creates the servlet-side decoder.
     *
     * @param delegate the transport-neutral decoder in dlagent-core
     */
    public ServletHeaderDecoder(HttpHeaderDecoder delegate) {
        this.delegate = delegate;
    }

    /**
     * Determines whether the http headers are expected to be encoded based on presence of the special dlagent http header.
     *
     * @param request http servlet request
     * @return true if the header values are expected to be encoded, false otherwise
     */
    public boolean areHeadersEncoded(HttpServletRequest request) {
        String value = delegate.getHeaderValue(HttpHeaderDecoder.ENCODED_HEADER_VALUES_NAME,
                Collections.list(request.getHeaders(HttpHeaderDecoder.ENCODED_HEADER_VALUES_NAME)),
                false);
        return StringUtils.equalsIgnoreCase(value, "true");
    }

    /**
     * Flattens the list of header values, and decodes if needed and the header name is produced by dlagent client APIs.
     *
     * @param name           header name
     * @param request        http servlet request
     * @param headersEncoded whether the header values are expected to be encoded
     * @return single value of the header
     */
    public String getHeaderValue(String name, HttpServletRequest request, boolean headersEncoded) {
        return delegate.getHeaderValue(name, Collections.list(request.getHeaders(name)), headersEncoded);
    }
}
