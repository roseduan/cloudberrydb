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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Service that can decode the values of http headers for dlagent client requests.
 * <p>
 * Header maps are taken as {@code Map<String, List<String>>} rather than
 * Spring's {@code MultiValueMap} so that both transports can feed it; a
 * {@code MultiValueMap<String, String>} <em>is</em> a
 * {@code Map<String, List<String>>}, so the REST path passes the very same
 * object it passed before. The two {@code HttpServletRequest} overloads that
 * used to live here moved to {@code service.spring.ServletHeaderDecoder} --
 * both of their callers are servlet filters in dlagent-rest.
 */
@Named
@Singleton
public class HttpHeaderDecoder {

    private static final String PROP_PREFIX = "X-GP-";

    /**
     * Header the dlagent client sets when it URL-encodes header values.
     * Public because the servlet-side decoder in dlagent-rest reads the same
     * header off the {@code HttpServletRequest}.
     */
    public static final String ENCODED_HEADER_VALUES_NAME = PROP_PREFIX + "ENCODED-HEADER-VALUES";
    private static final String UTF8_NAME = StandardCharsets.UTF_8.name();

    /**
     * Determines whether the http headers are expected to be encoded based on presence of the special dlagent http header.
     *
     * @param requestHeaders request headers
     * @return true if the header values are expected to be encoded, false otherwise
     */
    public boolean areHeadersEncoded(Map<String, List<String>> requestHeaders) {
        boolean headersEncoded = false;
        for (Map.Entry<String, List<String>> entry : requestHeaders.entrySet()) {
            if (StringUtils.equalsIgnoreCase(ENCODED_HEADER_VALUES_NAME, entry.getKey())) {
                headersEncoded = StringUtils.equalsIgnoreCase(flatten(entry.getValue()), "true");
                break;
            }
        }
        return headersEncoded;
    }

    /**
     * Flattens the list of header values, and decodes if needed and the header name is produced by dlagent client APIs.
     *
     * @param name           header name
     * @param values         header values
     * @param headersEncoded whether the header values are expected to be encoded
     * @return single value of the header
     */
    public String getHeaderValue(String name, List<String> values, boolean headersEncoded) {
        String value = flatten(values);
        if (value != null && headersEncoded && StringUtils.startsWithIgnoreCase(name, PROP_PREFIX)) {
            try {
                return URLDecoder.decode(value, UTF8_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(String.format("Error while URL decoding value '%s'", value), e);
            }
        }
        return value;
    }

    /**
     * Returns the value from the list of values. If the list has 1 element, it returns the element.
     * If the list has more than one element, it returns a flattened string joined with a comma.
     *
     * @param values the list of values
     * @return the value
     */
    private String flatten(List<String> values) {
        if (CollectionUtils.isEmpty(values)) {
            return null;
        }

        String value = values.size() > 1 ? StringUtils.join(values, ",") : values.get(0);
        if (value == null) return null;

        // Converting to value UTF-8 encoding
        return new String(value.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
    }
}
