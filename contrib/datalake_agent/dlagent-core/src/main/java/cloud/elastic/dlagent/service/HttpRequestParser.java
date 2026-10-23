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

import cloud.elastic.dlagent.api.utilities.ColumnDescriptor;
import cloud.elastic.dlagent.api.utilities.GpdbFragmentMetadata;
import jodd.util.URLDecoder;

import org.apache.commons.lang.StringUtils;
import cloud.elastic.dlagent.api.error.DlRuntimeException;
import cloud.elastic.dlagent.api.model.Fragment;
import cloud.elastic.dlagent.api.model.PluginConf;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.api.model.ServiceMethodConstants;
import cloud.elastic.dlagent.api.utilities.CharsetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Parser for HTTP requests that contain data in HTTP headers.
 * <p>
 * The request type is {@code Map<String, List<String>>} rather than Spring's
 * {@code MultiValueMap}: the latter is a subtype of the former, so the REST
 * path keeps handing over the exact same {@code HttpHeaders} instance, while
 * gRPC can feed its own metadata map (the
 * {@code transport/RequestContextFactory} shape).
 */
@Named
@Singleton
public class HttpRequestParser implements RequestParser<Map<String, List<String>>> {
    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestParser.class);

    private static final String TRUE_LCASE = "true";

    private final CharsetUtils charsetUtils;
    private final PluginConf pluginConf;
    private final HttpHeaderDecoder headerDecoder;

    /**
     * Create a new instance of the HttpRequestParser with the given PluginConf
     * <p>
     * The former fourth parameter, spring-boot's {@code BuildProperties}, was
     * dropped: it was stored in a field that nothing ever read, and keeping it
     * would have tied dlagent-core to spring-boot for a value nobody uses.
     * {@code /actuator/info} still reports the build info - it reads the
     * {@code BuildProperties} bean directly, which the {@code build-info}
     * goal keeps generating for dlagent-rest.
     *
     * @param pluginConf    the plugin conf
     * @param charsetUtils  utilities for Charset
     * @param headerDecoder decoder of http headers
     */
    @Inject
    public HttpRequestParser(PluginConf pluginConf, CharsetUtils charsetUtils, HttpHeaderDecoder headerDecoder) {
        this.pluginConf = pluginConf;
        this.charsetUtils = charsetUtils;
        this.headerDecoder = headerDecoder;
    }

    /**
     * Throws an exception when the given property value is missing in request.
     *
     * @param property missing property name
     * @throws IllegalArgumentException throws an exception with the property
     *                                  name in the error message
     */
    private static void protocolViolation(String property) {
        String error = String.format("Property %s has no value in the current request", property);
        throw new IllegalArgumentException(error);
    }

    @Override
    public RequestContext parseRequest(Map<String, List<String>> requestHeaders) {
        return parseRequest(requestHeaders, null);
    }

    public RequestContext parseRequest(Map<String, List<String>> requestHeaders, String requestBody) {
        RequestMap params = new RequestMap(requestHeaders, headerDecoder);
        if (LOG.isDebugEnabled()) {
            // Logging only keys to prevent sensitive data to be logged
            LOG.debug("Parsing request parameters: " + params.keySet());
        }

        RequestContext context = new RequestContext();

        // first of all, set profile and enrich parameters with information from specified profile
        String profile = params.removeUserProperty("PROFILE").toLowerCase();
        context.setProfile(profile);
        addProfilePlugins(profile, params);

        context.setMethod(params.removeUserProperty("METHOD"));
        context.setConfig(params.removeUserProperty("CONFIG"));
        context.setServerName(params.removeUserProperty("SERVER"));
        context.setPath(URLDecoder.decode(params.removeProperty("DATA-DIR"), "utf8"));
        context.setDataSource(params.removeUserProperty("DATA-SOURCE"));

        context.setCatalogType(params.removeUserProperty("CATALOG-TYPE"));
        context.setSplitSize(params.removeUserProperty("SPLIT-SIZE"));
        context.setQueryType(params.removeUserProperty("QUERY-TYPE"));
        context.setMetadataTableEnabled(params.removeUserProperty("METADATA-TABLE-ENABLE"));

        String filterString = params.removeOptionalProperty("FILTER");
        String hasFilter = params.removeProperty("HAS-FILTER");
        if (filterString != null) {
            context.setFilterString(filterString);
        } else if ("1".equals(hasFilter)) {
            LOG.info("Original query has filter, but it was not propagated to datalake-proxy");
        }

        context.setMetadataFetcher(params.removeUserProperty("METADATA"));

        context.setSchemaName(params.removeProperty("SCHEMA-NAME"));
        context.setTableName(params.removeProperty("TABLE-NAME"));
        context.setTotalSegments(params.removeIntProperty("SEGMENT-COUNT"));
        context.setTransactionId(params.removeProperty("XID"));

        // parse tuple description
        parseTupleDescription(params, context);
        if (context.getMethod().equals("batchAppend") || context.getMethod().equals("rowUpdate")) {
            parseFragmentList(params, context);
        }
        context.setGpSessionId(params.removeIntProperty("SESSION-ID"));
        context.setGpCommandCount(params.removeIntProperty("COMMAND-COUNT"));
        context.setUser(params.removeProperty("USER"));
        context.setPluginConf(pluginConf);

        // validate that the result has all required fields, and values are in valid ranges
        // Check if we have a JSON configuration in the request body
        if (requestBody != null && !requestBody.isEmpty()) {
            context.setIcebergConfigJsonString(requestBody);
            LOG.debug("Set Iceberg config JSON string from request body");
        }

        context.validate();

        // Iterate over the remaining properties
        // we clone the keyset to prevent concurrent modification exceptions
        List<String> paramNames = new ArrayList<>(params.keySet());
        for (String param : paramNames) {
            if (StringUtils.startsWithIgnoreCase(param, RequestMap.USER_PROP_PREFIX)) {
                // Add all left-over user properties as options
                String optionName = param.toLowerCase().replace(RequestMap.USER_PROP_PREFIX_LOWERCASE, "");
                String optionValue = params.removeUserProperty(optionName);
                context.addOption(optionName, optionValue);
                LOG.debug("Added option {} to request context", optionName);
            }
        }

        return context;
    }

    /*
     * Sets the tuple description for the record
     * Attribute Projection information is optional
     */
    private void parseTupleDescription(RequestMap params, RequestContext context) {
        int columns = params.removeOptionalIntProperty("ATTRS");
        BitSet attrsProjected = new BitSet(columns + 1);

        /* Process column projection info */
        String columnProjStr = params.removeOptionalProperty("ATTRS-PROJ");
        if (columnProjStr != null) {
            int numberOfProjectedColumns = Integer.parseInt(columnProjStr);
            context.setNumAttrsProjected(numberOfProjectedColumns);
            if (numberOfProjectedColumns > 0) {
                String[] projectionIndices = params.removeProperty("ATTRS-PROJ-IDX").split(",");
                for (String s : projectionIndices) {
                    attrsProjected.set(Integer.parseInt(s));
                }
            } else {
                /* This is a special case to handle aggregate queries not related to any specific column
                 * eg: count(*) queries. */
                attrsProjected.set(0);
            }
        }

        for (int attrNumber = 0; attrNumber < columns; attrNumber++) {
            String columnName = params.removeProperty("ATTR-NAME" + attrNumber);
            int columnOID = params.removeIntProperty("ATTR-TYPECODE" + attrNumber);
            String columnTypeName = params.removeProperty("ATTR-TYPENAME" + attrNumber);
            Integer[] columnTypeMods = parseTypeMods(params, attrNumber);
            // Project the column if columnProjStr is null
            boolean isProjected = columnProjStr == null || attrsProjected.get(attrNumber);
            ColumnDescriptor column = new ColumnDescriptor(
                    columnName,
                    columnOID,
                    attrNumber,
                    columnTypeName,
                    columnTypeMods,
                    isProjected);
            context.getTupleDescription().add(column);
        }
    }

    private Integer[] parseTypeMods(RequestMap params, int columnIndex) {
        String typeModCountPropName = "ATTR-TYPEMOD" + columnIndex + "-COUNT";
        String typeModeCountStr = params.removeOptionalProperty(typeModCountPropName);
        if (typeModeCountStr == null)
            return null;

        int typeModeCount = parsePositiveIntOrError(typeModeCountStr, typeModCountPropName);

        Integer[] result = new Integer[typeModeCount];
        for (int i = 0; i < typeModeCount; i++) {
            String typeModItemPropName = "ATTR-TYPEMOD" + columnIndex + "-" + i;
            result[i] = parsePositiveIntOrError(params.removeProperty(typeModItemPropName), typeModItemPropName);
        }
        return result;
    }

    private int parsePositiveIntOrError(String s, String propName) {
        int n;
        try {
            n = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("%s must be an integer", propName), e);
        }
        if (n < 0) {
            throw new IllegalArgumentException(String.format("%s must be a positive integer", propName));
        }
        return n;
    }

    private void parseFragmentList(RequestMap params, RequestContext context) {
        int fileCount = params.removeIntProperty("FILE-COUNT");
        for (int i = 0; i < fileCount; i++) {
            String filePath = URLDecoder.decode(params.removeProperty("FILE-PATH" + i), "utf8");
            Long fileSize = Long.parseLong(params.removeProperty("FILE-SIZE" + i)); 
            Long rowCount = Long.parseLong(params.removeProperty("ROW-COUNT" + i));
            String format = params.removeProperty("FILE-FORMAT" + i);
            String content = params.removeProperty("FILE-CONTENT"+i);
            LOG.info("Get file {}, {}, {}, {}, {}", filePath, fileSize, rowCount, format, content);
            context.getFragments().add(new Fragment(filePath, new GpdbFragmentMetadata(fileSize, format, rowCount, content)));
        }
    }

    /**
     * Sets the requested profile plugins from the profile file into the request map.
     *
     * @param profile profile specified in the user request
     * @param params  parameters provided in the user request
     */
    private void addProfilePlugins(String profile, RequestMap params) {
        // if profile was not specified in the request, do nothing
        if (StringUtils.isBlank(profile)) {
            LOG.debug("Profile was not specified in the request");
            return;
        }

        LOG.debug("Adding plugins for profile {}", profile);

        // get Profile's plugins from the configuration file
        Map<String, String> pluginsMap = pluginConf.getPlugins(profile);

        // create sets of keys to find out duplicates between what user has specified in the request
        // and what is configured in the configuration file -- DO NOT ALLOW DUPLICATES
        List<String> duplicates = pluginsMap.keySet().stream()
                .filter(n -> params.containsKey(RequestMap.USER_PROP_PREFIX + n))
                .collect(Collectors.toList());

        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException(String.format("Profile '%s' already defines: %s",
                    profile, duplicates));
        }

        // since there are guaranteed to be no duplications at this point,
        // add properties defined by profiles to the request map as if they were specified by the user
        pluginsMap.forEach((k, v) -> params.put(RequestMap.USER_PROP_PREFIX + k, v));
    }

    /**
     * Converts the request headers multivalued map to a case-insensitive
     * regular map by taking only first values and storing them in a
     * CASE_INSENSITIVE_ORDER TreeMap. All values are converted from ISO_8859_1
     * (ISO-LATIN-1) to UTF_8.
     */
    static class RequestMap extends TreeMap<String, String> {
        private static final long serialVersionUID = 4745394510220213936L;
        private static final String PROP_PREFIX = "X-GP-";
        private static final String USER_PROP_PREFIX = "X-GP-OPTIONS-";
        private static final String USER_PROP_PREFIX_LOWERCASE = "x-gp-options-";


        RequestMap(Map<String, List<String>> requestHeaders, HttpHeaderDecoder headerDecoder) {
            super(String.CASE_INSENSITIVE_ORDER);

            boolean headersEncoded = headerDecoder.areHeadersEncoded(requestHeaders);
            for (Map.Entry<String, List<String>> entry : requestHeaders.entrySet()) {
                String key = entry.getKey();
                String value = headerDecoder.getHeaderValue(key, entry.getValue(), headersEncoded);
                if (value != null) {
                    LOG.trace("Key: {} Value: {}", key, value);
                    put(key, value);
                }
            }
        }

        /**
         * Returns a user defined property.
         *
         * @param userProp the lookup user property
         * @return property value as a String
         */
        private String removeUserProperty(String userProp) {
            return remove(USER_PROP_PREFIX + userProp);
        }

        /**
         * Returns the optional property value. Unlike {@link #removeProperty}, it will
         * not fail if the property is not found. It will just return null instead.
         *
         * @param property the lookup optional property
         * @return property value as a String
         */
        private String removeOptionalProperty(String property) {
            return remove(PROP_PREFIX + property);
        }

        /**
         * Returns a property value as an int type and removes it from the map
         *
         * @param property the lookup property
         * @return property value as an int type
         * @throws NumberFormatException if the value is missing or can't be
         *                               represented by an Integer
         */
        private int removeIntProperty(String property) {
            return Integer.parseInt(removeProperty(property));
        }

        private int removeOptionalIntProperty(String property) {
            String result = removeOptionalProperty(property);
            if (result != null) {
                return Integer.parseInt(result);
            }

            return 0;
        }

        /**
         * Returns the value to which the specified property is mapped and
         * removes it from the map
         *
         * @param property the lookup property key
         * @throws IllegalArgumentException if property key is missing
         */
        private String removeProperty(String property) {
            String result = remove(PROP_PREFIX + property);

            if (result == null) {
                protocolViolation(property);
            }

            return result;
        }
    }
}
