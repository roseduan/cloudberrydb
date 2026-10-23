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

package cloud.elastic.dlagent.plugins.iceberg;

import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.constants.IcebergConfigConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Named("polarisIcebergCatalogManager")
@Singleton
@Slf4j
public class IcebergPolarisCatalogManager {

    // API endpoints
    private static final String CATALOGS_ENDPOINT = "/api/management/v1/catalogs";
    private static final String OAUTH_TOKEN_ENDPOINT = "/api/catalog/v1/oauth/tokens";

    // HTTP methods
    private static final String HTTP_POST = "POST";
    private static final String HTTP_GET = "GET";

    // HTTP headers
    private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String HEADER_POLARIS_REALM = "Polaris-Realm";
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String CONTENT_TYPE_FORM = "application/x-www-form-urlencoded";
    private static final String BEARER_PREFIX = "Bearer ";

    // JSON field names
    private static final String FIELD_NAME = "name";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_READ_ONLY = "readOnly";
    private static final String FIELD_PROPERTIES = "properties";
    private static final String FIELD_STORAGE_CONFIG_INFO = "storageConfigInfo";
    private static final String FIELD_CATALOG = "catalog";
    private static final String FIELD_CATALOGS = "catalogs";
    private static final String FIELD_ACCESS_TOKEN = "access_token";

    // Catalog type
    private static final String CATALOG_TYPE_INTERNAL = "INTERNAL";

    // OAuth parameters
    private static final String OAUTH_GRANT_TYPE = "grant_type=client_credentials";
    private static final String OAUTH_CLIENT_ID_PARAM = "&client_id=";
    private static final String OAUTH_CLIENT_SECRET_PARAM = "&client_secret=";
    private static final String OAUTH_SCOPE_PARAM = "&scope=";

    // Operation names for exceptions
    private static final String OP_CREATE_CATALOG = "createCatalog";
    private static final String OP_LIST_CATALOGS = "listCatalogs";
    private static final String OP_LIST_NAMESPACES = "listNamespaces";
    private static final String OP_GET_TOKEN = "getToken";

    // HTTP status codes
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_CONFLICT = 409;
    private static final int HTTP_BAD_REQUEST = 400;

    // HTTP timeouts (ms) to avoid blocking indefinitely on a hung Polaris service
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 30_000;

    private static void applyTimeouts(HttpURLConnection conn) {
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
    }

    private final ObjectMapper objectMapper = new ObjectMapper();

    public boolean createCatalog(String catalogName,
            Map<String, String> catalogProperties,
            Map<String, Object> storageConfig,
            Map<String, String> properties) throws Exception {
        try {
            RequestContext context = createRequestContextForCatalogOps(properties);

            String token = getToken(context);
            String polarisBaseUrl = getPolarisBaseUrl(context);
            String realm = getRealm(context);

            Map<String, Object> catalog = new HashMap<>();
            catalog.put(FIELD_NAME, catalogName);
            catalog.put(FIELD_TYPE, CATALOG_TYPE_INTERNAL);
            catalog.put(FIELD_READ_ONLY, false);
            catalog.put(FIELD_PROPERTIES, catalogProperties != null ? catalogProperties : new HashMap<>());
            if (storageConfig != null) {
                catalog.put(FIELD_STORAGE_CONFIG_INFO, storageConfig);
            }

            Map<String, Object> payload = new HashMap<>();
            payload.put(FIELD_CATALOG, catalog);

            URL url = new URL(polarisBaseUrl + CATALOGS_ENDPOINT);

            String requestBody = objectMapper.writeValueAsString(payload);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            applyTimeouts(conn);
            conn.setRequestMethod(HTTP_POST);
            conn.setRequestProperty(HEADER_AUTHORIZATION, BEARER_PREFIX + token);
            conn.setRequestProperty(HEADER_POLARIS_REALM, realm);
            conn.setRequestProperty(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(requestBody.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();

            if (responseCode != HTTP_CREATED && responseCode != HTTP_CONFLICT) {
                String responseBody = "";
                InputStream inputStream = responseCode >= HTTP_BAD_REQUEST ? conn.getErrorStream() : conn.getInputStream();
                if (inputStream != null) {
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
                        StringBuilder response = new StringBuilder();
                        String line;
                        while ((line = br.readLine()) != null) {
                            response.append(line);
                        }
                        responseBody = response.toString();
                    }
                }

                throw new cloud.elastic.dlagent.api.exception.CatalogOperationException(
                    OP_CREATE_CATALOG,
                    responseCode,
                    responseBody,
                    "Failed to create catalog: " + catalogName
                );
            }

            /* Management-path operation; free the socket eagerly to avoid
             * keep-alive reuse stalls from the unconsumed response body. */
            conn.disconnect();
            return responseCode == HTTP_CREATED || responseCode == HTTP_CONFLICT;
        } catch (Exception e) {
            if (e instanceof cloud.elastic.dlagent.api.exception.CatalogOperationException) {
                throw e;
            }
            log.error("Failed to create Polaris catalog: {}", catalogName, e);
            throw e;
        }
    }

    public List<String> listCatalogs(Map<String, String> properties) throws Exception {
        try {
            RequestContext context = createRequestContextForCatalogOps(properties);

            String token = getToken(context);
            String polarisBaseUrl = getPolarisBaseUrl(context);
            String realm = getRealm(context);

            URL url = new URL(polarisBaseUrl + CATALOGS_ENDPOINT);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            applyTimeouts(conn);
            conn.setRequestMethod(HTTP_GET);
            conn.setRequestProperty(HEADER_AUTHORIZATION, BEARER_PREFIX + token);
            conn.setRequestProperty(HEADER_POLARIS_REALM, realm);

            int responseCode = conn.getResponseCode();
            if (responseCode != HTTP_OK) {
                throw new cloud.elastic.dlagent.api.exception.CatalogOperationException(
                    OP_LIST_CATALOGS,
                    responseCode,
                    readErrorBody(conn),
                    "Failed to list Polaris catalogs"
                );
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    response.append(line);
                }
                JsonNode json = objectMapper.readTree(response.toString());

                List<String> catalogs = new ArrayList<>();
                if (json.has(FIELD_CATALOGS)) {
                    for (JsonNode catalog : json.get(FIELD_CATALOGS)) {
                        /* path() returns a MissingNode for absent fields,
                         * avoiding NPE on malformed responses. */
                        catalogs.add(catalog.path(FIELD_NAME).asText());
                    }
                }
                return catalogs;
            }
        } catch (Exception e) {
            if (e instanceof cloud.elastic.dlagent.api.exception.CatalogOperationException) {
                throw e;
            }
            log.error("Failed to list Polaris catalogs", e);
            throw e;
        }
    }

    private static String readErrorBody(HttpURLConnection conn) {
        InputStream err = conn.getErrorStream();
        if (err == null) {
            return "";
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(err, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        } catch (Exception ignored) {
            return "";
        }
    }

    public List<String> listNamespaces(String catalogName, Map<String, String> properties) throws Exception {
        try {
            RequestContext context = createRequestContextForCatalogOps(properties);

            String token = getToken(context);
            String polarisBaseUrl = getPolarisBaseUrl(context);
            String realm = getRealm(context);

            String encodedCatalog = URLEncoder.encode(catalogName, StandardCharsets.UTF_8.name());
            URL url = new URL(polarisBaseUrl + "/api/catalog/v1/" + encodedCatalog + "/namespaces");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            applyTimeouts(conn);
            conn.setRequestMethod(HTTP_GET);
            conn.setRequestProperty(HEADER_AUTHORIZATION, BEARER_PREFIX + token);
            conn.setRequestProperty(HEADER_POLARIS_REALM, realm);

            int responseCode = conn.getResponseCode();
            if (responseCode != HTTP_OK) {
                throw new cloud.elastic.dlagent.api.exception.CatalogOperationException(
                    OP_LIST_NAMESPACES,
                    responseCode,
                    readErrorBody(conn),
                    "Failed to list Polaris namespaces for catalog: " + catalogName
                );
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    response.append(line);
                }
                JsonNode json = objectMapper.readTree(response.toString());

                List<String> namespaces = new ArrayList<>();
                if (json.has("namespaces")) {
                    /* Iceberg REST spec: each namespace is an array of
                     * string levels (e.g. ["a","b"]); join with '.'. */
                    for (JsonNode namespace : json.get("namespaces")) {
                        if (namespace.isArray()) {
                            StringBuilder joined = new StringBuilder();
                            for (JsonNode level : namespace) {
                                if (joined.length() > 0) {
                                    joined.append('.');
                                }
                                joined.append(level.asText());
                            }
                            namespaces.add(joined.toString());
                        } else {
                            namespaces.add(namespace.asText());
                        }
                    }
                }
                return namespaces;
            }
        } catch (Exception e) {
            if (e instanceof cloud.elastic.dlagent.api.exception.CatalogOperationException) {
                throw e;
            }
            log.error("Failed to list Polaris namespaces for catalog: {}", catalogName, e);
            throw e;
        }
    }

    public RequestContext createRequestContextForCatalogOps(Map<String, String> properties) {
        Map<String, String> safeProps = properties != null ? properties : Collections.emptyMap();

        RequestContext context = new RequestContext();
        Configuration configuration = new Configuration(false);

        String catalogType = safeProps.getOrDefault(
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." +
                        IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE,
                IcebergConfigConstants.CATALOG_TYPE_POLARIS);
        context.setCatalogType(catalogType);

        if (!IcebergConfigConstants.CATALOG_TYPE_POLARIS.equalsIgnoreCase(catalogType)) {
            throw new UnsupportedOperationException(
                    "Catalog management operations are only supported for Polaris catalog type, but got: "
                            + catalogType);
        }

        applyPolarisConfig(configuration, safeProps);
        context.setConfiguration(configuration);
        return context;
    }

    private void applyPolarisConfig(Configuration configuration, Map<String, String> properties) {
        setIfPresent(configuration,
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_URL,
                properties,
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." +
                        IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_URL);

        setIfPresent(configuration,
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_ID,
                properties,
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." +
                        IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_ID);

        setIfPresent(configuration,
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_SECRET,
                properties,
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." +
                        IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_SECRET);

        setIfPresent(configuration,
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SCOPE,
                properties,
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." +
                        IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SCOPE);

        setIfPresent(configuration,
                "polaris_server_realm",
                properties,
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + ".polaris_server_realm");
    }

    private void setIfPresent(Configuration configuration, String key, Map<String, String> properties, String propKey) {
        String value = properties.getOrDefault(propKey, "");
        if (!value.isEmpty()) {
            configuration.set(key, value);
        }
    }

    private String getToken(RequestContext context) throws Exception {
        /* Fail fast if credentials are missing rather than silently falling
         * back to the well-known "root:secret" Polaris test account. */
        String clientId = context.getConfiguration().get(
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_ID, "");
        String clientSecret = context.getConfiguration().get(
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CLIENT_SECRET, "");
        if (clientId.isEmpty() || clientSecret.isEmpty()) {
            throw new IllegalStateException(
                    "Polaris OAuth credentials are missing: set client_id and "
                            + "client_secret on the foreign catalog options.");
        }
        String scope = context.getConfiguration().get(
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SCOPE, "PRINCIPAL_ROLE:ALL");

        String charset = StandardCharsets.UTF_8.name();
        String body = OAUTH_GRANT_TYPE +
                OAUTH_CLIENT_ID_PARAM + URLEncoder.encode(clientId, charset) +
                OAUTH_CLIENT_SECRET_PARAM + URLEncoder.encode(clientSecret, charset) +
                OAUTH_SCOPE_PARAM + URLEncoder.encode(scope, charset);

        URL url = new URL(getPolarisBaseUrl(context) + OAUTH_TOKEN_ENDPOINT);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        applyTimeouts(conn);
        conn.setRequestMethod(HTTP_POST);
        conn.setRequestProperty(HEADER_CONTENT_TYPE, CONTENT_TYPE_FORM);
        conn.setRequestProperty(HEADER_POLARIS_REALM, getRealm(context));
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        if (responseCode != HTTP_OK) {
            /* getErrorStream() may be null (e.g. server closed without body);
             * readErrorBody() handles that safely. */
            String errorResponse = readErrorBody(conn);

            throw new cloud.elastic.dlagent.api.exception.CatalogOperationException(
                OP_GET_TOKEN,
                responseCode,
                errorResponse,
                "Failed to get OAuth token from Polaris"
            );
        }

        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            String responseBody = response.toString();
            /* Do not log responseBody: it carries the access_token. */
            log.debug("OAuth token response received successfully");

            try {
                JsonNode json = objectMapper.readTree(responseBody);
                JsonNode tokenNode = json.get(FIELD_ACCESS_TOKEN);
                if (tokenNode == null) {
                    /* Do not pass responseBody — it may contain credential data
                     * that would leak via getMessage() when logged. */
                    throw new cloud.elastic.dlagent.api.exception.CatalogOperationException(
                        OP_GET_TOKEN,
                        HTTP_OK,
                        "",
                        "OAuth response missing access_token field"
                    );
                }
                return tokenNode.asText();
            } catch (Exception e) {
                if (e instanceof cloud.elastic.dlagent.api.exception.CatalogOperationException) {
                    throw e;
                }
                /* Do not pass responseBody — partial/malformed OAuth responses
                 * may still contain credential fragments. */
                throw new cloud.elastic.dlagent.api.exception.CatalogOperationException(
                    OP_GET_TOKEN,
                    HTTP_OK,
                    "",
                    "Failed to parse OAuth token response: " + e.getMessage()
                );
            }
        }
    }

    private String getPolarisBaseUrl(RequestContext context) {
        String serverUrl = context.getConfiguration().get(
                IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.POLARIS_SERVER_URL, "");
        if (serverUrl.endsWith("/api/catalog")) {
            return serverUrl.substring(0, serverUrl.length() - "/api/catalog".length());
        }
        return serverUrl;
    }

    private String getRealm(RequestContext context) {
        return context.getConfiguration().get("polaris_server_realm", "POLARIS");
    }
}
