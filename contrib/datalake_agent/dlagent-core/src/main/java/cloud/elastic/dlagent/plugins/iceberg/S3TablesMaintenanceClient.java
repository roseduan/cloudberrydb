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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Submits table-maintenance jobs to an S3 Tables-shaped maintenance API.
 *
 * <p>This is not the Iceberg REST protocol.  The REST spec defines no
 * maintenance endpoints, so a platform that offers compaction offers it on a
 * surface of its own -- here one addressed by table-bucket ARN, signed for
 * the {@code s3tables} service, and living beside the catalog rather than
 * inside it.  Iceberg's own REST client signs only for the catalog URL, so
 * these calls are built and signed here.
 *
 * <p>Submission only.  The platform queues the job and runs it on its own
 * schedule; the reply says whether the request was accepted, and a 409 means
 * a job for this table already exists -- which is the platform's own throttle
 * of one job per (table, task type), not a failure.
 */
public final class S3TablesMaintenanceClient {

    private static final Logger LOG =
            LoggerFactory.getLogger(S3TablesMaintenanceClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SERVICE = "s3tables";
    private static final int TIMEOUT_MS = 30_000;

    private final String endpoint;
    private final String region;
    private final String accessKey;
    private final String secretKey;

    public S3TablesMaintenanceClient(String endpoint, String region,
                                     String accessKey, String secretKey) {
        this.endpoint = stripTrailingSlash(endpoint);
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    /**
     * Status and body of one call, passed back to SQL untouched.
     *
     * <p>{@code details} carries anything this client worked out about the
     * reply that SQL has to branch on.  It is kept apart from {@code body} so
     * the caller can put it at the top level of its own response: nested
     * inside a JSON string, every quote is escaped and the fields stop being
     * findable by the plain text search the SQL side does.
     */
    public static final class Result {
        public final int status;
        public final String body;
        public final Map<String, String> details;

        Result(int status, String body) {
            this(status, body, null);
        }

        Result(int status, String body, Map<String, String> details) {
            this.status = status;
            this.body = body;
            this.details = details;
        }
    }

    /**
     * Submit a maintenance job for one table.
     *
     * @param bucket    table-bucket name, which is the catalog name on this
     *                  surface
     * @param namespace Iceberg namespace
     * @param table     table name
     * @param taskType  compaction | expire | orphan | manifest-rewrite
     */
    public Result submit(String bucket, String namespace, String table,
                         String taskType) throws IOException {
        String arn = resolveBucketArn(bucket);
        /*
         * Every component is encoded, not just the ARN: encodedPath() takes
         * the path as already encoded and passes it through, so a namespace or
         * table name carrying a path-significant character would otherwise
         * address something else.
         */
        String path = String.format("/s3tables/tables/%s/%s/%s/maintenance-jobs",
                                    encode(arn), encode(namespace), encode(table));
        Result posted = call(SdkHttpMethod.POST, path, "task-type", taskType);
        if (posted.status != HttpURLConnection.HTTP_CONFLICT) {
            return posted;
        }
        return describeConflict(path, taskType, posted);
    }

    /**
     * Say which kind of conflict a 409 is.
     *
     * <p>The platform allows one job per (table, task type) and answers 409
     * for a second one -- but it answers the same 409 whether that job is
     * still running or finished long ago and failed.  Those need opposite
     * handling: a running job means the caller's request is already being
     * served, while a job left behind in a terminal state blocks every future
     * submission for that table until someone deletes it.  The reply says
     * only "Task already exists", so the state is read back here and the
     * answer normalised into a field the caller can branch on.
     *
     * <p>A lookup that itself fails leaves the original 409 untouched: not
     * knowing which case this is must not turn into a claim that it is the
     * bad one.
     */
    private Result describeConflict(String path, String taskType, Result posted) {
        JsonNode job;
        try {
            Result listed = call(SdkHttpMethod.GET, path, null, null);
            if (listed.status != HttpURLConnection.HTTP_OK) {
                return posted;
            }
            job = null;
            for (JsonNode entry : MAPPER.readTree(listed.body).path("jobs")) {
                if (taskType.equals(entry.path("jobType").asText())) {
                    job = entry;
                }
            }
        } catch (IOException | RuntimeException e) {
            LOG.warn("could not read back the conflicting maintenance job for {}: {}",
                     path, e.toString());
            return posted;
        }

        if (job == null) {
            return posted;
        }

        Map<String, String> answer = new LinkedHashMap<>();
        answer.put("jobType", taskType);
        answer.put("jobStatus", job.path("status").asText());
        if (job.path("terminal").asBoolean(false)) {
            answer.put("maintenanceConflict", "terminal");
            answer.put("failureMessage", job.path("failureMessage").asText(""));
            /* Task type goes in the path here, not the query string. */
            answer.put("deletePath", path + "/" + encode(taskType));
        } else {
            answer.put("maintenanceConflict", "running");
        }
        return new Result(posted.status, posted.body, answer);
    }

    /**
     * The ARN carries an account id this database is never told, so it is
     * asked for rather than assembled.  One extra round trip per submission,
     * against a request that queues a job measured in minutes.
     */
    private String resolveBucketArn(String bucket) throws IOException {
        Result listed = call(SdkHttpMethod.GET, "/s3tables/buckets", null, null);
        if (listed.status != HttpURLConnection.HTTP_OK) {
            throw new IOException("cannot list table buckets to resolve the ARN for \""
                    + bucket + "\": HTTP " + listed.status + " " + listed.body);
        }
        JsonNode buckets = MAPPER.readTree(listed.body).path("tableBuckets");
        for (JsonNode entry : buckets) {
            if (bucket.equals(entry.path("name").asText())) {
                return entry.path("arn").asText();
            }
        }
        throw new IOException("no table bucket named \"" + bucket
                + "\" on this endpoint; maintenance cannot be addressed without its ARN");
    }

    private Result call(SdkHttpMethod method, String path,
                        String queryKey, String queryValue) throws IOException {
        SdkHttpFullRequest.Builder builder = SdkHttpFullRequest.builder()
                .method(method)
                .uri(URI.create(endpoint))
                .encodedPath(path);
        if (queryKey != null) {
            builder.appendRawQueryParameter(queryKey, queryValue);
        }

        Aws4SignerParams params = Aws4SignerParams.builder()
                .awsCredentials(AwsBasicCredentials.create(accessKey, secretKey))
                .signingName(SERVICE)
                .signingRegion(Region.of(region))
                .build();
        SdkHttpFullRequest signed = Aws4Signer.create().sign(builder.build(), params);

        HttpURLConnection conn =
                (HttpURLConnection) URI.create(signed.getUri().toString()).toURL().openConnection();
        conn.setRequestMethod(method.name());
        conn.setConnectTimeout(TIMEOUT_MS);
        conn.setReadTimeout(TIMEOUT_MS);
        for (Map.Entry<String, List<String>> header : signed.headers().entrySet()) {
            for (String value : header.getValue()) {
                conn.addRequestProperty(header.getKey(), value);
            }
        }
        if (method == SdkHttpMethod.POST) {
            /*
             * The API takes every parameter in the query string and ignores a
             * body, but a POST with no body still has to say so or the
             * connection stalls waiting for one.
             */
            conn.setDoOutput(true);
            conn.setFixedLengthStreamingMode(0);
            conn.getOutputStream().close();
        }

        int status = conn.getResponseCode();
        String body = readAll(status < HttpURLConnection.HTTP_BAD_REQUEST
                              ? conn.getInputStream() : conn.getErrorStream());
        LOG.info("s3tables maintenance {} {} -> {} {}", method, path, status,
                 body.length() > 200 ? body.substring(0, 200) : body);
        return new Result(status, body);
    }

    private static String readAll(InputStream in) throws IOException {
        if (in == null) {
            return "";
        }
        try (InputStream stream = in) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            int n;
            while ((n = stream.read(chunk)) != -1) {
                out.write(chunk, 0, n);
            }
            return new String(out.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    /**
     * The whole ARN is one path segment, so every character that structures a
     * path or a URI has to be escaped -- including the colons and slashes the
     * ARN is made of.  URLEncoder is form encoding, which differs in exactly
     * two places, so both are corrected.
     */
    private static String encode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name())
                    .replace("+", "%20")
                    .replace("*", "%2A");
        } catch (java.io.UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 is always available", e);
        }
    }

    private static String stripTrailingSlash(String value) {
        return (value != null && value.endsWith("/"))
               ? value.substring(0, value.length() - 1) : value;
    }
}
