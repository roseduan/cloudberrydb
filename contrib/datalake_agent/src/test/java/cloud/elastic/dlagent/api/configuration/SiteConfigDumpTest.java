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

package cloud.elastic.dlagent.api.configuration;

import cloud.elastic.dlagent.api.model.iceberg.VolumeInfo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Produces the oracle half of the differential test that holds the C parser
 * in {@code contrib/datalake_fdw/src/common/iceberg_site_config.c} to this
 * loader's behaviour. This class asserts nothing on its own: it dumps what
 * the loader makes of each fixture, and
 * {@code contrib/datalake_fdw/test/sitediff/run.sh} compares those dumps
 * against the ones the database produces for the same fixtures.
 *
 * <p>It lives in test scope on purpose. A hidden CLI subcommand would have
 * done the same job while adding a permanently supported entry point that
 * reads config files and prints credentials; this reaches the same code with
 * no production surface at all.
 *
 * <p>Skipped unless {@code -Dsitediff.fixtures=...} names a fixture root, so
 * an ordinary {@code mvn test} is unaffected. Driven by run.sh; see that
 * script for the invocation.
 */
public class SiteConfigDumpTest {

    @Test
    public void dumpFixtures() throws IOException {
        String fixtures = System.getProperty("sitediff.fixtures");
        String out = System.getProperty("sitediff.out");
        assumeTrue(fixtures != null && out != null,
                "set -Dsitediff.fixtures and -Dsitediff.out to run the dumper");

        Path fixtureRoot = Paths.get(fixtures);
        Path outRoot = Paths.get(out);
        Files.createDirectories(outRoot);

        for (String line : Files.readAllLines(fixtureRoot.resolve("cases.tsv"),
                StandardCharsets.UTF_8)) {
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            /* case name, fixture directory, server name, volume type, expectation */
            String[] parts = line.split("\t", -1);
            String caseName = parts[0].trim();
            String dir = parts[1].trim();
            /* "-" stands for the empty string; see cases.tsv */
            String serverName = unplaceholder(parts[2].trim());
            String volumeType = unplaceholder(parts[3].trim());

            Files.write(outRoot.resolve(caseName + ".json"),
                    dumpOne(fixtureRoot.resolve(dir), serverName, volumeType)
                            .getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String unplaceholder(String field) {
        return "-".equals(field) ? "" : field;
    }

    /**
     * Reproduce IcebergRequestConfigParser.resolveVolume()'s dispatch: "hdfs"
     * case-insensitively picks gphdfs.conf, anything else -- an unknown type
     * or none at all -- picks s3.conf. The location argument is empty because
     * the iceberg request never carries one.
     */
    private String dumpOne(Path baseDir, String serverName, String volumeType) {
        SiteConfigLoader loader = new SiteConfigLoader();
        loader.setBaseDir(baseDir.toString());

        try {
            VolumeInfo info = "hdfs".equalsIgnoreCase(volumeType)
                    ? loader.loadHdfsSite(serverName)
                    : loader.loadS3Site(serverName, "");
            return render(info);
        } catch (RuntimeException e) {
            /*
             * The message is the comparable part for the errors this loader
             * raises itself; the wrapped IO and cast failures carry JVM text
             * that run.sh only compares by category.
             */
            return "ERROR\t" + e.getMessage() + "\n";
        }
    }

    /**
     * Field order is VolumeInfo's declaration order so the two dumpers can be
     * read side by side; extras are sorted because the loader keeps them in a
     * HashMap and their order means nothing. An unset Boolean prints as null
     * -- distinct from an explicit false, which is the whole reason the SQL
     * option layer can still override it.
     */
    private String render(VolumeInfo info) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        appendString(sb, "volumeServerType", info.getVolumeServerType());
        appendString(sb, "serverName", info.getServerName());
        appendString(sb, "volumeEndpoint", info.getVolumeEndpoint());
        appendString(sb, "volumeRegion", info.getVolumeRegion());
        appendString(sb, "bucketName", info.getBucketName());
        appendBoolean(sb, "pathStyleAccess", info.getPathStyleAccess());
        appendString(sb, "accessKeyId", info.getAccessKeyId());
        appendString(sb, "secretAccessKey", info.getSecretAccessKey());
        appendString(sb, "username", info.getUsername());
        appendString(sb, "hdfsNamenodeHost", info.getHdfsNamenodeHost());
        appendString(sb, "hdfsNamenodePort", info.getHdfsNamenodePort());
        appendBoolean(sb, "isHaSupported", info.getIsHaSupported());
        appendString(sb, "dfsNameservices", info.getDfsNameservices());
        appendString(sb, "dfsHaNamenodes", info.getDfsHaNamenodes());
        appendString(sb, "dfsNamenodeRpcAddress", info.getDfsNamenodeRpcAddress());
        appendString(sb, "dfsClientFailoverProxyProvider",
                info.getDfsClientFailoverProxyProvider());
        appendBoolean(sb, "dfsClientUseDatanodeHostname",
                info.getDfsClientUseDatanodeHostname());
        appendString(sb, "hdfsAuthMethod", info.getHdfsAuthMethod());
        appendString(sb, "krbPrincipal", info.getKrbPrincipal());
        appendString(sb, "krbPrincipalKeytab", info.getKrbPrincipalKeytab());
        appendString(sb, "hadoopRpcProtection", info.getHadoopRpcProtection());
        appendString(sb, "dataTransferProtocol", info.getDataTransferProtocol());
        appendString(sb, "dataTransferProtection", info.getDataTransferProtection());

        sb.append("\"extraProperties\":{");
        Map<String, String> extras = new TreeMap<>(info.getExtraProperties());
        List<String> rendered = new ArrayList<>();
        for (Map.Entry<String, String> entry : extras.entrySet()) {
            rendered.add(escape(entry.getKey()) + ":" + escape(entry.getValue()));
        }
        sb.append(String.join(",", rendered));
        sb.append("}}");
        sb.append('\n');
        return sb.toString();
    }

    private void appendString(StringBuilder sb, String name, String value) {
        sb.append('"').append(name).append("\":");
        sb.append(value == null ? "null" : escape(value));
        sb.append(',');
    }

    private void appendBoolean(StringBuilder sb, String name, Boolean value) {
        sb.append('"').append(name).append("\":");
        sb.append(value == null ? "null" : value.toString());
        sb.append(',');
    }

    /** Matches PostgreSQL's escape_json() so the two dumps compare as bytes. */
    private String escape(String value) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                default:
                    if (c < ' ') {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.append('"').toString();
    }
}
