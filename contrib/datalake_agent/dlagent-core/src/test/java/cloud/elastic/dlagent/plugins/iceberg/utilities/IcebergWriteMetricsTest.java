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

package cloud.elastic.dlagent.plugins.iceberg.utilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Differential test for the write-time statistics path (issue #442).
 *
 * <p>Every case pits {@link IcebergWriteMetrics} against the thing it replaces:
 * Iceberg's own {@code ParquetUtil.fileMetrics}, reading the same parquet file.
 * The two are given the same metrics config and their output is compared field
 * by field, bound by bound, byte by byte. That is the claim the whole change
 * rests on -- that a manifest built from statistics the segment sent is
 * indistinguishable from one built by reopening the file -- and it is a claim
 * that has to be tested against the real implementation rather than against a
 * restatement of it.
 *
 * <p>The fixtures are real parquet files written by the collector's own test
 * with the schema shapes the datalake writer emits, each paired with the
 * payload the segment would have sent for it:
 *
 * <pre>
 *     make -C contrib/datalake_fdw/test/unit check-parquet
 *     ./iceberg_column_stats_test ../../../datalake_agent/src/test/resources/column_stats
 * </pre>
 */
public class IcebergWriteMetricsTest {

    private static final String FIXTURE_DIR = "column_stats";

    /**
     * The metrics modes a column can be under. Each is exercised against every
     * fixture, because the mode decides whether bounds are written at all and
     * how far they are truncated -- and a mismatch there is a silently wrong
     * manifest, not a crash.
     */
    private static final Map<String, Map<String, String>> MODES = new TreeMap<>();

    static {
        MODES.put("default-truncate(16)", Collections.emptyMap());
        MODES.put("none", Collections.singletonMap(
                "write.metadata.metrics.default", "none"));
        MODES.put("counts", Collections.singletonMap(
                "write.metadata.metrics.default", "counts"));
        MODES.put("full", Collections.singletonMap(
                "write.metadata.metrics.default", "full"));
        MODES.put("truncate(2)", Collections.singletonMap(
                "write.metadata.metrics.default", "truncate(2)"));
    }

    /**
     * The one place the two paths are allowed to disagree, and the only one.
     *
     * <p>A value longer than the collector's per-bound ceiling makes the
     * segment send no bound for that column, where the footer reader would
     * have truncated one. The manifest then says nothing about that column's
     * range instead of saying something narrow, so a scan reads the file
     * rather than skipping it -- pruning lost, never rows. The exception is
     * listed per column so a drop appearing anywhere else still fails.
     */
    private static final Map<String, List<Integer>> ALLOWED_MISSING_BOUNDS =
            Collections.singletonMap("oversized", Collections.singletonList(1));

    @Test
    public void matchesFooterMetricsForEveryFixture() throws Exception {
        List<String> fixtures = Arrays.asList(
                "types", "counts", "rowgroups", "oversized",
                "single_sided_drop", "empty_lower", "posdelete");

        for (String fixture : fixtures) {
            for (Map.Entry<String, Map<String, String>> mode : MODES.entrySet()) {
                assertSameMetrics(fixture, mode.getKey(), mode.getValue());
            }
        }
    }

    /**
     * A file whose statistics never arrived has to leave the caller reading the
     * footer, which is what keeps ORC files and older segments working.
     */
    @Test
    public void absentStatisticsProduceNoMetrics() {
        assertNull(IcebergWriteMetrics.fromColumnStats(
                null, 10L, new Schema(Types.NestedField.optional(
                        1, "c", Types.StringType.get())),
                MetricsConfig.getDefault()));
    }

    /**
     * A payload from a future writer is refused rather than guessed at: the
     * caller falls back, which costs a footer read, where misreading an
     * unfamiliar layout would cost correctness.
     */
    @Test
    public void unknownWireFormatIsRefused() {
        Map<String, Object> future = new HashMap<>();
        future.put("v", 99);
        future.put("cols", Collections.emptyList());

        assertNull(IcebergWriteMetrics.fromColumnStats(
                future, 10L, new Schema(Types.NestedField.optional(
                        1, "c", Types.StringType.get())),
                MetricsConfig.getDefault()));
    }

    /** A position-delete file's columns resolve to the names Iceberg pins. */
    @Test
    public void positionDeleteSchemaNamesTheReservedColumns() {
        Schema schema = IcebergWriteMetrics.positionDeleteSchema();

        assertEquals("file_path", schema.findColumnName(2147483546));
        assertEquals("pos", schema.findColumnName(2147483545));
    }

    private void assertSameMetrics(String fixture, String modeName,
                                   Map<String, String> properties)
            throws IOException {
        JsonNode sidecar = readSidecar(fixture);
        File parquet = fixtureFile(fixture + ".parquet");
        MetricsConfig config = MetricsConfig.fromProperties(properties);
        String where = fixture + " @ " + modeName;

        Metrics expected = ParquetUtil.fileMetrics(
                Files.localInput(parquet), config);
        Metrics actual = IcebergWriteMetrics.fromColumnStats(
                toPlain(sidecar.get("column_stats")),
                sidecar.get("record_count").asLong(),
                nameSchema(sidecar),
                config);

        assertNotNull(actual, where + ": statistics should have been usable");

        assertEquals(expected.recordCount(), actual.recordCount(),
                     where + ": record count");
        assertEquals(expected.columnSizes(), actual.columnSizes(),
                     where + ": column sizes");
        assertEquals(expected.valueCounts(), actual.valueCounts(),
                     where + ": value counts");
        assertEquals(expected.nullValueCounts(), actual.nullValueCounts(),
                     where + ": null value counts");
        assertBoundsEqual(expected.lowerBounds(), actual.lowerBounds(),
                          ALLOWED_MISSING_BOUNDS.getOrDefault(
                                  fixture, Collections.emptyList()),
                          where + ": lower bounds");
        assertBoundsEqual(expected.upperBounds(), actual.upperBounds(),
                          ALLOWED_MISSING_BOUNDS.getOrDefault(
                                  fixture, Collections.emptyList()),
                          where + ": upper bounds");
    }

    /**
     * Bounds are compared as bytes, not as values: the manifest stores bytes,
     * and two encodings of the same number are two different bounds to every
     * reader that will ever look at this table.
     *
     * <p>A bound may be missing only where the ceiling is known to drop it,
     * and a bound that is present must be identical. A bound the footer path
     * did not produce at all is always a failure: that direction would mean
     * claiming a range from statistics Iceberg itself declined to trust.
     */
    private void assertBoundsEqual(Map<Integer, ByteBuffer> expected,
                                   Map<Integer, ByteBuffer> actual,
                                   List<Integer> mayBeMissing,
                                   String where) {
        Map<Integer, ByteBuffer> reference =
                expected == null ? Collections.emptyMap() : expected;
        Map<Integer, ByteBuffer> ours =
                actual == null ? Collections.emptyMap() : actual;

        for (Integer fieldId : ours.keySet()) {
            assertTrue(reference.containsKey(fieldId),
                       where + ": field " + fieldId
                       + " has a bound the footer path did not produce");
        }

        for (Map.Entry<Integer, ByteBuffer> entry : reference.entrySet()) {
            Integer fieldId = entry.getKey();

            if (!ours.containsKey(fieldId)) {
                assertTrue(mayBeMissing.contains(fieldId),
                           where + ": field " + fieldId
                           + " lost its bound and is not a known exception");
                continue;
            }
            assertEquals(hex(entry.getValue()), hex(ours.get(fieldId)),
                         where + ": field " + fieldId);
        }
    }

    private static String hex(ByteBuffer buffer) {
        if (buffer == null) {
            return "<none>";
        }
        ByteBuffer dup = buffer.duplicate();
        StringBuilder out = new StringBuilder();
        while (dup.hasRemaining()) {
            out.append(String.format("%02x", dup.get()));
        }
        return out.toString();
    }

    /**
     * The field-id-to-name map the metrics config is keyed by. Types are not
     * consulted for that lookup -- the type a bound is decoded with comes from
     * the payload, which took it from the parquet file, exactly as the footer
     * reader does.
     */
    private Schema nameSchema(JsonNode sidecar) {
        List<Types.NestedField> fields = new ArrayList<>();

        for (JsonNode column : sidecar.get("columns")) {
            fields.add(Types.NestedField.optional(column.get("id").asInt(),
                                                  column.get("name").asText(),
                                                  Types.StringType.get()));
        }
        return new Schema(fields);
    }

    private JsonNode readSidecar(String fixture) throws IOException {
        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream(FIXTURE_DIR + "/" + fixture + ".json")) {
            assertNotNull(in, "missing fixture " + fixture + ".json");
            return new ObjectMapper().readTree(
                    new String(readAll(in), StandardCharsets.UTF_8));
        }
    }

    private File fixtureFile(String name) {
        URL url = getClass().getClassLoader().getResource(FIXTURE_DIR + "/" + name);
        assertNotNull(url, "missing fixture " + name);
        File file = new File(url.getFile());
        assertTrue(file.isFile(), "missing fixture " + name);
        return file;
    }

    private static byte[] readAll(InputStream in) throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    /**
     * Turn the parsed sidecar back into the plain maps and lists the payload
     * arrives as in production, so the code under test sees what a request
     * body really hands it rather than a Jackson tree.
     */
    private Object toPlain(JsonNode node) throws IOException {
        return new ObjectMapper().treeToValue(node, Object.class);
    }
}
