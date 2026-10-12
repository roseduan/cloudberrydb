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

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.UnicodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Build manifest {@link Metrics} from the statistics a segment read out of the
 * file it had just written, instead of reopening that file to read them again
 * (issue #442).
 *
 * <p>The statistics arrive as wire format v1: per column, a field id taken
 * from the parquet file itself, a type tag derived from the file's own schema,
 * counts, and one base64 bound pair per row group still in parquet's encoding.
 * For every type this writer emits, that encoding is byte-identical to
 * Iceberg's, so no value is reconstructed on the way -- the bytes are decoded
 * here only because merging and truncating need values, and they go back out
 * through Iceberg's own encoder.
 *
 * <p>What this class produces has to match, field for field, what
 * {@code ParquetUtil.footerMetrics} produced from the same file, because the
 * manifest statistics decide which files a scan can skip. So the order of
 * operations below deliberately mirrors it: per-column metrics mode first,
 * counts, then bounds merged across row groups with Iceberg's comparators,
 * truncated by the column's mode, and finally re-encoded. Merging is done here
 * rather than on the segment because parquet orders strings by UTF-8 bytes and
 * Iceberg by UTF-16 code units, and the two disagree outside the basic plane.
 *
 * <p>Anything unrecognized -- a future format version, a type tag this build
 * does not know, a bound that will not decode -- yields no metrics for that
 * column or file, and the caller falls back to reading the footer. Missing
 * statistics cost pruning; wrong ones cost rows.
 */
public final class IcebergWriteMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergWriteMetrics.class);

    /** The only wire format this build understands. */
    public static final int WIRE_FORMAT_VERSION = 1;

    /**
     * Field-id-to-name view of a position-delete file, so its columns resolve
     * to the names {@code MetricsConfig.forPositionDelete()} pins to Full.
     */
    private static final Schema POSITION_DELETE_SCHEMA = new Schema(
            org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH,
            org.apache.iceberg.MetadataColumns.DELETE_FILE_POS);

    private IcebergWriteMetrics() {
    }

    /** The schema whose names a position-delete file's statistics resolve against. */
    public static Schema positionDeleteSchema() {
        return POSITION_DELETE_SCHEMA;
    }

    /**
     * Build Metrics from a fragment's statistics payload.
     *
     * @param columnStats the {@code column_stats} object as parsed from the
     *                    request, or null when the fragment carried none
     * @param recordCount the file's row count, from the fragment
     * @param schema      the schema the field ids resolve against, used only to
     *                    name columns for the metrics config -- the table's for
     *                    a data file, {@link #positionDeleteSchema()} for a
     *                    delete file
     * @param metricsConfig the table's metrics config, or the position-delete
     *                    one for a delete file
     * @return the metrics, or null if the payload is absent or unusable
     */
    public static Metrics fromColumnStats(Object columnStats,
                                          long recordCount,
                                          Schema schema,
                                          MetricsConfig metricsConfig) {
        if (columnStats == null || schema == null) {
            return null;
        }

        try {
            return build(asMap(columnStats), recordCount, schema,
                         metricsConfig != null ? metricsConfig : MetricsConfig.getDefault());
        } catch (Exception e) {
            /*
             * Never let a malformed payload fail a commit: the file is written
             * either way, and the caller still has the footer to read.
             */
            LOG.warn("unusable write-time column statistics, falling back to the footer: {}",
                     e.toString());
            return null;
        }
    }

    private static Metrics build(Map<String, Object> stats,
                                 long recordCount,
                                 Schema schema,
                                 MetricsConfig metricsConfig) {
        Number version = (Number) stats.get("v");
        if (version == null || version.intValue() != WIRE_FORMAT_VERSION) {
            LOG.warn("column statistics wire format {} is not understood by this build",
                     version);
            return null;
        }

        Object colsObj = stats.get("cols");
        if (!(colsObj instanceof List)) {
            return null;
        }
        List<?> cols = (List<?>) colsObj;

        Map<Integer, Long> columnSizes = new HashMap<>();
        Map<Integer, Long> valueCounts = new HashMap<>();
        Map<Integer, Long> nullValueCounts = new HashMap<>();
        Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
        Map<Integer, ByteBuffer> upperBounds = new HashMap<>();

        for (Object colObj : cols) {
            Map<String, Object> col = asMap(colObj);
            Number idNum = (Number) col.get("id");
            if (idNum == null) {
                continue;
            }
            int fieldId = idNum.intValue();

            Type type = parseType((String) col.get("t"));
            if (type == null) {
                /*
                 * A tag this build does not know: keep nothing for the column
                 * rather than guess at how to read its bytes.
                 */
                continue;
            }

            Long size = asLong(col.get("sz"));
            if (size != null) {
                columnSizes.put(fieldId, size);
            }

            /*
             * The per-column mode gates everything below it, exactly as it does
             * when the metrics come from the footer: "none" records only the
             * column's size, "counts" stops before the bounds.
             */
            MetricsModes.MetricsMode mode =
                    MetricsUtil.metricsMode(schema, metricsConfig, fieldId);
            if (mode instanceof MetricsModes.None) {
                continue;
            }

            Long valueCount = asLong(col.get("vc"));
            if (valueCount != null) {
                valueCounts.put(fieldId, valueCount);
            }
            /*
             * An absent null count means the segment could not determine one
             * for every row group, which is the same state the footer reader
             * treats as "no null count for this column" -- so it stays absent
             * rather than being guessed at as zero.
             */
            Long nullCount = asLong(col.get("nc"));
            if (nullCount != null) {
                nullValueCounts.put(fieldId, nullCount);
            }

            if (mode instanceof MetricsModes.Counts) {
                continue;
            }

            Object rowGroupsObj = col.get("rg");
            if (!(rowGroupsObj instanceof List)) {
                continue;
            }
            mergeBounds((List<?>) rowGroupsObj, fieldId, type, mode,
                        lowerBounds, upperBounds);
        }

        /*
         * nanValueCounts stays empty: the footer path also produced an empty
         * map here, because parquet drops min/max for a chunk containing NaN
         * rather than counting them.
         */
        return new Metrics(recordCount, columnSizes, valueCounts, nullValueCounts,
                           Collections.emptyMap(), lowerBounds, upperBounds);
    }

    /**
     * Fold one column's per-row-group bounds into a file-level pair.
     *
     * <p>Each candidate is compared against what is already stored -- which is
     * the truncated form, not the original -- and only then truncated itself.
     * That ordering is not incidental: it is what the footer path does, and
     * doing it the other way round would produce different bytes for the same
     * data.
     */
    private static void mergeBounds(List<?> rowGroups,
                                    int fieldId,
                                    Type type,
                                    MetricsModes.MetricsMode mode,
                                    Map<Integer, ByteBuffer> lowerBounds,
                                    Map<Integer, ByteBuffer> upperBounds) {
        Comparator<Object> comparator = Comparators.forType(type.asPrimitiveType());
        Object min = null;
        Object max = null;

        for (Object entryObj : rowGroups) {
            Map<String, Object> entry = asMap(entryObj);
            Object lower = decode(type, (String) entry.get("lo"));
            Object upper = decode(type, (String) entry.get("hi"));

            if (lower == null || upper == null) {
                /*
                 * One unreadable row group makes the whole column's range
                 * unknowable: the rows it covers would be outside whatever the
                 * others agree on, and a scan would skip this file for them.
                 */
                return;
            }

            if (min == null || comparator.compare(lower, min) < 0) {
                min = truncateLower(type, lower, mode);
            }
            if (max == null || comparator.compare(upper, max) > 0) {
                Object truncated = truncateUpper(type, upper, mode);
                if (truncated != null) {
                    max = truncated;
                }
            }
        }

        if (min != null) {
            lowerBounds.put(fieldId, Conversions.toByteBuffer(type, min));
        }
        if (max != null) {
            upperBounds.put(fieldId, Conversions.toByteBuffer(type, max));
        }
    }

    /**
     * Shorten a lower bound to the column's truncation length, downwards.
     *
     * <p>Only strings and binaries can be shortened at all; everything else is
     * already as short as its type allows. Iceberg's own helpers do the work
     * because getting the direction wrong here -- rounding a lower bound up --
     * would make scans skip files that hold matching rows.
     */
    @SuppressWarnings("unchecked")
    private static Object truncateLower(Type type, Object value,
                                        MetricsModes.MetricsMode mode) {
        if (!(mode instanceof MetricsModes.Truncate)) {
            return value;
        }
        int length = ((MetricsModes.Truncate) mode).length();

        switch (type.typeId()) {
            case STRING:
                return UnicodeUtil.truncateStringMin(
                        Literal.of((CharSequence) value), length).value();
            case FIXED:
            case BINARY:
                return BinaryUtil.truncateBinaryMin(
                        Literal.of((ByteBuffer) value), length).value();
            default:
                return value;
        }
    }

    /**
     * Shorten an upper bound to the column's truncation length, upwards.
     *
     * <p>Returns null when no shortened value can stand above the original --
     * a string of nothing but the highest code point has no truncated ceiling.
     * The caller then keeps whatever it had, which is what the footer path
     * does too.
     */
    @SuppressWarnings("unchecked")
    private static Object truncateUpper(Type type, Object value,
                                        MetricsModes.MetricsMode mode) {
        if (!(mode instanceof MetricsModes.Truncate)) {
            return value;
        }
        int length = ((MetricsModes.Truncate) mode).length();

        switch (type.typeId()) {
            case STRING: {
                Literal<CharSequence> truncated = UnicodeUtil.truncateStringMax(
                        Literal.of((CharSequence) value), length);
                return truncated == null ? null : truncated.value();
            }
            case FIXED:
            case BINARY: {
                Literal<ByteBuffer> truncated = BinaryUtil.truncateBinaryMax(
                        Literal.of((ByteBuffer) value), length);
                return truncated == null ? null : truncated.value();
            }
            default:
                return value;
        }
    }

    /**
     * Read one bound back into an Iceberg value.
     *
     * <p>The bytes are handed to Iceberg's own decoder unmodified, which is the
     * whole argument for this route: parquet wrote them in the encoding Iceberg
     * reads. The one place the two differ is decimal, where the writer emits a
     * fixed width and Iceberg the narrowest that fits -- decoding to a
     * BigDecimal and re-encoding through {@code Conversions.toByteBuffer} is
     * what reconciles them, since leading sign bytes do not change the value.
     */
    private static Object decode(Type type, String encoded) {
        if (encoded == null) {
            return null;
        }
        try {
            byte[] raw = Base64.getDecoder().decode(encoded);
            return Conversions.fromByteBuffer(type, ByteBuffer.wrap(raw));
        } catch (RuntimeException e) {
            LOG.warn("undecodable {} bound in write-time column statistics: {}",
                     type, e.toString());
            return null;
        }
    }

    /**
     * Resolve a wire type tag to the Iceberg type whose encoding the bound
     * bytes are in.
     *
     * <p>The tag describes the PARQUET FILE's column, not the table's. For
     * "char", time and interval the two disagree -- the file says binary, the
     * table says string -- and the file wins, because that is the type the
     * footer reader would have derived and therefore the one whose truncation
     * rules the old bounds followed.
     */
    private static Type parseType(String tag) {
        if (tag == null) {
            return null;
        }

        switch (tag) {
            case "bool":
                return Types.BooleanType.get();
            case "i32":
                return Types.IntegerType.get();
            case "i64":
                return Types.LongType.get();
            case "f32":
                return Types.FloatType.get();
            case "f64":
                return Types.DoubleType.get();
            case "date":
                return Types.DateType.get();
            case "ts":
                return Types.TimestampType.withoutZone();
            case "tstz":
                return Types.TimestampType.withZone();
            case "str":
                return Types.StringType.get();
            case "bin":
                return Types.BinaryType.get();
            default:
                return parseDecimal(tag);
        }
    }

    /** "dec(P,S)" -- the only tag carrying parameters. */
    private static Type parseDecimal(String tag) {
        if (!tag.startsWith("dec(") || !tag.endsWith(")")) {
            return null;
        }
        String[] parts = tag.substring(4, tag.length() - 1).split(",");
        if (parts.length != 2) {
            return null;
        }
        try {
            return Types.DecimalType.of(Integer.parseInt(parts[0].trim()),
                                        Integer.parseInt(parts[1].trim()));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        throw new IllegalArgumentException(
                "column statistics entry is not an object: " + value);
    }

    private static Long asLong(Object value) {
        return value instanceof Number ? ((Number) value).longValue() : null;
    }
}
