/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import cloud.elastic.dlagent.service.ServiceResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The catalog prefix and the table name must reach the operation identically,
 * although the two transports carry them in completely different places.
 *
 * <p>REST takes them out of the URL path; gRPC takes them out of call
 * metadata. That was a deliberate choice -- it keeps the request BODY
 * byte-identical between the transports, so the C client can reuse the buffer
 * cJSON already built -- but it means the two sides decode these two values
 * with no shared code at all, which is exactly where a difference hides.
 */
class RoutingParityTest {

    private static void assertRouting(Op op, String prefix, String table) throws Exception {
        try (TransportHarness harness = new TransportHarness(() -> ServiceResult.success(null))) {
            harness.rest(op, prefix, table, "{}");
            RecordingOperations.Call rest = harness.operations().lastCall();

            harness.operations().reset();
            harness.grpc(op, prefix, table, "{}");
            RecordingOperations.Call grpc = harness.operations().lastCall();

            assertNotNull(rest, op + ": the REST handler never called the operation");
            assertNotNull(grpc, op + ": the gRPC handler never called the operation");
            assertEquals(rest.operation, grpc.operation,
                    op + ": the two transports called different operations");
            assertEquals(rest.prefix, grpc.prefix,
                    op + ": prefix differs -- rest '" + rest.prefix + "' grpc '" + grpc.prefix + "'");
            assertEquals(rest.table, grpc.table,
                    op + ": table differs -- rest '" + rest.table + "' grpc '" + grpc.table + "'");
            if (table != null) {
                assertEquals(table, rest.table, op + ": REST did not decode the table name");
            }
        }
    }

    @Test
    @DisplayName("both routing values present")
    void bothPresent() throws Exception {
        assertRouting(Op.LOAD_TABLE, "my_catalog", "my_table");
    }

    @Test
    @DisplayName("no prefix: the path has no {prefix} segment, the metadata has no key")
    void noPrefix() throws Exception {
        assertRouting(Op.LOAD_TABLE, null, "my_table");
    }

    @Test
    @DisplayName("an operation that takes no table")
    void noTable() throws Exception {
        assertRouting(Op.LIST_CATALOGS, "my_catalog", null);
    }

    /*
     * THE TABLE NAME IS PERCENT-ENCODED ON BOTH TRANSPORTS.
     *
     * The C client escapes it with curl_easy_escape (encode_path_segment):
     * into the URL path for REST, into the dlagent-table metadata value for
     * gRPC, which may only carry ASCII. Spring decodes the path variable; the
     * gRPC agent decodes the metadata value the same way, so the operation
     * sees the same string. The harness escapes exactly as the client does
     * (TransportHarness.curlEscape), and the names below cover non-ASCII,
     * '+' (a path keeps it, form decoding would turn it into a space), '%',
     * space and other reserved characters.
     *
     * The DATA plane's x-gp-* values are escaped the same way by the client,
     * so what reaches the agent there is ASCII and byte-identical to the REST
     * header value.
     */

    @Test
    @DisplayName("a table name with a space")
    void spaceInTableName() throws Exception {
        assertRouting(Op.LOAD_TABLE, "cat", "my table");
    }

    @Test
    @DisplayName("table names with non-ASCII and reserved characters")
    void encodedTableNames() throws Exception {
        String[] names = {
            "\u5317\u4eac",
            "\u5317\u4eac+x y",
            "a+b",
            "100%",
            "%41",
            "semi;colon",
            "q?x=1&y#z",
            "emoji\ud83d\ude80",
            "t~._-",
        };
        for (String name : names) {
            assertRouting(Op.TABLE_EXISTS, null, name);
            assertRouting(Op.LOAD_TABLE, "cat", name);
        }
    }
}
