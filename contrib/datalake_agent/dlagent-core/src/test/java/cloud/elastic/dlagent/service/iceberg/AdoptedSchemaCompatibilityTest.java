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


package cloud.elastic.dlagent.service.iceberg;

import cloud.elastic.dlagent.api.model.RequestedColumn;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * cloud#610: a local definition may be bound to a table the catalog already
 * holds (adopting CREATE, or a mount) only when the reader decodes the table
 * correctly as that definition.  See ExistingTableSchemaCheck.
 */
public class AdoptedSchemaCompatibilityTest {

    private static final TableIdentifier ID = TableIdentifier.of("public", "t");

    /** A local column as the kernel describes it: name and PostgreSQL type tag. */
    private static RequestedColumn pg(String name, String pgType) {
        return new RequestedColumn(name, pgType, null);
    }

    private static Schema table(Types.NestedField... fields) {
        return new Schema(fields);
    }

    private static Types.NestedField opt(int id, String name, Type type) {
        return Types.NestedField.optional(id, name, type);
    }

    private static void accept(List<RequestedColumn> local, Schema existing) {
        ExistingTableSchemaCheck.check(ID, local, existing);
    }

    private static String reject(List<RequestedColumn> local, Schema existing) {
        return assertThrows(IllegalArgumentException.class,
                () -> ExistingTableSchemaCheck.check(ID, local, existing)).getMessage();
    }

    // ---- bound: the reader decodes the table as the definition --------------

    @Test
    public void identicalDefinitionIsBound() {
        accept(Arrays.asList(pg("id", "int4"), pg("v", "text")),
               table(opt(1, "id", Types.IntegerType.get()), opt(2, "v", Types.StringType.get())));
    }

    @Test
    public void nullabilityDoesNotMatter() {
        accept(Collections.singletonList(pg("id", "int4")),
               table(Types.NestedField.required(1, "id", Types.IntegerType.get())));
    }

    @Test
    public void namesMatchCaseInsensitivelyLikeTheReader() {
        accept(Arrays.asList(pg("id", "int4"), pg("name", "text")),
               table(opt(1, "Id", Types.IntegerType.get()), opt(2, "Name", Types.StringType.get())));
    }

    @Test
    public void typesTheReaderDecodesAreAccepted() {
        // uuid / time / unconstrained numeric must not be compared through the
        // kernel's lossy Iceberg mapping (string / string / decimal(38,9))
        accept(Arrays.asList(pg("id", "uuid"), pg("t", "time"), pg("amount", "numeric"),
                             pg("s", "int2"), pg("ts", "timestamptz"), pg("j", "other")),
               table(opt(1, "id", Types.UUIDType.get()), opt(2, "t", Types.TimeType.get()),
                     opt(3, "amount", Types.DecimalType.of(10, 2)), opt(4, "s", Types.IntegerType.get()),
                     opt(5, "ts", Types.TimestampType.withZone()), opt(6, "j", Types.StringType.get())));
    }

    @Test
    public void definitionInTheTablesOwnOrderWithIdsOneToNIsBound() {
        accept(Arrays.asList(pg("id", "int4"), pg("v", "text"), pg("n", "int8")),
               table(opt(1, "id", Types.IntegerType.get()), opt(2, "v", Types.StringType.get()),
                     opt(3, "n", Types.LongType.get())));
    }

    // ---- refused: the reader would decode the table wrongly -----------------

    @Test
    public void anotherOrderIsRejected() {
        // The reader and the writer take the attribute number as the field-id.
        String msg = reject(Arrays.asList(pg("v", "text"), pg("id", "int4")),
                            table(opt(1, "id", Types.IntegerType.get()), opt(2, "v", Types.StringType.get())));
        assertTrue(msg.contains("column \"v\": is column 2 of the existing table, declared at position 1"), msg);
    }

    @Test
    public void someColumnsOnlyIsRejected() {
        String msg = reject(Collections.singletonList(pg("id", "int4")),
                            table(opt(1, "id", Types.IntegerType.get()), opt(2, "v", Types.StringType.get())));
        assertTrue(msg.contains("the local table declares 1 columns, the existing table has 2"), msg);
        // including a nested column the reader could not decode anyway
        reject(Arrays.asList(pg("id", "int4"), pg("v", "text")),
               table(opt(1, "id", Types.IntegerType.get()), opt(2, "v", Types.StringType.get()),
                     opt(3, "attrs", Types.MapType.ofOptional(4, 5,
                             Types.StringType.get(), Types.StringType.get()))));
    }

    @Test
    public void fieldIdsThatAreNotPositionsAreRejected() {
        // Another engine dropped column 2: v keeps field-id 3 at position 2.
        String msg = reject(Arrays.asList(pg("id", "int4"), pg("v", "text")),
                            table(opt(1, "id", Types.IntegerType.get()), opt(3, "v", Types.StringType.get())));
        assertTrue(msg.contains("existing column \"v\" has field-id 3 at position 2"), msg);
    }

    @Test
    public void typeTheReaderCannotDecodeIsRejected() {
        String msg = reject(Collections.singletonList(pg("id", "text")),
                            table(opt(1, "id", Types.IntegerType.get())));
        assertTrue(msg.contains("column \"id\": local type text cannot read existing table type int"), msg);
        reject(Collections.singletonList(pg("t", "time")), table(opt(1, "t", Types.StringType.get())));
        reject(Collections.singletonList(pg("id", "int8")), table(opt(1, "id", Types.IntegerType.get())));
        reject(Collections.singletonList(pg("id", "int4")), table(opt(1, "id", Types.LongType.get())));
        reject(Collections.singletonList(pg("ts", "timestamptz")),
               table(opt(1, "ts", Types.TimestampType.withoutZone())));
        reject(Collections.singletonList(pg("j", "other")), table(opt(1, "j", Types.IntegerType.get())));
        // fixed(n) is FIXED_LEN_BYTE_ARRAY; the reader reads bytea through a BYTE_ARRAY scanner.
        reject(Collections.singletonList(pg("b", "bytea")), table(opt(1, "b", Types.FixedType.ofLength(16))));
    }

    @Test
    public void columnNotInTheTableIsRejected() {
        String msg = reject(Arrays.asList(pg("id", "int4"), pg("extra", "int4")),
                            table(opt(1, "id", Types.IntegerType.get())));
        assertTrue(msg.contains("column \"extra\": not in the existing table"), msg);
        reject(Collections.singletonList(pg("other_id", "int4")), table(opt(1, "id", Types.IntegerType.get())));
    }

    @Test
    public void dottedNameDoesNotMatchNestedField() {
        String msg = reject(Collections.singletonList(pg("s.x", "int4")),
                            table(opt(1, "s", Types.StructType.of(opt(2, "x", Types.IntegerType.get())))));
        assertTrue(msg.contains("column \"s.x\": not in the existing table"), msg);
    }

    @Test
    public void nestedFieldTheReaderReachesFirstIsRejected() {
        // The reader binds the first parquet leaf of that name, depth first:
        // s.id (a string) comes before the top-level int id.
        String msg = reject(Arrays.asList(pg("id", "int4"), pg("s", "other")),
                            table(opt(1, "id", Types.IntegerType.get()),
                                  opt(2, "s", Types.StructType.of(opt(3, "id", Types.StringType.get())))));
        assertFalse(msg.contains("nested field"), msg);
        msg = reject(Arrays.asList(pg("s", "other"), pg("id", "int4")),
                     table(opt(1, "s", Types.StructType.of(opt(3, "id", Types.StringType.get()))),
                           opt(2, "id", Types.IntegerType.get())));
        assertTrue(msg.contains("column \"id\": the reader would read nested field \"s.id\""), msg);
        // a map's value leaf is named "value"
        reject(Collections.singletonList(pg("value", "int4")),
               table(opt(1, "attrs", Types.MapType.ofOptional(3, 4,
                             Types.StringType.get(), Types.StringType.get())),
                     opt(2, "value", Types.IntegerType.get())));
    }

    @Test
    public void twoLocalColumnsMatchingOneExistingColumnAreRejected() {
        String msg = reject(Arrays.asList(pg("A", "int4"), pg("a", "int4")),
                            table(opt(1, "a", Types.IntegerType.get()), opt(2, "b", Types.IntegerType.get())));
        assertTrue(msg.contains("column \"a\": is column 1 of the existing table, declared at position 2"), msg);
    }

    @Test
    public void existingNamesDifferingOnlyInCaseAreRejectedClearly() {
        String msg = reject(Collections.singletonList(pg("id", "int4")),
                            table(opt(1, "id", Types.IntegerType.get()), opt(2, "ID", Types.IntegerType.get())));
        assertTrue(msg.contains("column \"id\": matches 2 columns of the existing table that differ only in case"), msg);
    }

    @Test
    public void onlyAsciiLettersFoldLikeTheReader() {
        // pg_strcasecmp leaves non-ASCII letters alone
        reject(Collections.singletonList(pg("ärger", "text")),
               table(opt(1, "Ärger", Types.StringType.get())));
        accept(Arrays.asList(pg("Ä", "text"), pg("ä", "text")),
               table(opt(1, "Ä", Types.StringType.get()), opt(2, "ä", Types.StringType.get())));
    }

    @Test
    public void withoutPgTypeFallsBackToExactIcebergType() {
        // A kernel too old to send pg-type: compare the Iceberg types it sent.
        accept(Collections.singletonList(new RequestedColumn("id", null, Types.IntegerType.get())),
               table(opt(1, "id", Types.IntegerType.get())));
        reject(Collections.singletonList(new RequestedColumn("id", null, Types.StringType.get())),
               table(opt(1, "id", Types.IntegerType.get())));
        // The fallback takes each column's Iceberg type by name, whatever order the converted schema has.
        Map<String, Object> idField = new HashMap<>();
        idField.put("name", "id");
        Map<String, Object> vField = new HashMap<>();
        vField.put("name", "v");
        Map<String, Object> request = new HashMap<>();
        request.put("fields", Arrays.asList(idField, vField));
        List<RequestedColumn> fromOldKernel = RequestedColumn.fromSchemaMap(request,
                table(opt(1, "v", Types.StringType.get()), opt(2, "id", Types.IntegerType.get())));
        accept(fromOldKernel, table(opt(1, "id", Types.IntegerType.get()), opt(2, "v", Types.StringType.get())));
        String msg = reject(Collections.singletonList(new RequestedColumn("id", null, null)),
                            table(opt(1, "id", Types.IntegerType.get())));
        assertTrue(msg.contains("local type (unknown) cannot read"), msg);
    }

    // ---- malformed requests are a 400, not a 500 ----------------------------

    @Test
    public void malformedRequestSchemaIsAnIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> RequestedColumn.fromSchemaMap(new HashMap<>(), null));
        Map<String, Object> noName = new HashMap<>();
        noName.put("fields", Collections.singletonList(new HashMap<String, Object>()));
        assertThrows(IllegalArgumentException.class, () -> RequestedColumn.fromSchemaMap(noName, null));
        Map<String, Object> field = new HashMap<>();
        field.put("name", "id");
        field.put(RequestedColumn.KEY_PG_TYPE, 23);
        Map<String, Object> badType = new HashMap<>();
        badType.put("fields", Collections.singletonList(field));
        assertThrows(IllegalArgumentException.class, () -> RequestedColumn.fromSchemaMap(badType, null));
    }
}
