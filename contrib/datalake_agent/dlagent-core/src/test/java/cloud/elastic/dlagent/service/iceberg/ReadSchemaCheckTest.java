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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The read-time schema check (lightning issue #461): a table bound by the
 * rules of {@link ExistingTableSchemaCheck} whose schema another engine has
 * since changed is refused at read time instead of being decoded by a
 * definition that no longer describes it.
 *
 * The schema changes go through Iceberg's own UpdateSchema on a real table,
 * so the field-ids are the ones Spark would leave behind.
 */
public class ReadSchemaCheckTest {

    private static final TableIdentifier ID = TableIdentifier.of("db", "t");

    /** The local definition the table was bound with: (id int, v text). */
    private static final List<RequestedColumn> LOCAL =
            Arrays.asList(new RequestedColumn("id", "int4", null),
                          new RequestedColumn("v", "text", null));

    /**
     * The catalog behind the last newTable(); its Table keeps using it, so it
     * stays open until the test ends or newTable() is called again.
     */
    private HadoopCatalog catalog;

    @AfterEach
    void closeCatalog() throws IOException {
        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
    }

    /** A new table in its own catalog; the previous one is closed first. */
    private Table newTable(Path dir) {
        try {
            closeCatalog();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catalog = new HadoopCatalog(new Configuration(), dir.toString());
        return catalog.createTable(ID, new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "v", Types.StringType.get())));
    }

    private static String refused(Table t) {
        return assertThrows(IllegalArgumentException.class,
                () -> ExistingTableSchemaCheck.checkForRead(ID, LOCAL, t.schema())).getMessage();
    }

    @Test
    void unchangedSchemaIsRead(@TempDir Path dir) {
        ExistingTableSchemaCheck.checkForRead(ID, LOCAL, newTable(dir).schema());
    }

    @Test
    void droppedColumnIsRefused(@TempDir Path dir) {
        Table t = newTable(dir);
        t.updateSchema().deleteColumn("v").commit();
        t.refresh();
        String msg = refused(t);
        assertTrue(msg.contains("no longer matches this table's definition"), msg);
        assertTrue(msg.contains("column \"v\": not in the existing table"), msg);
        assertTrue(msg.contains("recreate the table"), msg);
    }

    @Test
    void droppedAndReAddedColumnIsRefused(@TempDir Path dir) {
        // Same name and type, but a fresh field-id: the old files' v is gone.
        Table t = newTable(dir);
        t.updateSchema().deleteColumn("v").commit();
        t.updateSchema().addColumn("v", Types.StringType.get()).commit();
        t.refresh();
        String msg = refused(t);
        assertTrue(msg.contains("has field-id 3 at position 2"), msg);
    }

    @Test
    void renamedColumnIsRefused(@TempDir Path dir) {
        Table t = newTable(dir);
        t.updateSchema().renameColumn("v", "w").commit();
        t.refresh();
        assertTrue(refused(t).contains("column \"v\": not in the existing table"));
    }

    @Test
    void widenedTypeIsRefused(@TempDir Path dir) {
        // int -> long is a legal Iceberg promotion; the reader still decodes
        // the column through the local int4 scanner.
        Table t = newTable(dir);
        t.updateSchema().updateColumn("id", Types.LongType.get()).commit();
        t.refresh();
        assertTrue(refused(t).contains("local type int4 cannot read existing table type long"));
    }

    @Test
    void appendedColumnIsRead(@TempDir Path dir) {
        // Past the last declared column and the last declared field-id: what
        // the local table reads is unchanged.
        Table t = newTable(dir);
        t.updateSchema().addColumn("extra", Types.IntegerType.get()).commit();
        t.refresh();
        ExistingTableSchemaCheck.checkForRead(ID, LOCAL, t.schema());
    }

    @Test
    void writeIsRefusedLikeARead(@TempDir Path dir) {
        // Commit-time check (issue #461): files written against (id int, v text)
        // must not be committed once v is gone or id has been widened.
        Table t = newTable(dir);
        t.updateSchema().deleteColumn("v").commit();
        t.refresh();
        String msg = assertThrows(IllegalArgumentException.class,
                () -> ExistingTableSchemaCheck.checkForWrite(ID, LOCAL, t.schema())).getMessage();
        assertTrue(msg.contains("cannot be written"), msg);
        assertTrue(msg.contains("column \"v\": not in the existing table"), msg);

        Table w = newTable(dir.resolve("w"));
        w.updateSchema().updateColumn("id", Types.LongType.get()).commit();
        w.refresh();
        assertThrows(IllegalArgumentException.class,
                () -> ExistingTableSchemaCheck.checkForWrite(ID, LOCAL, w.schema()));
    }

    @Test
    void writeToATableWithAnAppendedColumnIsAllowed(@TempDir Path dir) {
        // The written files lack the appended column, which then reads NULL.
        Table t = newTable(dir);
        t.updateSchema().addColumn("extra", Types.IntegerType.get()).commit();
        t.refresh();
        ExistingTableSchemaCheck.checkForWrite(ID, LOCAL, t.schema());
    }

    @Test
    void appendedColumnStillRefusesABind(@TempDir Path dir) {
        // A bind declares the table in full; only reads tolerate the append.
        Table t = newTable(dir);
        t.updateSchema().addColumn("extra", Types.IntegerType.get()).commit();
        t.refresh();
        String msg = assertThrows(IllegalArgumentException.class,
                () -> ExistingTableSchemaCheck.check(ID, LOCAL, t.schema())).getMessage();
        assertTrue(msg.contains("the local table declares 2 columns, the existing table has 3"), msg);
    }

    @Test
    void columnInsertedBeforeTheDeclaredOnesIsRefused(@TempDir Path dir) {
        // Moved to the front: v is no longer at position 2.
        Table t = newTable(dir);
        t.updateSchema().addColumn("extra", Types.IntegerType.get()).moveFirst("extra").commit();
        t.refresh();
        refused(t);
    }
}
