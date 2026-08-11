package cloud.elastic.dlagent.service.iceberg;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link SchemaEvolutionOps#apply} against a real Iceberg table on a temp
 * Hadoop catalog (no Spring, no object storage).
 */
public class SchemaEvolutionOpsTest {

    private final SchemaConverter converter = new SchemaConverter();

    private Table newTable(Path dir) {
        HadoopCatalog catalog = new HadoopCatalog(new Configuration(), dir.toString());
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "val", Types.IntegerType.get()));
        return catalog.createTable(TableIdentifier.of("db", "t"), schema);
    }

    @Test
    void addColumnIsOptionalAndWidenType(@TempDir Path dir) {
        Table t = newTable(dir);
        UpdateSchema us = t.updateSchema();
        SchemaEvolutionOps.apply(us, t.schema(), Arrays.asList(
                new SchemaOp("addColumn", "note", null, "string"),
                new SchemaOp("updateColumn", "val", null, "long")), converter);
        us.commit();
        t.refresh();
        assertEquals(Types.LongType.get(), t.schema().findField("val").type());
        assertNotNull(t.schema().findField("note"));
        assertTrue(t.schema().findField("note").isOptional());
    }

    @Test
    void dropColumn(@TempDir Path dir) {
        Table t = newTable(dir);
        UpdateSchema us = t.updateSchema();
        SchemaEvolutionOps.apply(us, t.schema(),
                Collections.singletonList(new SchemaOp("dropColumn", "val", null, null)), converter);
        us.commit();
        t.refresh();
        assertNull(t.schema().findField("val"));
    }

    @Test
    void renameColumnKeepsFieldId(@TempDir Path dir) {
        Table t = newTable(dir);
        int idBefore = t.schema().findField("val").fieldId();
        UpdateSchema us = t.updateSchema();
        SchemaEvolutionOps.apply(us, t.schema(),
                Collections.singletonList(new SchemaOp("renameColumn", "val", "amount", null)), converter);
        us.commit();
        t.refresh();
        assertNull(t.schema().findField("val"));
        assertNotNull(t.schema().findField("amount"));
        assertEquals(idBefore, t.schema().findField("amount").fieldId());
    }

    @Test
    void setNotNullNeedsAllowIncompatible(@TempDir Path dir) {
        Table t = newTable(dir);
        UpdateSchema us = t.updateSchema();
        SchemaEvolutionOps.apply(us, t.schema(),
                Collections.singletonList(new SchemaOp("requireColumn", "val", null, null)), converter);
        us.commit();
        t.refresh();
        assertTrue(t.schema().findField("val").isRequired());
    }

    @Test
    void dropNotNull(@TempDir Path dir) {
        Table t = newTable(dir);
        UpdateSchema us = t.updateSchema();
        SchemaEvolutionOps.apply(us, t.schema(),
                Collections.singletonList(new SchemaOp("makeOptional", "id", null, null)), converter);
        us.commit();
        t.refresh();
        assertTrue(t.schema().findField("id").isOptional());
    }

    @Test
    void rejectsNarrowing(@TempDir Path dir) {
        Table t = newTable(dir);
        // First widen id int->long (allowed).
        UpdateSchema us1 = t.updateSchema();
        SchemaEvolutionOps.apply(us1, t.schema(),
                Collections.singletonList(new SchemaOp("updateColumn", "id", null, "long")), converter);
        us1.commit();
        t.refresh();
        // Now narrowing long->int must be rejected.
        UpdateSchema us2 = t.updateSchema();
        assertThrows(IllegalArgumentException.class, () -> SchemaEvolutionOps.apply(us2, t.schema(),
                Collections.singletonList(new SchemaOp("updateColumn", "id", null, "int")), converter));
    }
}
