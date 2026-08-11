package cloud.elastic.dlagent.service.iceberg;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaOpTest {

    private static Map<String, Object> op(String... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void parsesOperations() {
        List<Map<String, Object>> raw = Arrays.asList(
                op("op", "addColumn", "name", "c", "type", "long"),
                op("op", "renameColumn", "name", "a", "newName", "a2"));
        List<SchemaOp> ops = SchemaOp.fromRequestList(raw);
        assertEquals(2, ops.size());
        assertEquals("addColumn", ops.get(0).getOp());
        assertEquals("c", ops.get(0).getName());
        assertEquals("long", ops.get(0).getType());
        assertNull(ops.get(0).getNewName());
        assertEquals("renameColumn", ops.get(1).getOp());
        assertEquals("a2", ops.get(1).getNewName());
    }

    @Test
    void nullOperationsYieldsEmpty() {
        assertTrue(SchemaOp.fromRequestList(null).isEmpty());
    }
}
