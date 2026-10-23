package cloud.elastic.dlagent.service.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * One schema-evolution operation in an ALTER TABLE request (issue #401).
 *
 * <p>{@code op} is one of: addColumn, dropColumn, renameColumn, updateColumn,
 * makeOptional, requireColumn. {@code name} is the (current) column name;
 * {@code newName} is used by renameColumn; {@code type} is the Iceberg target
 * type string (e.g. "long", "double", "decimal(20,2)") used by addColumn/updateColumn.
 */
public class SchemaOp {
    private final String op;
    private final String name;
    private final String newName;
    private final String type;

    public SchemaOp(String op, String name, String newName, String type) {
        this.op = op;
        this.name = name;
        this.newName = newName;
        this.type = type;
    }

    public String getOp() {
        return op;
    }

    public String getName() {
        return name;
    }

    public String getNewName() {
        return newName;
    }

    public String getType() {
        return type;
    }

    /**
     * Build a list of ops from the raw {@code operations} array in an updateSchema
     * request body. Returns an empty list for a null/absent array.
     */
    public static List<SchemaOp> fromRequestList(List<Map<String, Object>> raw) {
        List<SchemaOp> ops = new ArrayList<>();
        if (raw == null) {
            return ops;
        }
        for (Map<String, Object> m : raw) {
            ops.add(new SchemaOp(
                    (String) m.get("op"),
                    (String) m.get("name"),
                    (String) m.get("newName"),
                    (String) m.get("type")));
        }
        return ops;
    }
}
