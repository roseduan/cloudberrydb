package cloud.elastic.dlagent.service.iceberg;

import java.util.List;

import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Applies a list of {@link SchemaOp} to an Iceberg {@link UpdateSchema} for builtin
 * catalog ALTER TABLE (issue #401). Extracted from the service layer so the op-dispatch
 * and validation logic can be unit-tested against a real (temp) Iceberg table without
 * Spring or object storage.
 *
 * <p>Callers commit the {@code UpdateSchema} (or bundle it into a transaction) themselves.
 */
public final class SchemaEvolutionOps {

    private SchemaEvolutionOps() {
    }

    /**
     * Apply each op to {@code us}. {@code current} is the table's schema before this update,
     * used to validate widening for updateColumn. {@code converter} parses target type strings.
     *
     * @throws IllegalArgumentException on an unknown op, a non-widening type change, or a
     *     non-primitive target type.
     */
    public static void apply(UpdateSchema us, Schema current, List<SchemaOp> ops, SchemaConverter converter) {
        for (SchemaOp op : ops) {
            String kind = op.getOp();
            if ("addColumn".equals(kind)) {
                us.addColumn(op.getName(), converter.parseColumnType(op.getType()));
            } else if ("dropColumn".equals(kind)) {
                us.deleteColumn(op.getName());
            } else if ("renameColumn".equals(kind)) {
                us.renameColumn(op.getName(), op.getNewName());
            } else if ("updateColumn".equals(kind)) {
                Type.PrimitiveType newType = converter.parseColumnType(op.getType());
                /* findField returns null when the column is absent from the Iceberg
                 * schema (possible if the PG and Iceberg schemas have drifted); guard
                 * before dereferencing so the caller gets a meaningful error instead of
                 * an NPE surfacing as a generic HTTP 500. */
                Types.NestedField field = current.findField(op.getName());
                if (field == null) {
                    throw new IllegalArgumentException(
                            "column '" + op.getName() + "' not found in Iceberg schema; cannot updateColumn");
                }
                SchemaEvolutionValidator.assertWideningAllowed(field.type(), newType);
                us.updateColumn(op.getName(), newType);
            } else if ("makeOptional".equals(kind)) {
                us.makeColumnOptional(op.getName());
            } else if ("requireColumn".equals(kind)) {
                /* SET NOT NULL: Iceberg treats optional->required as an incompatible change
                 * and rejects it unless allowIncompatibleChanges() is set. PG has already
                 * validated the table has no NULLs, so this is safe here.
                 *
                 * CAUTION: allowIncompatibleChanges() flags the *entire* UpdateSchema
                 * builder, not just this one call, and the flag cannot be cleared. Once a
                 * requireColumn op appears in `ops`, Iceberg's own compatibility and
                 * type-promotion checks are disabled for every op applied afterwards.
                 * That is safe today only because each risky op pre-validates itself
                 * (updateColumn above goes through SchemaEvolutionValidator, and the C
                 * side additionally gates on iceberg_is_widening). Any new op type added
                 * to this dispatch MUST carry its own explicit validation rather than
                 * relying on Iceberg to reject bad input. */
                us.allowIncompatibleChanges().requireColumn(op.getName());
            } else {
                throw new IllegalArgumentException("unknown schema evolution op: " + kind);
            }
        }
    }
}
