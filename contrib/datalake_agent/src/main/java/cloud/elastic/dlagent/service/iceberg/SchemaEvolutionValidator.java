package cloud.elastic.dlagent.service.iceberg;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;

/**
 * Validates Iceberg schema-evolution type changes for builtin catalog tables.
 *
 * <p>Only widening type promotions are allowed (issue #401), matching Iceberg's own
 * {@link TypeUtil#isPromotionAllowed(Type, Type.PrimitiveType)}: int-&gt;long, float-&gt;double,
 * and decimal(p,s)-&gt;decimal(p',s) with the same scale and a non-decreasing precision.
 */
public final class SchemaEvolutionValidator {

    private SchemaEvolutionValidator() {
    }

    /**
     * Throws {@link IllegalArgumentException} if changing a column from {@code from} to
     * {@code to} is not an allowed widening promotion.
     */
    public static void assertWideningAllowed(Type from, Type.PrimitiveType to) {
        if (!TypeUtil.isPromotionAllowed(from, to)) {
            throw new IllegalArgumentException(
                    "Cannot change column type: " + from + " -> " + to + " (only widening is allowed)");
        }
    }
}
