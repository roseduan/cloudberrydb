package cloud.elastic.dlagent.service.iceberg;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SchemaEvolutionValidatorTest {

    @Test
    void allowsIntToLong() {
        assertDoesNotThrow(() -> SchemaEvolutionValidator.assertWideningAllowed(
                Types.IntegerType.get(), Types.LongType.get()));
    }

    @Test
    void allowsFloatToDouble() {
        assertDoesNotThrow(() -> SchemaEvolutionValidator.assertWideningAllowed(
                Types.FloatType.get(), Types.DoubleType.get()));
    }

    @Test
    void allowsDecimalPrecisionWiden() {
        assertDoesNotThrow(() -> SchemaEvolutionValidator.assertWideningAllowed(
                Types.DecimalType.of(10, 2), Types.DecimalType.of(20, 2)));
    }

    @Test
    void rejectsLongToInt() {
        assertThrows(IllegalArgumentException.class, () -> SchemaEvolutionValidator.assertWideningAllowed(
                Types.LongType.get(), Types.IntegerType.get()));
    }

    @Test
    void rejectsDecimalScaleChange() {
        assertThrows(IllegalArgumentException.class, () -> SchemaEvolutionValidator.assertWideningAllowed(
                Types.DecimalType.of(10, 2), Types.DecimalType.of(10, 3)));
    }

    @Test
    void rejectsCrossFamily() {
        assertThrows(IllegalArgumentException.class, () -> SchemaEvolutionValidator.assertWideningAllowed(
                Types.IntegerType.get(), Types.StringType.get()));
    }
}
