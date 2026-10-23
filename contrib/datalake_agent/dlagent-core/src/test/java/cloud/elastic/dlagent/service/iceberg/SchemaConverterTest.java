package cloud.elastic.dlagent.service.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SchemaConverter
 */
public class SchemaConverterTest {

    private SchemaConverter schemaConverter;

    @BeforeEach
    public void setUp() {
        schemaConverter = new SchemaConverter();
    }

    @Test
    public void testSimpleSchemaConversion() {
        // Create a simple schema
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get())
        );
        
        // Convert schema to JSON
        Map<String, Object> json = schemaConverter.toJson(schema);
        
        // Verify JSON structure
        assertEquals("struct", json.get("type"));
        List<Map<String, Object>> fields = (List<Map<String, Object>>) json.get("fields");
        assertEquals(3, fields.size());
        
        // Verify field properties
        Map<String, Object> idField = fields.get(0);
        assertEquals(1, idField.get("id"));
        assertEquals("id", idField.get("name"));
        assertEquals(true, idField.get("required"));
        assertEquals("long", idField.get("type"));
        
        Map<String, Object> nameField = fields.get(1);
        assertEquals(2, nameField.get("id"));
        assertEquals("name", nameField.get("name"));
        assertEquals(true, nameField.get("required"));
        assertEquals("string", nameField.get("type"));
        
        Map<String, Object> ageField = fields.get(2);
        assertEquals(3, ageField.get("id"));
        assertEquals("age", ageField.get("name"));
        assertEquals(false, ageField.get("required"));
        assertEquals("int", ageField.get("type"));
        
        // Convert JSON back to schema
        Schema convertedSchema = schemaConverter.fromJson(json);
        
        // Verify converted schema
        assertEquals(schema.columns().size(), convertedSchema.columns().size());
        assertEquals(schema.findField("id").fieldId(), convertedSchema.findField("id").fieldId());
        assertEquals(schema.findField("name").fieldId(), convertedSchema.findField("name").fieldId());
        assertEquals(schema.findField("age").fieldId(), convertedSchema.findField("age").fieldId());
        assertEquals(schema.findField("id").isRequired(), convertedSchema.findField("id").isRequired());
        assertEquals(schema.findField("name").isRequired(), convertedSchema.findField("name").isRequired());
        assertEquals(schema.findField("age").isRequired(), convertedSchema.findField("age").isRequired());
    }

    @Test
    public void testComplexSchemaConversion() {
        // Create a complex schema with nested types
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "address", Types.StructType.of(
                Types.NestedField.required(4, "street", Types.StringType.get()),
                Types.NestedField.required(5, "city", Types.StringType.get()),
                Types.NestedField.optional(6, "zip", Types.IntegerType.get())
            )),
            Types.NestedField.optional(7, "phones", Types.ListType.ofOptional(8, Types.StringType.get()))
        );
        
        // Convert schema to JSON
        Map<String, Object> json = schemaConverter.toJson(schema);
        
        // Convert JSON back to schema
        Schema convertedSchema = schemaConverter.fromJson(json);
        
        // Verify converted schema
        assertEquals(schema.columns().size(), convertedSchema.columns().size());
        assertEquals(schema.findField("id").fieldId(), convertedSchema.findField("id").fieldId());
        assertEquals(schema.findField("name").fieldId(), convertedSchema.findField("name").fieldId());
        assertEquals(schema.findField("address").fieldId(), convertedSchema.findField("address").fieldId());
        assertEquals(schema.findField("phones").fieldId(), convertedSchema.findField("phones").fieldId());
        
        // Verify nested fields
        Types.StructType addressType = convertedSchema.findField("address").type().asStructType();
        assertEquals(3, addressType.fields().size());
        assertEquals("street", addressType.fields().get(0).name());
        assertEquals("city", addressType.fields().get(1).name());
        assertEquals("zip", addressType.fields().get(2).name());
    }

    @Test
    public void testFromJsonWithManualMap() {
        // Create a JSON schema manually
        Map<String, Object> json = new HashMap<>();
        json.put("type", "struct");
        
        List<Map<String, Object>> fields = new ArrayList<>();
        
        Map<String, Object> idField = new HashMap<>();
        idField.put("id", 1);
        idField.put("name", "id");
        idField.put("type", "long");
        idField.put("required", true);
        fields.add(idField);
        
        Map<String, Object> nameField = new HashMap<>();
        nameField.put("id", 2);
        nameField.put("name", "name");
        nameField.put("type", "string");
        nameField.put("required", true);
        fields.add(nameField);
        
        json.put("fields", fields);
        
        // Convert JSON to schema
        Schema schema = schemaConverter.fromJson(json);
        
        // Verify schema
        assertEquals(2, schema.columns().size());
        assertEquals(1, schema.findField("id").fieldId());
        assertEquals(2, schema.findField("name").fieldId());
        assertTrue(schema.findField("id").isRequired());
        assertTrue(schema.findField("name").isRequired());
        assertEquals(Types.LongType.get(), schema.findField("id").type());
        assertEquals(Types.StringType.get(), schema.findField("name").type());
    }
}
