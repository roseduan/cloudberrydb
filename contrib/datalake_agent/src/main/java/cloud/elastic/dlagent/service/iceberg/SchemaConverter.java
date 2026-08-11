package cloud.elastic.dlagent.service.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Utility class for converting between Iceberg Schema and JSON representation
 */
@Component
@Slf4j
public class SchemaConverter {

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Convert a JSON map to Iceberg Schema
     *
     * @param schemaMap JSON representation of schema
     * @return Iceberg Schema
     */
    public Schema fromJson(Map<String, Object> schemaMap) {
        log.debug("Converting JSON to Schema: {}", schemaMap);
        
        // Extract schema ID if available
        Integer schemaId = null;
        if (schemaMap.containsKey("schema-id")) {
            schemaId = (Integer) schemaMap.get("schema-id");
        }
        
        // Extract identifier field IDs if available
        List<Integer> identifierFieldIds = new ArrayList<>();
        if (schemaMap.containsKey("identifier-field-ids")) {
            List<Integer> ids = (List<Integer>) schemaMap.get("identifier-field-ids");
            identifierFieldIds.addAll(ids);
        }
        
        // Check if this is a struct type
        String type = (String) schemaMap.get("type");
        if (!"struct".equals(type)) {
            throw new IllegalArgumentException("Root schema must be a struct type");
        }
        
        // Parse fields
        List<Map<String, Object>> fields = (List<Map<String, Object>>) schemaMap.get("fields");
        List<NestedField> nestedFields = new ArrayList<>();
        
        for (Map<String, Object> field : fields) {
            NestedField nestedField = parseField(field);
            nestedFields.add(nestedField);
        }
        
        // Create schema
        StructType structType = Types.StructType.of(nestedFields);
        
        List<NestedField> columns = structType.fields();
        
        if (schemaId != null) {
            if (!identifierFieldIds.isEmpty()) {
                return new Schema(columns, new HashSet<>(identifierFieldIds));
            } else {
                return new Schema(columns);
            }
        } else {
            return new Schema(columns);
        }
    }

    /**
     * Convert Iceberg Schema to JSON map
     *
     * @param schema Iceberg Schema
     * @return JSON representation of schema
     */
    public Map<String, Object> toJson(Schema schema) {
        log.debug("Converting Schema to JSON: {}", schema);
        
        Map<String, Object> result = new HashMap<>();
        
        // Add schema ID if available
        int schemaId = schema.schemaId();
        result.put("schema-id", schemaId);
        
        // Add identifier field IDs if available
        if (schema.identifierFieldIds() != null && !schema.identifierFieldIds().isEmpty()) {
            result.put("identifier-field-ids", schema.identifierFieldIds());
        }
        
        // Add type and fields
        result.put("type", "struct");
        
        List<Map<String, Object>> fields = new ArrayList<>();
        for (Types.NestedField field : schema.columns()) {
            fields.add(convertField(field));
        }
        
        result.put("fields", fields);
        
        return result;
    }

    /**
     * Parse a field from JSON to NestedField
     */
    private NestedField parseField(Map<String, Object> fieldMap) {
        Integer id = (Integer) fieldMap.get("id");
        String name = (String) fieldMap.get("name");
        Boolean required = (Boolean) fieldMap.get("required");
        
        // Parse type
        Object typeObj = fieldMap.get("type");
        Type fieldType;
        
        if (typeObj instanceof String) {
            // Primitive type
            fieldType = parsePrimitiveType((String) typeObj);
        } else if (typeObj instanceof Map) {
            // Complex type
            Map<String, Object> typeMap = (Map<String, Object>) typeObj;
            String typeStr = (String) typeMap.get("type");
            
            if ("struct".equals(typeStr)) {
                fieldType = parseStructType(typeMap);
            } else if ("list".equals(typeStr)) {
                fieldType = parseListType(typeMap);
            } else if ("map".equals(typeStr)) {
                fieldType = parseMapType(typeMap);
            } else {
                throw new IllegalArgumentException("Unknown complex type: " + typeStr);
            }
        } else {
            throw new IllegalArgumentException("Invalid type format: " + typeObj);
        }
        
        // Create nested field
        if (required) {
            return Types.NestedField.required(id, name, fieldType);
        } else {
            return Types.NestedField.optional(id, name, fieldType);
        }
    }

    /**
     * Parse primitive type from string
     */
    /**
     * Parse an Iceberg primitive type string (e.g. "int", "long", "double",
     * "decimal(20,2)") for schema evolution. Rejects complex/non-primitive types.
     */
    public Type.PrimitiveType parseColumnType(String typeStr) {
        Type t = parsePrimitiveType(typeStr);
        if (!(t instanceof Type.PrimitiveType)) {
            throw new IllegalArgumentException(
                    "schema evolution supports primitive column types only, got: " + typeStr);
        }
        return (Type.PrimitiveType) t;
    }

    private Type parsePrimitiveType(String typeStr) {
        if ("boolean".equals(typeStr)) {
            return Types.BooleanType.get();
        } else if ("int".equals(typeStr)) {
            return Types.IntegerType.get();
        } else if ("long".equals(typeStr)) {
            return Types.LongType.get();
        } else if ("float".equals(typeStr)) {
            return Types.FloatType.get();
        } else if ("double".equals(typeStr)) {
            return Types.DoubleType.get();
        } else if ("date".equals(typeStr)) {
            return Types.DateType.get();
        } else if ("time".equals(typeStr)) {
            return Types.TimeType.get();
        } else if ("timestamp".equals(typeStr)) {
            return Types.TimestampType.withoutZone();
        } else if ("timestamptz".equals(typeStr)) {
            return Types.TimestampType.withZone();
        } else if ("string".equals(typeStr)) {
            return Types.StringType.get();
        } else if ("uuid".equals(typeStr)) {
            return Types.UUIDType.get();
        } else if ("binary".equals(typeStr)) {
            return Types.BinaryType.get();
        } else if (typeStr.startsWith("decimal")) {
            // Parse decimal precision and scale
            String[] parts = typeStr.substring(8, typeStr.length() - 1).split(",");
            int precision = Integer.parseInt(parts[0].trim());
            int scale = Integer.parseInt(parts[1].trim());
            return Types.DecimalType.of(precision, scale);
        } else if (typeStr.startsWith("fixed")) {
            // Parse fixed length
            int length = Integer.parseInt(typeStr.substring(6, typeStr.length() - 1));
            return Types.FixedType.ofLength(length);
        } else {
            throw new IllegalArgumentException("Unknown primitive type: " + typeStr);
        }
    }

    /**
     * Parse struct type from map
     */
    private StructType parseStructType(Map<String, Object> typeMap) {
        List<Map<String, Object>> fields = (List<Map<String, Object>>) typeMap.get("fields");
        List<NestedField> nestedFields = new ArrayList<>();
        
        for (Map<String, Object> field : fields) {
            NestedField nestedField = parseField(field);
            nestedFields.add(nestedField);
        }
        
        return Types.StructType.of(nestedFields);
    }

    /**
     * Parse list type from map
     */
    private Type parseListType(Map<String, Object> typeMap) {
        Integer elementId = (Integer) typeMap.get("element-id");
        Boolean elementRequired = (Boolean) typeMap.get("element-required");
        
        // Parse element type
        Object elementTypeObj = typeMap.get("element");
        Type elementType;
        
        if (elementTypeObj instanceof String) {
            elementType = parsePrimitiveType((String) elementTypeObj);
        } else if (elementTypeObj instanceof Map) {
            Map<String, Object> elementTypeMap = (Map<String, Object>) elementTypeObj;
            String elementTypeStr = (String) elementTypeMap.get("type");
            
            if ("struct".equals(elementTypeStr)) {
                elementType = parseStructType(elementTypeMap);
            } else if ("list".equals(elementTypeStr)) {
                elementType = parseListType(elementTypeMap);
            } else if ("map".equals(elementTypeStr)) {
                elementType = parseMapType(elementTypeMap);
            } else {
                throw new IllegalArgumentException("Unknown complex element type: " + elementTypeStr);
            }
        } else {
            throw new IllegalArgumentException("Invalid element type format: " + elementTypeObj);
        }
        
        if (elementRequired) {
            return Types.ListType.ofRequired(elementId, elementType);
        } else {
            return Types.ListType.ofOptional(elementId, elementType);
        }
    }

    /**
     * Parse map type from map
     */
    private Type parseMapType(Map<String, Object> typeMap) {
        Integer keyId = (Integer) typeMap.get("key-id");
        Integer valueId = (Integer) typeMap.get("value-id");
        Boolean valueRequired = (Boolean) typeMap.get("value-required");
        
        // Parse key type
        Object keyTypeObj = typeMap.get("key");
        Type keyType;
        
        if (keyTypeObj instanceof String) {
            keyType = parsePrimitiveType((String) keyTypeObj);
        } else {
            throw new IllegalArgumentException("Map keys must be primitive types");
        }
        
        // Parse value type
        Object valueTypeObj = typeMap.get("value");
        Type valueType;
        
        if (valueTypeObj instanceof String) {
            valueType = parsePrimitiveType((String) valueTypeObj);
        } else if (valueTypeObj instanceof Map) {
            Map<String, Object> valueTypeMap = (Map<String, Object>) valueTypeObj;
            String valueTypeStr = (String) valueTypeMap.get("type");
            
            if ("struct".equals(valueTypeStr)) {
                valueType = parseStructType(valueTypeMap);
            } else if ("list".equals(valueTypeStr)) {
                valueType = parseListType(valueTypeMap);
            } else if ("map".equals(valueTypeStr)) {
                valueType = parseMapType(valueTypeMap);
            } else {
                throw new IllegalArgumentException("Unknown complex value type: " + valueTypeStr);
            }
        } else {
            throw new IllegalArgumentException("Invalid value type format: " + valueTypeObj);
        }
        
        if (valueRequired) {
            return Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
        } else {
            return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
        }
    }

    /**
     * Convert NestedField to JSON map
     */
    private Map<String, Object> convertField(NestedField field) {
        Map<String, Object> result = new HashMap<>();
        
        result.put("id", field.fieldId());
        result.put("name", field.name());
        result.put("required", field.isRequired());
        
        if (field.doc() != null) {
            result.put("doc", field.doc());
        }
        
        result.put("type", convertType(field.type()));
        
        return result;
    }

    /**
     * Convert Type to JSON representation
     */
    private Object convertType(Type type) {
        if (type.isPrimitiveType()) {
            return convertPrimitiveType(type);
        } else if (type.isStructType()) {
            return convertStructType(type.asStructType());
        } else if (type.isListType()) {
            return convertListType(type.asListType());
        } else if (type.isMapType()) {
            return convertMapType(type.asMapType());
        } else {
            throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    /**
     * Convert primitive type to string
     */
    private String convertPrimitiveType(Type type) {
        if (type == Types.BooleanType.get()) {
            return "boolean";
        } else if (type == Types.IntegerType.get()) {
            return "int";
        } else if (type == Types.LongType.get()) {
            return "long";
        } else if (type == Types.FloatType.get()) {
            return "float";
        } else if (type == Types.DoubleType.get()) {
            return "double";
        } else if (type == Types.DateType.get()) {
            return "date";
        } else if (type == Types.TimeType.get()) {
            return "time";
        } else if (type instanceof Types.TimestampType) {
            Types.TimestampType timestampType = (Types.TimestampType) type;
            if (timestampType.shouldAdjustToUTC()) {
                return "timestamptz";
            } else {
                return "timestamp";
            }
        } else if (type == Types.StringType.get()) {
            return "string";
        } else if (type == Types.UUIDType.get()) {
            return "uuid";
        } else if (type == Types.BinaryType.get()) {
            return "binary";
        } else if (type instanceof Types.DecimalType) {
            Types.DecimalType decimalType = (Types.DecimalType) type;
            return String.format("decimal(%d,%d)", decimalType.precision(), decimalType.scale());
        } else if (type instanceof Types.FixedType) {
            Types.FixedType fixedType = (Types.FixedType) type;
            return String.format("fixed[%d]", fixedType.length());
        } else {
            throw new IllegalArgumentException("Unknown primitive type: " + type);
        }
    }

    /**
     * Convert struct type to JSON map
     */
    private Map<String, Object> convertStructType(Types.StructType structType) {
        Map<String, Object> result = new HashMap<>();
        
        result.put("type", "struct");
        
        List<Map<String, Object>> fields = new ArrayList<>();
        for (Types.NestedField field : structType.fields()) {
            fields.add(convertField(field));
        }
        
        result.put("fields", fields);
        
        return result;
    }

    /**
     * Convert list type to JSON map
     */
    private Map<String, Object> convertListType(Types.ListType listType) {
        Map<String, Object> result = new HashMap<>();
        
        result.put("type", "list");
        result.put("element-id", listType.elementId());
        result.put("element", convertType(listType.elementType()));
        result.put("element-required", listType.isElementRequired());
        
        return result;
    }

    /**
     * Convert map type to JSON map
     */
    private Map<String, Object> convertMapType(Types.MapType mapType) {
        Map<String, Object> result = new HashMap<>();
        
        result.put("type", "map");
        result.put("key-id", mapType.keyId());
        result.put("key", convertType(mapType.keyType()));
        result.put("value-id", mapType.valueId());
        result.put("value", convertType(mapType.valueType()));
        result.put("value-required", mapType.isValueRequired());
        
        return result;
    }
}
