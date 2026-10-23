/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.service.iceberg;

import lombok.extern.slf4j.Slf4j;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts between Iceberg's object model and the plain maps the datalake_fdw
 * wire format uses: an Iceberg {@link Table} into the metadata map that is the
 * body of a loadTable response, and a request's schema / partition-spec maps
 * back into Iceberg {@link Schema} and {@link PartitionSpec}.
 *
 * <p>This is the code whose output <em>is</em> a response body, so it is the
 * one where a silent change would show up as different bytes on the wire; the
 * bodies are kept identical to what the REST controller always ran: the REST
 * path's bytes may not change, and both transports must emit the same bytes.
 *
 * <p>See the note in {@code MetadataFileCleaner} on why both {@code log} and
 * {@code LOG} exist here.
 */
@Named
@Singleton
@Slf4j
public class IcebergMetadataMapper {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataMapper.class);

    private final SchemaConverter schemaConverter;

    @Inject
    public IcebergMetadataMapper(SchemaConverter schemaConverter) {
        this.schemaConverter = schemaConverter;
    }

    public Map<String, Object> convertTableToMetadata(Table table) {
        Map<String, Object> metadata = new HashMap<>();

        // Required fields
        metadata.put("format-version", 2); // Default to format version 2

        // Basic table information
        metadata.put("location", table.location());
        metadata.put("last-updated-ms", table.currentSnapshot() != null ?
                table.currentSnapshot().timestampMillis() : System.currentTimeMillis());

        // Schema tracking
        Schema currentSchema = table.schema();
        metadata.put("current-schema-id", currentSchema.schemaId());
        metadata.put("last-column-id", currentSchema.highestFieldId());

        // Convert schemas to list format
        List<Map<String, Object>> schemas = new ArrayList<>();
        for (Schema schema : table.schemas().values()) {
            Map<String, Object> schemaMap = new HashMap<>();
            schemaMap.put("schema-id", schema.schemaId());
            schemaMap.put("fields", convertSchemaFields(schema.columns()));
            schemas.add(schemaMap);
        }
        metadata.put("schemas", schemas);

        // Partition spec tracking
        PartitionSpec currentSpec = table.spec();
        metadata.put("default-spec-id", currentSpec.specId());

        List<Map<String, Object>> partitionSpecs = new ArrayList<>();
        for (PartitionSpec spec : table.specs().values()) {
            Map<String, Object> specMap = new HashMap<>();
            specMap.put("spec-id", spec.specId());
            specMap.put("fields", convertPartitionFields(spec.fields()));
            partitionSpecs.add(specMap);
        }
        metadata.put("partition-specs", partitionSpecs);

        // Sort order tracking
        SortOrder currentSortOrder = table.sortOrder();
        metadata.put("default-sort-order-id", currentSortOrder.orderId());

        List<Map<String, Object>> sortOrders = new ArrayList<>();
        for (SortOrder sortOrder : table.sortOrders().values()) {
            Map<String, Object> sortOrderMap = new HashMap<>();
            sortOrderMap.put("order-id", sortOrder.orderId());
            sortOrderMap.put("fields", convertSortFields(sortOrder.fields()));
            sortOrders.add(sortOrderMap);
        }
        metadata.put("sort-orders", sortOrders);

        // Snapshot tracking
        if (table.currentSnapshot() != null) {
            metadata.put("current-snapshot-id", table.currentSnapshot().snapshotId());
        }

        List<Map<String, Object>> snapshots = new ArrayList<>();
        for (Snapshot snapshot : table.snapshots()) {
            Map<String, Object> snapshotMap = new HashMap<>();
            snapshotMap.put("snapshot-id", snapshot.snapshotId());
            snapshotMap.put("timestamp-ms", snapshot.timestampMillis());
            snapshotMap.put("summary", snapshot.summary());
            snapshotMap.put("manifest-list", snapshot.manifestListLocation());
            snapshotMap.put("schema-id", snapshot.schemaId());
            snapshots.add(snapshotMap);
        }
        metadata.put("snapshots", snapshots);

        // Snapshot references
        Map<String, Object> refs = new HashMap<>();
        for (Map.Entry<String, SnapshotRef> entry : table.refs().entrySet()) {
            SnapshotRef ref = entry.getValue();
            Map<String, Object> refMap = new HashMap<>();
            refMap.put("snapshot-id", ref.snapshotId());
            refMap.put("type", ref.isBranch() ? "branch" : "tag");
            refs.put(entry.getKey(), refMap);
        }
        metadata.put("refs", refs);

        // Properties — expose only user-facing TBLPROPERTIES. Legacy tables
        // may have internal runtime config leaked into table.properties(); this
        // filter prevents re-exposure of e.g. secret_access_key or node-local
        // gopher paths over HTTP.
        metadata.put("properties",
                IcebergUtilities.stripInternalProperties(
                        new HashMap<>(table.properties())));

        return metadata;
    }
    private List<Map<String, Object>> convertSchemaFields(List<Types.NestedField> fields) {
        List<Map<String, Object>> fieldList = new ArrayList<>();
        for (Types.NestedField field : fields) {
            Map<String, Object> fieldMap = new HashMap<>();
            fieldMap.put("id", field.fieldId());
            fieldMap.put("name", field.name());
            fieldMap.put("required", field.isRequired());
            fieldMap.put("type", field.type().toString());
            if (field.doc() != null) {
                fieldMap.put("doc", field.doc());
            }
            fieldList.add(fieldMap);
        }
        return fieldList;
    }
    private List<Map<String, Object>> convertPartitionFields(List<PartitionField> fields) {
        List<Map<String, Object>> fieldList = new ArrayList<>();
        for (PartitionField field : fields) {
            Map<String, Object> fieldMap = new HashMap<>();
            fieldMap.put("source-id", field.sourceId());
            fieldMap.put("field-id", field.fieldId());
            fieldMap.put("name", field.name());
            fieldMap.put("transform", field.transform().toString());
            fieldList.add(fieldMap);
        }
        return fieldList;
    }
    private List<Map<String, Object>> convertSortFields(List<SortField> fields) {
        List<Map<String, Object>> fieldList = new ArrayList<>();
        for (SortField field : fields) {
            Map<String, Object> fieldMap = new HashMap<>();
            fieldMap.put("transform", field.transform().toString());
            fieldMap.put("source-id", field.sourceId());
            fieldMap.put("direction", field.direction().toString());
            fieldMap.put("null-order", field.nullOrder().toString());
            fieldList.add(fieldMap);
        }
        return fieldList;
    }
    public Schema convertMapToSchema(Map<String, Object> schemaMap) {
        return schemaConverter.fromJson(schemaMap);
    }
    public PartitionSpec buildPartitionSpec(Schema schema, Map<String, Object> partitionSpecMap) {
        if (partitionSpecMap == null) {
            return null;
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fields = (List<Map<String, Object>>) partitionSpecMap.get("fields");
        if (fields == null || fields.isEmpty()) {
            return null;
        }

        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (Map<String, Object> field : fields) {
            Object transformObj = field.get("transform");
            if (transformObj == null) {
                throw new IllegalArgumentException(
                    "Partition field " + field + " is missing the required \"transform\" key");
            }
            if (!(transformObj instanceof String)) {
                throw new IllegalArgumentException(String.format(
                    "Partition field %s has a non-string \"transform\" value", field));
            }
            String transform = (String) transformObj;
            if (!"identity".equals(transform)) {
                throw new IllegalArgumentException(String.format(
                    "Unsupported partition transform \"%s\": only identity is supported", transform));
            }

            // Resolve the source column through source-id (assigned in schema
            // field order by the client); fall back to the field name.
            String columnName = null;
            Object sourceId = field.get("source-id");
            if (sourceId instanceof Number) {
                columnName = schema.findColumnName(((Number) sourceId).intValue());
            }
            if (columnName == null) {
                /* Guard the cast like the sibling "source-id" branch: a
                 * non-string "name" leaves columnName null and falls into the
                 * clean IllegalArgumentException below, rather than throwing a
                 * ClassCastException (a confusing 500 instead of a 400). */
                Object nameObj = field.get("name");
                columnName = (nameObj instanceof String) ? (String) nameObj : null;
            }
            if (columnName == null || schema.findField(columnName) == null) {
                throw new IllegalArgumentException(String.format(
                    "Partition field %s does not resolve to a schema column", field));
            }

            builder.identity(columnName);
        }
        return builder.build();
    }
    public String partitionSpecSummary(Table table) {
        StringBuilder summary = new StringBuilder();
        for (PartitionField field : table.spec().fields()) {
            if (summary.length() > 0) {
                summary.append(',');
            }
            String columnName = table.schema().findColumnName(field.sourceId());
            if (columnName == null) {
                columnName = field.name();
            }
            String transform = field.transform().toString();
            if ("identity".equals(transform)) {
                summary.append(columnName);
            } else {
                summary.append(transform).append('(').append(columnName).append(')');
            }
        }
        return summary.toString();
    }
    public String generateETag(Table table, String operation) {
        try {
            StringBuilder etagBuilder = new StringBuilder();

            // Add timestamp for debugging and version tracking
            long timestamp = table.currentSnapshot() != null ? 
                table.currentSnapshot().timestampMillis() : System.currentTimeMillis();
            etagBuilder.append(timestamp);

            // Add schema ID for structure version identification
            etagBuilder.append("-").append(table.schema().schemaId());

            // Add operation type for source tracing
            etagBuilder.append("-").append(operation);

            // Add hash of metadata for uniqueness guarantee
            int metadataHash = convertTableToMetadata(table).hashCode();
            etagBuilder.append("-").append(Math.abs(metadataHash));

            String etag = etagBuilder.toString();
            log.debug("Generated ETag for {} operation: {} (format: timestamp-schemaId-operation-metadataHash)", 
                     operation, etag);

            return etag;
        } catch (Exception e) {
            log.warn("Failed to generate detailed ETag, using fallback timestamp: {}", e.getMessage());
            return String.valueOf(System.currentTimeMillis());
        }
    }
}
