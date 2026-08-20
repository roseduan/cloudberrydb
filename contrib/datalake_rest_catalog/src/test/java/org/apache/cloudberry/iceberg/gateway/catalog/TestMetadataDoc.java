package org.apache.cloudberry.iceberg.gateway.catalog;

/**
 * The one metadata.json document the catalog-layer tests share. Kept in a single place so the
 * "database returns a complete document" fixture cannot drift between the store test and the
 * no-object-storage-IO proof.
 */
final class TestMetadataDoc {

    private TestMetadataDoc() {}

    static final String DOC =
            "{\"format-version\":2,\"table-uuid\":\"c8f0e0a2-0000-4000-8000-000000000001\","
            + "\"location\":\"s3://warehouse/db/t\",\"last-sequence-number\":0,"
            + "\"last-updated-ms\":1700000000000,\"last-column-id\":1,"
            + "\"current-schema-id\":0,\"schemas\":[{\"type\":\"struct\",\"schema-id\":0,"
            + "\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}],"
            + "\"default-spec-id\":0,\"partition-specs\":[{\"spec-id\":0,\"fields\":[]}],"
            + "\"last-partition-id\":999,\"default-sort-order-id\":0,"
            + "\"sort-orders\":[{\"order-id\":0,\"fields\":[]}],\"properties\":{},"
            + "\"current-snapshot-id\":-1,\"refs\":{},\"snapshots\":[],\"snapshot-log\":[],"
            + "\"metadata-log\":[]}";

    static final String LOC = "s3://warehouse/db/t/metadata/00001-abc.metadata.json";
}
