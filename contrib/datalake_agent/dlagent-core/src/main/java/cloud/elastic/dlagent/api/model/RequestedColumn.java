/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package cloud.elastic.dlagent.api.model;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * One column of the local (PostgreSQL) definition that a request is about to
 * bind to an existing Iceberg table, either by a CREATE that adopts it or by a
 * mount, OPTIONS (table '...').  See service.iceberg.ExistingTableSchemaCheck
 * (cloud#610).
 *
 * {@code pgType} is the kernel's tag for the column's PostgreSQL type ("int4",
 * "time", "other", ...).  It decides compatibility, because the PostgreSQL
 * type is what the reader decodes the data files as; the Iceberg type the
 * kernel maps it to loses information (time, uuid and json all map to
 * string).  A kernel too old to send the tag leaves it null, and the check
 * then falls back to comparing {@code icebergType}.
 */
public final class RequestedColumn {

    /** Field key the kernel sends; see pgTypeTag() in iceberg_catalog_fdw.c. */
    public static final String KEY_PG_TYPE = "pg-type";

    private final String name;
    private final String pgType;
    private final Type icebergType;

    public RequestedColumn(String name, String pgType, Type icebergType) {
        this.name = name;
        this.pgType = pgType;
        this.icebergType = icebergType;
    }

    public String getName() {
        return name;
    }

    public String getPgType() {
        return pgType;
    }

    public Type getIcebergType() {
        return icebergType;
    }

    /**
     * Read the columns from a request's "schema" object.  {@code converted} is
     * that object already converted to an Iceberg schema, when the request is a
     * create-table (whose fields carry an Iceberg "type"); the mount's
     * load-table request sends only name and pg-type, and passes null.
     *
     * A malformed object is a 400 (IllegalArgumentException), like every other
     * refused binding -- not a ClassCastException or NPE and a 500, and not a
     * silently column-less definition.
     */
    @SuppressWarnings("unchecked")
    public static List<RequestedColumn> fromSchemaMap(Map<String, Object> schemaMap, Schema converted) {
        Object fieldsObj = schemaMap.get("fields");
        if (!(fieldsObj instanceof List)) {
            throw new IllegalArgumentException("schema has no \"fields\" list");
        }
        List<?> fields = (List<?>) fieldsObj;
        List<RequestedColumn> columns = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            if (!(fields.get(i) instanceof Map)) {
                throw new IllegalArgumentException("schema field " + i + " is not an object");
            }
            Map<String, Object> field = (Map<String, Object>) fields.get(i);
            Object name = field.get("name");
            Object pgType = field.get(KEY_PG_TYPE);
            if (!(name instanceof String)) {
                throw new IllegalArgumentException("schema field " + i + " has no name");
            }
            if (pgType != null && !(pgType instanceof String)) {
                throw new IllegalArgumentException("schema field " + i + " has a non-string " + KEY_PG_TYPE);
            }
            // By name, not position: the converted schema's field order is not a contract.
            // Top-level only and exact -- findField() would resolve "a.b" to a nested field.
            Types.NestedField convertedField = converted != null ? converted.asStruct().field((String) name) : null;
            Type icebergType = convertedField != null ? convertedField.type() : null;
            columns.add(new RequestedColumn((String) name, (String) pgType, icebergType));
        }
        return columns;
    }
}
