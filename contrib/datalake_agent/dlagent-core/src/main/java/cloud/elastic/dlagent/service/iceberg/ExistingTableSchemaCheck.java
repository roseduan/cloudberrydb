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


package cloud.elastic.dlagent.service.iceberg;

import cloud.elastic.dlagent.api.model.RequestedColumn;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Whether a local definition may be bound to a table the catalog already holds
 * (cloud#610, i.e. hashdata-cloud issue 610; the position rule below is
 * hashdata-lightning issue #461).
 *
 * Two requests bind one: a CREATE of a name the catalog already holds adopts
 * the existing table (cloud#609: by design), and OPTIONS (table '...')
 * mounts it.  Either way the local definition then decides how the existing
 * data files are read: the reader finds each local column by name among the
 * leaves of the parquet schema and picks its scanner from the column's
 * PostgreSQL type, not from the file (parquet_reader.cpp createMapping /
 * resolveReadFn).  A definition that does not describe the data is decoded
 * with the wrong scanner: garbage, or a segment SIGSEGV in the parquet
 * dictionary decoder.
 * Refuse the binding unless every local column
 *
 *   - names exactly one top-level column of the table, compared the way the
 *     reader compares names (ASCII case-insensitive);
 *   - is that column for the reader too: no nested field of the same name
 *     comes before it in parquet order, which the reader would bind instead;
 *   - has a PostgreSQL type the reader can decode the column's type as
 *     (see {@link #readable});
 *   - is the table's column at the same position, and that column's
 *     field-id is its position: the reader and the writer both take a
 *     column's attribute number as its field-id, so the local definition
 *     must declare every column of the table, in the table's order, and the
 *     table's ids must be 1..n.  A table another engine has dropped a column
 *     from, or declaring only some columns or another order, is refused --
 *     the same as Spark, which binds a table only by its own schema.
 *
 * Another nullability does not affect reads and is allowed.
 *
 * Violations raise IllegalArgumentException, which maps to a 400 whose message
 * becomes the CREATE's ERROR (rest.IcebergExceptionHandler); every problem is
 * listed rather than just the first.
 */
public final class ExistingTableSchemaCheck {

    private ExistingTableSchemaCheck() {
    }

    public static void check(TableIdentifier tableId, List<RequestedColumn> requested, Schema existing) {
        List<String> errors = new ArrayList<>();
        List<Types.NestedField> columns = existing.columns();
        // Top-level columns by case-folded name; a list, because the table may
        // hold names that differ only in case.
        Map<String, List<Types.NestedField>> byName = new HashMap<>();
        for (Types.NestedField f : columns) {
            byName.computeIfAbsent(fold(f.name()), k -> new ArrayList<>()).add(f);
        }
        // The reader does not look at top-level columns but at every leaf of
        // the parquet schema, depth first, and takes the first whose own name
        // matches.  Index each folded name to the first leaf carrying it.
        Map<String, Types.NestedField> firstLeaf = new HashMap<>();
        Map<Integer, String> leafPath = new HashMap<>();
        for (Types.NestedField f : columns) {
            indexLeaves(f, f.name(), firstLeaf, leafPath);
        }

        if (requested.size() != columns.size()) {
            errors.add(String.format("the local table declares %d columns, the existing table has %d",
                    requested.size(), columns.size()));
        }
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).fieldId() != i + 1) {
                errors.add(String.format("existing column \"%s\" has field-id %d at position %d; "
                        + "only a table whose field-ids are its column positions can be bound",
                        columns.get(i).name(), columns.get(i).fieldId(), i + 1));
            }
        }

        int position = 0;
        for (RequestedColumn col : requested) {
            int at = position++;
            List<Types.NestedField> matches = byName.getOrDefault(
                    fold(col.getName()), Collections.emptyList());
            if (matches.isEmpty()) {
                errors.add(String.format("column \"%s\": not in the existing table", col.getName()));
                continue;
            }
            if (matches.size() > 1) {
                errors.add(String.format("column \"%s\": matches %d columns of the existing table "
                        + "that differ only in case", col.getName(), matches.size()));
                continue;
            }
            Types.NestedField match = matches.get(0);
            if (at >= columns.size() || columns.get(at).fieldId() != match.fieldId()) {
                errors.add(String.format("column \"%s\": is column %d of the existing table, "
                        + "declared at position %d", col.getName(), columns.indexOf(match) + 1, at + 1));
                continue;
            }
            Types.NestedField read = firstLeaf.get(fold(col.getName()));
            if (read != null && read.fieldId() != match.fieldId()) {
                errors.add(String.format("column \"%s\": the reader would read nested field \"%s\" "
                        + "instead, which comes first under that name", col.getName(),
                        leafPath.get(read.fieldId())));
                continue;
            }
            boolean typeOk = col.getPgType() != null
                    ? readable(col.getPgType(), match.type())
                    : col.getIcebergType() != null && col.getIcebergType().equals(match.type());
            if (!typeOk) {
                String localType = col.getPgType() != null ? col.getPgType()
                        : col.getIcebergType() != null ? col.getIcebergType().toString() : "(unknown)";
                errors.add(String.format("column \"%s\": local type %s cannot read existing table type %s",
                        col.getName(), localType, match.type()));
            }
        }

        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "Table %s already exists in the catalog with a different schema, so it cannot be "
                    + "used by this table: %s. Existing schema: %s. Declare the columns to match "
                    + "the existing schema.",
                    tableId, String.join("; ", errors), existing.asStruct()));
        }
    }

    /*
     * Case-fold a column name the way the reader compares names:
     * pg_strcasecmp folds ASCII letters only, so "Ä" and "ä" stay distinct.
     */
    private static String fold(String name) {
        StringBuilder sb = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            sb.append(c >= 'A' && c <= 'Z' ? (char) (c + ('a' - 'A')) : c);
        }
        return sb.toString();
    }

    /*
     * Record the leaves below `field` in parquet schema order (depth first;
     * a list's element, a map's key then value), keeping only the first leaf
     * per folded name -- the one the reader would bind.
     */
    private static void indexLeaves(Types.NestedField field, String path,
                                    Map<String, Types.NestedField> firstLeaf,
                                    Map<Integer, String> leafPath) {
        if (field.type().isPrimitiveType()) {
            if (firstLeaf.putIfAbsent(fold(field.name()), field) == null) {
                leafPath.put(field.fieldId(), path);
            }
            return;
        }
        for (Types.NestedField child : field.type().asNestedType().fields()) {
            indexLeaves(child, path + "." + child.name(), firstLeaf, leafPath);
        }
    }

    /**
     * Whether the reader decodes Iceberg type {@code type} correctly as the
     * PostgreSQL type the kernel tagged {@code pgType}.  Mirrors the scanner
     * the reader picks per PostgreSQL type (parquet_reader.cpp resolveReadFn /
     * resolveFillKind / readPrimitive):
     *
     *   int2 is accepted over int because that is what the kernel itself
     *   writes for a smallint column; values outside int2 written by another
     *   engine are truncated, which is a range problem, not a decoding one.
     *
     *   numeric accepts any decimal: the reader takes the scale from the file.
     *
     *   "other" -- json, arrays and every type the kernel has no Iceberg
     *   mapping for -- is written as string by the kernel, and the reader
     *   rejects such a column with an error rather than decoding it.
     */
    static boolean readable(String pgType, Type type) {
        switch (pgType) {
            case "bool":
                return type.typeId() == Type.TypeID.BOOLEAN;
            case "int2":
            case "int4":
                return type.typeId() == Type.TypeID.INTEGER;
            case "int8":
                return type.typeId() == Type.TypeID.LONG;
            case "float4":
                return type.typeId() == Type.TypeID.FLOAT;
            case "float8":
                return type.typeId() == Type.TypeID.DOUBLE;
            case "numeric":
                return type.typeId() == Type.TypeID.DECIMAL;
            case "text":
                return type.typeId() == Type.TypeID.STRING;
            case "bytea":
                // Not FIXED: parquet stores fixed(n) as FIXED_LEN_BYTE_ARRAY, and the reader
                // casts the bytea scanner to TypedScanner<ByteArrayType> (parquet_reader.cpp),
                // so a fixed(n) column would be decoded through the wrong scanner.
                return type.typeId() == Type.TypeID.BINARY;
            case "date":
                return type.typeId() == Type.TypeID.DATE;
            case "time":
                return type.typeId() == Type.TypeID.TIME;
            case "timestamp":
                return type.typeId() == Type.TypeID.TIMESTAMP
                        && !((Types.TimestampType) type).shouldAdjustToUTC();
            case "timestamptz":
                return type.typeId() == Type.TypeID.TIMESTAMP
                        && ((Types.TimestampType) type).shouldAdjustToUTC();
            case "uuid":
                return type.typeId() == Type.TypeID.UUID;
            default:
                return type.typeId() == Type.TypeID.STRING;
        }
    }
}
