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


package cloud.elastic.dlagent.plugins.iceberg.utilities;

import cloud.elastic.dlagent.api.error.UnsupportedTypeException;
import cloud.elastic.dlagent.api.io.DataType;
import cloud.elastic.dlagent.api.utilities.ColumnDescriptor;
import cloud.elastic.dlagent.api.utilities.EnumGpdbType;
import org.apache.iceberg.types.Type;

import com.google.common.collect.ImmutableMap;

import org.apache.iceberg.types.Types;

/**
 * Iceberg types, which are supported by plugin, mapped to GPDB's types
 *
 * @see EnumGpdbType
 */
public class GpdbToIcebergType {
    private static final ImmutableMap<DataType, Type> gpdbToIcebergTypeMap = ImmutableMap.<DataType, Type>builder()
            .put(DataType.SMALLINT, Types.IntegerType.get())
            .put(DataType.INTEGER, Types.IntegerType.get())
            .put(DataType.BIGINT, Types.LongType.get())
            .put(DataType.REAL, Types.FloatType.get())
            .put(DataType.FLOAT8, Types.DoubleType.get())
            .put(DataType.TEXT, Types.StringType.get())
            .put(DataType.BYTEA, Types.BinaryType.get())
            .put(DataType.DATE, Types.DateType.get())
            .put(DataType.TIMESTAMP, Types.TimestampType.withoutZone())
            .put(DataType.TIMESTAMP_WITH_TIME_ZONE, Types.TimestampType.withZone())
            .put(DataType.BOOLEAN, Types.BooleanType.get())
            .build();

    /**
     * Returns Iceberg to GPDB type mapping entry for given Iceberg type
     *
     * @param icebergType full Iceberg type with modifiers, for example - decimal(10, 0), binary, array&lt;string&gt;, map&lt;string,float&gt; etc
     * @return corresponding Iceberg to GPDB type mapping entry
     * @throws UnsupportedTypeException if there is no corresponding GPDB type
     */
    public static Type getGpdbToIcebergType(ColumnDescriptor cd) {
        DataType gpdbType = cd.getDataType();
        Integer[] modifiers = cd.columnTypeModifiers();
        if (gpdbToIcebergTypeMap.containsKey(gpdbType)) {
            return gpdbToIcebergTypeMap.get(gpdbType);
        }
        if (gpdbType == DataType.NUMERIC) {
            return Types.DecimalType.of(modifiers[0], modifiers[1]);
        }
        if (gpdbType == DataType.BPCHAR) {
            return Types.FixedType.ofLength(modifiers[0]);
        }
        throw new UnsupportedTypeException("Unable to map Gpdb's type: "
                + cd.columnName() + " to GPDB's type");
    }
}