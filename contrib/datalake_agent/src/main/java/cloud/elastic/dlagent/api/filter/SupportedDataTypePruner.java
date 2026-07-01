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

package cloud.elastic.dlagent.api.filter;

import cloud.elastic.dlagent.api.io.DataType;
import cloud.elastic.dlagent.api.utilities.ColumnDescriptor;

import java.util.EnumSet;
import java.util.List;

/**
 * A tree pruner that removes operator nodes for non-supported Greenplum column data types.
 */
public class SupportedDataTypePruner extends BaseTreePruner {

    private final List<ColumnDescriptor> columnDescriptors;
    private final EnumSet<DataType> supportedDataTypes;

    /**
     * Constructor
     *
     * @param columnDescriptors  the list of column descriptors for the table
     * @param supportedDataTypes the EnumSet of supported data types
     */
    public SupportedDataTypePruner(List<ColumnDescriptor> columnDescriptors,
                                   EnumSet<DataType> supportedDataTypes) {
        this.columnDescriptors = columnDescriptors;
        this.supportedDataTypes = supportedDataTypes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node visit(Node node, int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            if (!operatorNode.getOperator().isLogical()) {
                int colIdx = operatorNode.getColumnIndexOperand().index();
                ColumnDescriptor columnDescriptor =
                        (colIdx >= 0 && colIdx < columnDescriptors.size())
                                ? columnDescriptors.get(colIdx) : null;
                if (columnDescriptor == null) {
                    // Unresolvable column reference (null placeholder for a dropped
                    // attribute, or an out-of-range index): we cannot prove the
                    // predicate excludes the block, so prune the operator (keep).
                    LOG.debug("column index {} is unresolvable; skipping pushdown for this operator", colIdx);
                    return null;
                }
                DataType datatype = columnDescriptor.getDataType();
                if (!supportedDataTypes.contains(datatype)) {
                    // prune the operator node if its operand is a column of unsupported type
                    LOG.debug("DataType oid={} for column=(index:{} name:{}) is not supported",
                            datatype.getOID(), columnDescriptor.columnIndex(), columnDescriptor.columnName());
                    return null;
                }
            }
        }
        return node;
    }
}
