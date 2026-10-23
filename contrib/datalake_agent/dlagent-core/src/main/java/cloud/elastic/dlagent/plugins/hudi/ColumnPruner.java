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

package cloud.elastic.dlagent.plugins.hudi;

import cloud.elastic.dlagent.api.filter.ColumnIndexOperandNode;
import cloud.elastic.dlagent.api.filter.Node;
import cloud.elastic.dlagent.api.filter.Operator;
import cloud.elastic.dlagent.api.filter.OperatorNode;
import cloud.elastic.dlagent.api.filter.SupportedOperatorPruner;
import cloud.elastic.dlagent.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Prune the tree based on partition keys
 */
public class ColumnPruner extends SupportedOperatorPruner {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnPruner.class);

    private final Set<String> columns;
    private final List<ColumnDescriptor> columnDescriptors;
    private final boolean isIncludeMode;

    public ColumnPruner(EnumSet<Operator> supportedOperators,
                        Set<String> columns,
                        List<ColumnDescriptor> columnDescriptors,
                        boolean isIncludeMode) {
        super(supportedOperators);
        this.columns = columns;
        this.columnDescriptors = columnDescriptors;
        this.isIncludeMode = isIncludeMode;
    }

    @Override
    public Node visit(Node node, final int level) {
        if (node instanceof OperatorNode &&
                !shouldKeep((OperatorNode) node)) {
            return null;
        }
        return super.visit(node, level);
    }

    /**
     * Returns true when the operatorNode is logical, or for simple operators
     * true when the column is a partitioned column
     *
     * @param operatorNode the operatorNode node
     * @return true when the filter is compatible, false otherwise
     */
    private boolean shouldKeep(OperatorNode operatorNode) {
        Operator operator = operatorNode.getOperator();

        if (operator.isLogical()) {
            // Skip AND / OR
            return true;
        }

        ColumnIndexOperandNode columnIndexOperand = operatorNode.getColumnIndexOperand();
        ColumnDescriptor columnDescriptor = columnDescriptors.get(columnIndexOperand.index());
        String columnName = columnDescriptor.columnName();

        boolean found = columns.contains(columnName);
        if (isIncludeMode) {
            return found;
        }

        return !found;
    }
}
