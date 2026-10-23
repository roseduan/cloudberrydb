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

package cloud.elastic.dlagent.plugins.iceberg;

import cloud.elastic.dlagent.api.error.UnsupportedTypeException;
import cloud.elastic.dlagent.api.filter.CollectionOperandNode;
import cloud.elastic.dlagent.api.filter.ColumnIndexOperandNode;
import cloud.elastic.dlagent.api.filter.Node;
import cloud.elastic.dlagent.api.filter.OperandNode;
import cloud.elastic.dlagent.api.filter.Operator;
import cloud.elastic.dlagent.api.filter.OperatorNode;
import cloud.elastic.dlagent.api.filter.ScalarOperandNode;
import cloud.elastic.dlagent.api.filter.TreeVisitor;
import cloud.elastic.dlagent.api.io.DataType;
import cloud.elastic.dlagent.api.utilities.ColumnDescriptor;
import cloud.elastic.dlagent.api.utilities.Utilities;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class IcebergExpressionBuilder implements TreeVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergExpressionBuilder.class);
    private final List<ColumnDescriptor> columnDescriptors;
    private final Deque<Expression> filterQueue;

    public IcebergExpressionBuilder(List<ColumnDescriptor> tupleDescription) {
        this.columnDescriptors = tupleDescription;
        this.filterQueue = new LinkedList<>();
    }

    @Override
    public Node before(Node node, final int level) { return node; }

    @Override
    public Node visit(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (!operator.isLogical()) {
                processSimpleColumnOperator(operatorNode);
            }
        }
        return node;
    }

    @Override
    public Node after(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (operator.isLogical()) {
                processLogicalOperator(operator);
            }
        }
        return node;
    }

    /**
     * Builds a single argument
     *
     * @param operatorNode the operatorNode node
     * @return true if the argument is build, false otherwise
     */
    private void processSimpleColumnOperator(OperatorNode operatorNode) {
        Operator operator = operatorNode.getOperator();
        ColumnIndexOperandNode columnIndexOperand = operatorNode.getColumnIndexOperand();
        OperandNode valueOperandNode = operatorNode.getValueOperand();

        ColumnDescriptor filterColumn = columnDescriptors.get(columnIndexOperand.index());
        String filterColumnName = filterColumn.columnName();
        Object filterValue = null;

        if (valueOperandNode instanceof CollectionOperandNode) {
            CollectionOperandNode collectionOperand = (CollectionOperandNode) valueOperandNode;

            filterValue = collectionOperand
                    .getData()
                    .stream()
                    .map(data -> Utilities.boxLiteral(Utilities.convertDataValue(
                            filterColumn.getDataType(),
                            data)))
                    .collect(Collectors.toList());
        } else if (valueOperandNode != null) {
            ScalarOperandNode scalarOperand = (ScalarOperandNode) valueOperandNode;
            filterValue = Utilities.boxLiteral(Utilities.convertDataValue(
                    filterColumn.getDataType(),
                    scalarOperand.getValue()));
        }
        // IS_NULL / IS_NOT_NULL carry no value operand; filterValue stays null
        // and the operator switch builds isNull()/notNull() without it.

        if (operator == Operator.NOOP) {
            // NOT boolean wraps a NOOP
            //       NOT
            //        |
            //       NOOP
            //        |
            //    ---------
            //   |         |
            //   4        true
            // that needs to be replaced with equals

            // also IN
            operator = Operator.EQUALS;
        }

        Expression expression;
        switch (operator) {
            case LESS_THAN:
                expression = Expressions.lessThan(filterColumnName, filterValue);
                break;
            case GREATER_THAN:
                expression = Expressions.greaterThan(filterColumnName, filterValue);
                break;
            case LESS_THAN_OR_EQUAL:
                expression = Expressions.lessThanOrEqual(filterColumnName, filterValue);
                break;
            case GREATER_THAN_OR_EQUAL:
                expression = Expressions.greaterThanOrEqual(filterColumnName, filterValue);
                break;
            case EQUALS:
                expression = Expressions.equal(filterColumnName, filterValue);
                break;
            case NOT_EQUALS:
                expression = Expressions.notEqual(filterColumnName, filterValue);
                break;
            case IS_NULL:
                expression = Expressions.isNull(filterColumnName);
                break;
            case IS_NOT_NULL:
                expression = Expressions.notNull(filterColumnName);
                break;
            case IN:
                if (filterValue instanceof Collection) {
                    @SuppressWarnings("unchecked")
                    Collection<Object> l = (Collection<Object>) filterValue;
                    expression = Expressions.in(filterColumnName, l);
                } else {
                    throw new IllegalArgumentException("filterValue should be instance of List for IN operation");
                }
                break;
            default:
                throw new UnsupportedOperationException(String.format("Filter push-down is not supported for %s operation", operator));
        }

        filterQueue.push(expression);
    }

    private void processLogicalOperator(Operator operator) {
        Expression right = filterQueue.poll();
        Expression left = null;

        if (right == null) {
            throw new IllegalStateException("Unable to process logical operator " + operator.toString());
        }

        if (operator == Operator.AND || operator == Operator.OR) {
            left = filterQueue.poll();

            if (left == null) {
                throw new IllegalStateException("Unable to process logical operator " + operator.toString());
            }
        }

        switch (operator) {
            case AND:
                filterQueue.push(Expressions.and(left, right));
                break;
            case OR:
                filterQueue.push(Expressions.or(left, right));
                break;
            case NOT:
                filterQueue.push(Expressions.not(right));
                break;
        }
    }

    public Expression build() {
        if (filterQueue.isEmpty()) {
            return null;
        }

        Expression expression = filterQueue.poll();
        if (!filterQueue.isEmpty()) {
            throw new IllegalStateException("Filter queue is not empty after visiting all nodes");
        }

        return expression;
    }
}
