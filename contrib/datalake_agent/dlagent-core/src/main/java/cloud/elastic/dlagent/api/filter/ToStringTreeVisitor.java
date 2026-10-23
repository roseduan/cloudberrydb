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

import org.apache.commons.lang3.StringUtils;
import cloud.elastic.dlagent.api.io.DataType;

import static cloud.elastic.dlagent.api.filter.Operator.IS_NOT_NULL;
import static cloud.elastic.dlagent.api.filter.Operator.IS_NULL;
import static cloud.elastic.dlagent.api.filter.Operator.NOOP;
import static cloud.elastic.dlagent.api.filter.Operator.NOT;

/**
 * A tree visitor that produces a string representation of the input Node
 */
public class ToStringTreeVisitor implements TreeVisitor {

    private final StringBuilder sb = new StringBuilder();

    @Override
    public Node before(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();

            if (operator == Operator.NOT) {
                sb.append(getOperatorName(operatorNode)).append(" ");
            }

            if (operator.isLogical()) {
                sb.append("(");
            }
        }
        return node;
    }

    @Override
    public Node visit(Node node, final int level) {
        if (node instanceof OperandNode) {

            if (node instanceof ScalarOperandNode) {
                ScalarOperandNode scalarOperand = (ScalarOperandNode) node;
                // boolean does not need to be rendered when it's true
                if (scalarOperand.getDataType() == DataType.BOOLEAN) {
                    if (StringUtils.equals("true", scalarOperand.getValue())) {
                        return node;
                    } else {
                        // when boolean is not true
                        sb.append(" = ");
                    }
                }
            }

            sb.append(getNodeValue((OperandNode) node));
        } else if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();

            // Skip NOOP and NOT is already handled in the before method
            if (operator == NOOP || operator == NOT) {
                return node;
            }

            sb.append(" ").append(getOperatorName(operatorNode));
            if (operator != IS_NULL && operator != IS_NOT_NULL) {
                sb.append(" ");
            }
        }
        return node;
    }

    @Override
    public Node after(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            if (operatorNode.getOperator().isLogical()) {
                sb.append(")");
            }
        }
        return node;
    }

    /**
     * Returns the generated string
     *
     * @return the generated string
     */
    @Override
    public String toString() {
        return sb.toString();
    }

    /**
     * For testing purposes only
     */
    public void reset() {
        sb.setLength(0);
    }

    /**
     * Returns the {@link StringBuilder} for this TreeVisitor
     *
     * @return the {@link StringBuilder} for this TreeVisitor
     */
    protected StringBuilder getStringBuilder() {
        return sb;
    }

    /**
     * Returns the string representation of the value of the operandNode
     *
     * @param operandNode the operandNode
     * @return the string representation of the operandNode's value
     */
    protected String getNodeValue(OperandNode operandNode) {
        return operandNode.toString();
    }

    /**
     * Returns the string representation of the operator
     *
     * @param operatorNode the operator node
     * @return the string representation of the operator
     */
    private String getOperatorName(OperatorNode operatorNode) {
        return operatorNode.getOperator().toString();
    }
}
