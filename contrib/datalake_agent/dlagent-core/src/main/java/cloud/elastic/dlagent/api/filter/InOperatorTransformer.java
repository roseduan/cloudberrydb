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

import java.util.List;

/**
 * Transforms IN operator into a chain of OR operators. This transformer is
 * useful for predicate builders that do not support the IN operator.
 */
public class InOperatorTransformer implements TreeVisitor {

    /**
     * {@inheritDoc}
     */
    @Override
    public Node before(Node node, int level) {
        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node visit(Node node, int level) {

        if (node instanceof OperatorNode) {

            OperatorNode operatorNode = (OperatorNode) node;

            if (operatorNode.getOperator() == Operator.IN
                    && operatorNode.getLeft() instanceof ColumnIndexOperandNode
                    && operatorNode.getRight() instanceof CollectionOperandNode) {

                ColumnIndexOperandNode columnNode = (ColumnIndexOperandNode) operatorNode.getLeft();
                CollectionOperandNode collectionOperandNode = (CollectionOperandNode) operatorNode.getRight();
                List<String> data = collectionOperandNode.getData();
                DataType type = collectionOperandNode.getDataType().getTypeElem() != null
                        ? collectionOperandNode.getDataType().getTypeElem()
                        : collectionOperandNode.getDataType();

                // Transform the IN operator into a chain of ORs
                //       IN
                //        |
                //    --------
                //    |      |
                //   _1_   11,12
                //
                //  The transformed branch will look like this:

                //                   OR
                //                    |
                //         ------------------------
                //         |                      |
                //         eq                     eq
                //         |                      |
                //     ---------              ---------
                //     |       |              |       |
                //    _1_      11            _1_      12

                // build the first node as the equal operator of the column and the scalar operand
                Node currentNode = new OperatorNode(Operator.EQUALS, columnNode, new ScalarOperandNode(type, data.get(0)));

                for (int i = 1; i < data.size(); i++) {
                    // current node becomes left node
                    // scalar becomes right node
                    // the or operator becomes the current node
                    Node rightNode = new OperatorNode(Operator.EQUALS, columnNode, new ScalarOperandNode(type, data.get(i)));
                    currentNode = new OperatorNode(Operator.OR, currentNode, rightNode);
                }

                return currentNode;
            }
        }

        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node after(Node node, int level) {
        return node;
    }
}
