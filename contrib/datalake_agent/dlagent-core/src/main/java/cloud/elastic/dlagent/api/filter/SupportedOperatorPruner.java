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

import java.util.EnumSet;

/**
 * A tree pruner that removes operator nodes for non-supported operators.
 */
public class SupportedOperatorPruner extends BaseTreePruner {

    private final EnumSet<Operator> supportedOperators;

    /**
     * Constructor
     *
     * @param supportedOperators the set of supported operators
     */
    public SupportedOperatorPruner(EnumSet<Operator> supportedOperators) {
        this.supportedOperators = supportedOperators;
    }

    @Override
    public Node visit(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (!supportedOperators.contains(operator)) {
                // prune the operator node if its operator is not supported
                LOG.debug("Operator {} is not supported", operator);
                return null;
            }
        }
        return node;
    }
}
