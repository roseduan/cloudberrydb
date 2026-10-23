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

/**
 * Supported operations by the parser.
 */
public enum Operator {
    NOOP(null, false) {
        @Override
        public String toString() {
            throw new UnsupportedOperationException("NOOP doesn't have an operator");
        }
    },
    LESS_THAN("<", false) {
        @Override
        public Operator transpose() {
            return GREATER_THAN;
        }
    },
    GREATER_THAN(">", false) {
        @Override
        public Operator transpose() {
            return LESS_THAN;
        }
    },
    LESS_THAN_OR_EQUAL("<=", false) {
        @Override
        public Operator transpose() {
            return GREATER_THAN_OR_EQUAL;
        }
    },
    GREATER_THAN_OR_EQUAL(">=", false) {
        @Override
        public Operator transpose() {
            return LESS_THAN_OR_EQUAL;
        }
    },
    EQUALS("=", false),
    NOT_EQUALS("<>", false),
    LIKE("LIKE", false),
    IS_NULL("IS NULL", false),
    IS_NOT_NULL("IS NOT NULL", false),
    IN("IN", false),
    AND("AND", true),
    OR("OR", true),
    NOT("NOT", true);

    private final String printableName;
    private final boolean isLogical;

    Operator(String printableName, boolean isLogical) {
        this.printableName = printableName;
        this.isLogical = isLogical;
    }

    public boolean isLogical() {
        return isLogical;
    }

    /**
     * Transposes operator
     * e.g. > turns into <, = turns into =
     *
     * @return the transposed operator
     */
    public Operator transpose() {
        return this;
    }

    @Override
    public String toString() {
        return printableName;
    }
}
