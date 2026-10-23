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
 * Represents a collection of values
 */
public class CollectionOperandNode extends OperandNode {

    private final List<String> data;

    /**
     * Constructs a CollectionOperandNode with the given data type and a data
     * list
     *
     * @param dataType the data type
     * @param data     the data list
     */
    public CollectionOperandNode(DataType dataType, List<String> data) {
        super(dataType);
        this.data = data;
    }

    /**
     * Returns the collection of values
     *
     * @return the collection of values
     */
    public List<String> getData() {
        return data;
    }

    @Override
    public String toString() {
        return String.format("(%s)", String.join(",", data));
    }
}
