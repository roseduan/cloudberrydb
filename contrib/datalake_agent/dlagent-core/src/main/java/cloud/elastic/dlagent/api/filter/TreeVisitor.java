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
 * Tree visitor interface
 */
public interface TreeVisitor {

    /**
     * Called right before a Node is visited
     *
     * @param node  the Node that will be visited next
     * @param level the level in the recursion
     * @return the resulting node from the visit
     */
    Node before(Node node, final int level);

    /**
     * Called during the visit of a Node
     *
     * @param node  the Node being visited
     * @param level the level in the recursion
     * @return the resulting node from the visit
     */
    Node visit(Node node, final int level);

    /**
     * Called right after the Node has been visited
     *
     * @param node  the Node that completed the visit
     * @param level the level in the recursion
     * @return the resulting node from the visit
     */
    Node after(Node node, final int level);

}
