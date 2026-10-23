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
 * A node in the expression tree
 */
public class Node {

    private Node left;
    private Node right;

    /**
     * Default constructor
     */
    public Node() {
        this(null, null);
    }

    /**
     * Constructs a node with a left Node
     *
     * @param left the left node
     */
    public Node(Node left) {
        this(left, null);
    }

    /**
     * Constructs a node with a left and right node
     *
     * @param left  the left node
     * @param right the right node
     */
    public Node(Node left, Node right) {
        this.left = left;
        this.right = right;
    }

    /**
     * Sets the left {@link Node} of the tree
     *
     * @param left the left node
     */
    public void setLeft(Node left) {
        this.left = left;
    }

    /**
     * Returns the left {@link Node}
     *
     * @return the left {@link Node}
     */
    public Node getLeft() {
        return left;
    }

    /**
     * Sets the right {@link Node} of the tree
     *
     * @param right the right node
     */
    public void setRight(Node right) {
        this.right = right;
    }

    /**
     * Returns the right {@link Node}
     *
     * @return the right {@link Node}
     */
    public Node getRight() {
        return right;
    }

    /**
     * Returns the number of children for this node
     *
     * @return the number of children for this node
     */
    public int childCount() {
        int count = 0;
        if (left != null) count++;
        if (right != null) count++;
        return count;
    }
}
