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
package org.apache.jackrabbit.oak.api;

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A {@code ConflictHandler} is responsible for handling conflicts which happen
 * on {@link Root#rebase(ConflictHandler)} and on the implicit rebase operation which
 * takes part on {@link Root#commit(ConflictHandler)}.
 *
 * This interface contains one method per type of conflict which might occur.
 * Each of these methods must return a {@link Resolution} for the current conflict.
 * The resolution indicates to use the changes in the current {@code Root} instance
 * ({@link Resolution#OURS}) or to use the changes from the underlying persistence
 * store ({@link Resolution#THEIRS}). Alternatively the resolution can also indicate
 * that the changes have been successfully merged by this {@code ConflictHandler}
 * instance ({@link Resolution#MERGED}).
 */
public interface ConflictHandler {

    /**
     * Resolutions for conflicts
     */
    enum Resolution {
        /**
         * Use the changes from the current {@link Root} instance
         */
        OURS,

        /**
         * Use the changes from the underlying persistence store
         */
        THEIRS,

        /**
         * Indicated changes have been merged by this {@code ConflictHandler} instance.
         */
        MERGED}

    /**
     * The property {@code ours} has been added to {@code parent} which conflicts
     * with property {@code theirs} which has been added in the persistence store.
     *
     * @param parent  root of the conflict
     * @param ours  our version of the property
     * @param theirs  their version of the property
     * @return  {@link Resolution} of the conflict
     */
    Resolution addExistingProperty(Tree parent, PropertyState ours, PropertyState theirs);

    /**
     * The property {@code ours} has been changed in {@code parent} while it was
     * removed in the persistence store.
     *
     * @param parent  root of the conflict
     * @param ours  our version of the property
     * @return  {@link Resolution} of the conflict
     */
    Resolution changeDeletedProperty(Tree parent, PropertyState ours);

    /**
     * The property {@code ours} has been changed in {@code parent} while it was
     * also changed to a different value ({@code theirs}) in the persistence store.
     *
     * @param parent  root of the conflict
     * @param ours  our version of the property
     * @param theirs  their version of the property
     * @return  {@link Resolution} of the conflict
     */
    Resolution changeChangedProperty(Tree parent, PropertyState ours, PropertyState theirs);

    /**
     * The property {@code theirs} changed in the persistence store while it has been
     * deleted locally.
     *
     * @param parent  root of the conflict
     * @param theirs  their version of the property
     * @return  {@link Resolution} of the conflict
     */
    Resolution deleteChangedProperty(Tree parent, PropertyState theirs);

    /**
     * The node {@code ours} has been added to {@code parent} which conflicts
     * with node {@code theirs} which has been added in the persistence store.
     *
     * @param parent  root of the conflict
     * @param name  name of the node
     * @param ours  our version of the node
     * @param theirs  their version of the node
     * @return  {@link Resolution} of the conflict
     */
    Resolution addExistingNode(Tree parent, String name, NodeState ours, NodeState theirs);

    /**
     * The node {@code ours} has been changed in {@code parent} while it was
     * removed in the persistence store.
     *
     * @param parent  root of the conflict
     * @param name  name of the node
     * @param ours  our version of the node
     * @return  {@link Resolution} of the conflict
     */
    Resolution changeDeletedNode(Tree parent, String name, NodeState ours);

    /**
     * The node {@code theirs} changed in the persistence store while it has been
     * deleted locally.
     *
     * @param parent  root of the conflict
     * @param name  name of the node
     * @param theirs
     * @param theirs  their version of the node
     * @return  {@link Resolution} of the conflict
     */
    Resolution deleteChangedNode(Tree parent, String name, NodeState theirs);
}
