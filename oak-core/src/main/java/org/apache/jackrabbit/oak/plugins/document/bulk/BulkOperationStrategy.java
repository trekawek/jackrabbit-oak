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
package org.apache.jackrabbit.oak.plugins.document.bulk;

import com.google.common.base.Predicate;

/**
 * BulkOperationStrategy allows to find out whether the update for a given
 * document can be included in the bulk operation or should be applied
 * separately. The implementing class can be stateful and may use the
 * information about updated which failed or succeeded in the past.
 */
public interface BulkOperationStrategy extends Predicate<String> {

    /**
     * Check if the update for the given id can be applied in a bulk operation.
     * 
     * @param id Document id
     * @return {@code true} if the modification can be included in the bulk update
     */
    boolean apply(String id);

    /**
     * Inform about the conflicting update.
     * @param id Document id
     */
    void updateConflicted(String id);

    /**
     * Inform about the succeeded update.
     * @param id Document id
     */
    void updateApplied(String id);

}
