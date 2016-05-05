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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.annotation.Nonnull;
import java.util.List;

public class HybridIndexPlan implements IndexPlan {

    private final IndexPlan asyncLucenePlan;

    private final IndexPlan memoryLucenePlan;

    private Filter filter;

    public HybridIndexPlan(@Nonnull IndexPlan asyncLucenePlan, @Nonnull IndexPlan memoryLucenePlan) {
        this.asyncLucenePlan = asyncLucenePlan;
        this.memoryLucenePlan = memoryLucenePlan;
        this.filter = asyncLucenePlan.getFilter();
    }

    @Override
    public double getCostPerExecution() {
        return asyncLucenePlan.getCostPerExecution();
    }

    @Override
    public double getCostPerEntry() {
        return asyncLucenePlan.getCostPerEntry();
    }

    @Override
    public long getEstimatedEntryCount() {
        return asyncLucenePlan.getEstimatedEntryCount() + memoryLucenePlan.getEstimatedEntryCount();
    }

    @Override
    public Filter getFilter() {
        return filter;
    }

    @Override
    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    @Override
    public boolean isDelayed() {
        return false;
    }

    @Override
    public boolean isFulltextIndex() {
        return asyncLucenePlan.isFulltextIndex();
    }

    @Override
    public boolean includesNodeData() {
        return asyncLucenePlan.includesNodeData() && memoryLucenePlan.includesNodeData();
    }

    @Override
    public List<QueryIndex.OrderEntry> getSortOrder() {
        return asyncLucenePlan.getSortOrder();
    }

    @Override
    public NodeState getDefinition() {
        return asyncLucenePlan.getDefinition();
    }

    @Override
    public String getPathPrefix() {
        return asyncLucenePlan.getPathPrefix();
    }

    @Override
    public Filter.PropertyRestriction getPropertyRestriction() {
        return asyncLucenePlan.getPropertyRestriction();
    }

    @Override
    public IndexPlan copy() {
        try {
            return (IndexPlan) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Object getAttribute(String name) {
        return asyncLucenePlan.getAttribute(name);
    }

    @Override
    public String getPlanName() {
        return asyncLucenePlan.getPlanName() + "," + memoryLucenePlan.getPlanName();
    }

    IndexPlan getAsyncLucenePlan() {
        return asyncLucenePlan;
    }

    IndexPlan getMemoryLucenePlan() {
        return memoryLucenePlan;
    }
}
