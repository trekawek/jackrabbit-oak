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

package org.apache.jackrabbit.oak.kv.osgi;

import javax.jcr.Value;

import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

class KVDiscoveryLiteDescriptors implements Descriptors {

    private static final String CLUSTER_VIEW = "oak.discoverylite.clusterview";

    private final SimpleValueFactory factory = new SimpleValueFactory();

    private final NodeStore nodeStore;

    KVDiscoveryLiteDescriptors(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    @Override
    public String[] getKeys() {
        return new String[] {CLUSTER_VIEW};
    }

    @Override
    public boolean isStandardDescriptor(String key) {
        return CLUSTER_VIEW.equals(key);
    }

    @Override
    public boolean isSingleValueDescriptor(String key) {
        return CLUSTER_VIEW.equals(key);
    }

    @Override
    public Value getValue(String key) {
        if (CLUSTER_VIEW.equals(key)) {
            return factory.createValue(getClusterView(nodeStore));
        }
        return null;
    }

    @Override
    public Value[] getValues(String key) {
        if (CLUSTER_VIEW.equals(key)) {
            return new Value[] {getValue(key)};
        }
        return null;
    }

    private static String getClusterView(NodeStore nodeStore) {
        return getClusterView(ClusterRepositoryInfo.getOrCreateId(nodeStore));
    }

    private static String getClusterView(String clusterId) {
        return "{\"seq\":1,\"final\":true,\"me\":1,\"id\":\"" + clusterId + "\",\"active\":[1],\"deactivating\":[],\"inactive\":[]}";
    }

}
