/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.remote.client;

import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.remote.proto.LeaseProtos;
import org.jetbrains.annotations.NotNull;

import javax.jcr.Value;
import java.util.Iterator;

/**
 * This provides the 'clusterView' repository descriptors
 **/
class RemoteNodeStoreDiscoveryLiteDescriptors implements Descriptors {

    private static final String OAK_DISCOVERYLITE_CLUSTERVIEW = "oak.discoverylite.clusterview";

    private final SimpleValueFactory factory = new SimpleValueFactory();

    private final RemoteNodeStore store;

    RemoteNodeStoreDiscoveryLiteDescriptors(RemoteNodeStore store) {
        this.store = store;
    }

    @NotNull
    @Override
    public String[] getKeys() {
        return new String[] {OAK_DISCOVERYLITE_CLUSTERVIEW};
    }

    @Override
    public boolean isStandardDescriptor(@NotNull String key) {
        return OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key);
    }

    @Override
    public boolean isSingleValueDescriptor(@NotNull String key) {
        return OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key);
    }

    @Override
    public Value getValue(@NotNull String key) {
        if (!OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key)) {
            return null;
        }
        return factory.createValue(getClusterViewAsDescriptorValue());
    }

    @Override
    public Value[] getValues(@NotNull String key) {
        if (!OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key)) {
            return null;
        }
        return new Value[] {getValue(key)};
    }

    private String getClusterViewAsDescriptorValue() {
        LeaseProtos.ClusterView view = store.getLastClusterView();
        return asJson(view.getSeq(), true, view.getId(), view.getMe(), view.getActiveList(), view.getDeactivatingList(), view.getInactiveList());
    }

    private static String asJson(final long viewSeqNum, final boolean viewFinal, final String clusterId, final int localId,
                          final Iterable<Integer> activeIds, final Iterable<Integer> deactivatingIds, final Iterable<Integer> inactiveIds) {
        JsopBuilder builder = new JsopBuilder();
        builder.object();
        builder.key("seq").value(viewSeqNum);
        builder.key("final").value(viewFinal);
        builder.key("id").value(clusterId);
        builder.key("me").value(localId);
        builder.key("active").array();
        for (Iterator<Integer> it = activeIds.iterator(); it.hasNext();) {
            Integer anInstance = it.next();
            builder.value(anInstance);
        }
        builder.endArray();
        builder.key("deactivating").array();
        for (Iterator<Integer> it = deactivatingIds.iterator(); it.hasNext();) {
            Integer anInstance = it.next();
            builder.value(anInstance);
        }
        builder.endArray();
        builder.key("inactive").array();
        for (Iterator<Integer> it = inactiveIds.iterator(); it.hasNext();) {
            Integer anInstance = it.next();
            builder.value(anInstance);
        }
        builder.endArray();
        builder.endObject();
        return builder.toString();
    }


}
