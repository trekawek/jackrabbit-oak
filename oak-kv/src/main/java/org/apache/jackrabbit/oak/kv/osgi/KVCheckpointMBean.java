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

import java.io.IOException;

import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularDataSupport;

import org.apache.jackrabbit.oak.commons.jmx.AbstractCheckpointMBean;
import org.apache.jackrabbit.oak.kv.KVCheckpoint;
import org.apache.jackrabbit.oak.kv.KVNodeStore;

class KVCheckpointMBean extends AbstractCheckpointMBean {

    private final KVNodeStore store;

    KVCheckpointMBean(KVNodeStore store) {
        this.store = store;
    }

    @Override
    protected void collectCheckpoints(TabularDataSupport tab) throws OpenDataException {
        Iterable<KVCheckpoint> checkpoints;

        try {
            checkpoints = store.getCheckpoints();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (KVCheckpoint checkpoint : checkpoints) {
            tab.put(toCompositeData(
                checkpoint.getCheckpoint(),
                Long.toString(checkpoint.getCreated()),
                Long.toString(checkpoint.getTimestamp()),
                checkpoint.getInfo()
            ));
        }
    }

    @Override
    public long getOldestCheckpointCreationTimestamp() {
        Iterable<KVCheckpoint> checkpoints;

        try {
            checkpoints = store.getCheckpoints();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVCheckpoint oldest = null;

        for (KVCheckpoint checkpoint : checkpoints) {
            if (oldest == null || checkpoint.getCreated() < oldest.getCreated()) {
                oldest = checkpoint;
            }
        }

        if (oldest == null) {
            return 0;
        }

        return oldest.getCreated();
    }

    @Override
    public String createCheckpoint(long lifetime) {
        return store.checkpoint(lifetime);
    }

    @Override
    public boolean releaseCheckpoint(String checkpoint) {
        return store.release(checkpoint);
    }

}
