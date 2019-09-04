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
package org.apache.jackrabbit.oak.remote.server;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.jackrabbit.oak.remote.proto.LeaseProtos.ClusterView;
import org.apache.jackrabbit.oak.remote.proto.LeaseProtos.LeaseInfo;
import org.apache.jackrabbit.oak.remote.proto.LeaseServiceGrpc;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class LeaseService extends LeaseServiceGrpc.LeaseServiceImplBase {

    private final List<ClusterEntry> clusterEntries = new ArrayList<>();

    private final String clusterId;

    private int seq = 1;

    public LeaseService(NodeStore nodeStore) {
        clusterId = ClusterRepositoryInfo.getOrCreateId(nodeStore);
    }

    public synchronized void acquire(Empty request, StreamObserver<LeaseInfo> responseObserver) {
        String token = UUID.randomUUID().toString();
        ClusterEntry entry = new ClusterEntry(token);
        updateEntries();
        boolean set = false;
        for (int i = 0; i < clusterEntries.size(); i++) {
            if (clusterEntries.get(i).getState() == ClusterEntryState.INACTIVE) {
                clusterEntries.set(i, entry);
                set = true;
            }
        }
        if (!set) {
            clusterEntries.add(entry);
        }
        seq++;
        responseObserver.onNext(LeaseInfo.newBuilder().setToken(token).build());
        responseObserver.onCompleted();
    }

    public synchronized void renew(LeaseInfo request, StreamObserver<ClusterView> responseObserver) {
        ClusterEntry entry = getEntryByToken(request.getToken());
        if (entry == null || entry.state == ClusterEntryState.INACTIVE) {
            responseObserver.onNext(ClusterView.getDefaultInstance());
            responseObserver.onCompleted();
        } else {
            entry.lastUpdate = Instant.now();
            updateEntries();

            ClusterView.Builder builder = ClusterView.newBuilder();

            for (int i = 0; i < clusterEntries.size(); i++) {
                ClusterEntry e = clusterEntries.get(i);
                if (e == entry) {
                    builder.setMe(i);
                }
                switch (e.state) {
                    case ACTIVE:
                        builder.addActive(i);
                        break;

                    case DEACTIVATING:
                        builder.addDeactivating(i);
                        break;

                    case INACTIVE:
                        builder.addInactive(i);
                        break;
                }
            }
            builder.setId(clusterId);
            builder.setSeq(seq);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }
    }

    public synchronized void release(LeaseInfo request, StreamObserver<Empty> responseObserver) {
        ClusterEntry entry = getEntryByToken(request.getToken());
        if (entry != null) {
            entry.lastUpdate = null;
            entry.state = ClusterEntryState.INACTIVE;
            seq++;
        }
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    private ClusterEntry getEntryByToken(String token) {
        for (ClusterEntry e : clusterEntries) {
            if (token.equals(e.token)) {
                return e;
            }
        }
        return null;
    }

    private void updateEntries() {
        for (ClusterEntry entry : clusterEntries) {
            Instant lastUpdate = entry.lastUpdate;
            if (lastUpdate == null) {
                continue;
            }
            Instant inactiveDeadline = Instant.now().minus(60, ChronoUnit.SECONDS);
            Instant deactivatingDeadline = Instant.now().minus(10, ChronoUnit.SECONDS);
            if (lastUpdate.isBefore(inactiveDeadline)) {
                updateState(entry, ClusterEntryState.INACTIVE);
            } else if (lastUpdate.isBefore(deactivatingDeadline)) {
                updateState(entry, ClusterEntryState.DEACTIVATING);
            } else {
                updateState(entry, ClusterEntryState.ACTIVE);
            }
        }
    }

    private void updateState(ClusterEntry entry, ClusterEntryState newState) {
        if (entry.state != newState) {
            entry.state = newState;
            seq++;
        }
    }

    private enum ClusterEntryState {
        ACTIVE, DEACTIVATING, INACTIVE
    }

    private static class ClusterEntry {

        private final String token;

        private Instant lastUpdate;

        private ClusterEntryState state;

        public ClusterEntry(String token) {
            this.token = token;
            this.lastUpdate = Instant.now();
            this.state = ClusterEntryState.ACTIVE;
        }

        public ClusterEntryState getState() {
            return state;
        }
    }

}
