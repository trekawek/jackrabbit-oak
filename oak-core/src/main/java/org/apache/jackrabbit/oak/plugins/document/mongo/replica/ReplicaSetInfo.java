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
package org.apache.jackrabbit.oak.plugins.document.mongo.replica;

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Sets.union;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.stats.Clock;
import org.bson.BasicBSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;

/**
 * This class analyses the replica set info provided by MongoDB to find out two
 * what's the current synchronization state of secondary instances in terms of
 * revision values and timestamp.
 */
public class ReplicaSetInfo implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaSetInfo.class);

    private final DB adminDb;

    private final long pullFrequencyMillis;

    private final long maxReplicationLagMillis;

    private final ExecutorService executors = Executors.newFixedThreadPool(5);

    private final NodeCollectionProvider nodeCollections;

    private final Clock clock;

    private final Object stopMonitor = new Object();

    private final List<ReplicaSetInfoListener> listeners = new CopyOnWriteArrayList<ReplicaSetInfoListener>();

    private volatile RevisionVector rootRevisions;

    volatile long secondariesSafeTimestamp;

    private volatile boolean stop;

    public ReplicaSetInfo(Clock clock, DB db, String credentials, long pullFrequencyMillis, long maxReplicationLagMillis) {
        this.clock = clock;
        this.adminDb = db.getSisterDB("admin");
        this.pullFrequencyMillis = pullFrequencyMillis;
        this.maxReplicationLagMillis = maxReplicationLagMillis;
        this.nodeCollections = new NodeCollectionProvider(credentials, db.getName());
    }

    public void addListener(ReplicaSetInfoListener listener) {
        listeners.add(listener);
    }

    public boolean isMoreRecentThan(RevisionVector revisions) {
        RevisionVector localRootRevisions = rootRevisions;
        if (localRootRevisions == null) {
            return false;
        } else {
            return revisions.compareTo(localRootRevisions) <= 0;
        }
    }

    public long getLag() {
        long localTS = secondariesSafeTimestamp;
        if (localTS == 0) {
            return maxReplicationLagMillis;
        } else {
            return clock.getTime() - localTS;
        }
    }

    @Nullable
    public RevisionVector getMinimumRootRevisions() {
        return rootRevisions;
    }

    public void stop() {
        synchronized (stopMonitor) {
            stop = true;
            stopMonitor.notify();
            executors.shutdown();
        }
    }

    @Override
    public void run() {
        try {
            while (!stop) {
                updateReplicaStatus();

                for (ReplicaSetInfoListener listener : listeners) {
                    listener.gotRootRevisions(rootRevisions);
                }

                synchronized (stopMonitor) {
                    try {
                        if (!stop) {
                            stopMonitor.wait(pullFrequencyMillis);
                        }
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
            LOG.debug("Stopping the replica set info");
            nodeCollections.close();
        } catch (Exception e) {
            LOG.error("Exception in the ReplicaSetInfo thread", e);
        }
    }

    void updateReplicaStatus() {
        BasicDBObject result = null;
        try {
            result = getReplicaStatus();
        } catch (MongoException e) {
            LOG.error("Can't get replica status", e);
            rootRevisions = null;
            secondariesSafeTimestamp = 0;
            return;
        }

        @SuppressWarnings("unchecked")
        Iterable<BasicBSONObject> members = (Iterable<BasicBSONObject>) result.get("members");
        if (members == null) {
            members = Collections.emptyList();
        }
        updateRevisions(members);
    }

    protected BasicDBObject getReplicaStatus() {
        return adminDb.command("replSetGetStatus", ReadPreference.primary());
    }

    private void updateRevisions(Iterable<BasicBSONObject> members) {
        Set<String> secondaries = new HashSet<String>();
        boolean unknownState = false;
        String primary = null;

        for (BasicBSONObject member : members) {
            MemberState state;
            try {
                state = MemberState.valueOf(member.getString("stateStr"));
            } catch (IllegalArgumentException e) {
                state = MemberState.UNKNOWN;
            }
            String name = member.getString("name");

            switch (state) {
            case PRIMARY:
                primary = name;
                continue;

            case SECONDARY:
                secondaries.add(name);
                break;

            case ARBITER:
                continue;

            default:
                LOG.debug("Invalid state {} for instance {}", state, name);
                unknownState = true;
                break;
            }
        }

        if (secondaries.isEmpty()) {
            LOG.debug("No secondaries found: {}", members);
            unknownState = true;
        }

        if (primary == null) {
            LOG.debug("No primary found: {}", members);
            unknownState = true;
        }

        Map<String, Timestamped<RevisionVector>> vectors = null;
        if (!unknownState) {
            vectors = getRootRevisions(union(secondaries, of(primary)));
            if (vectors.containsValue(null)) {
                unknownState = true;
            }
        }

        if (unknownState) {
            rootRevisions = null;
            secondariesSafeTimestamp = 0;
        } else {
            Timestamped<RevisionVector> primaryRevision = vectors.get(primary);
            Iterable<Timestamped<RevisionVector>> secondaryRevisions = filterKeys(vectors, in(secondaries)).values();

            rootRevisions = pmin(transform(secondaryRevisions, Timestamped.<RevisionVector>getExtractFunction()));
            if (rootRevisions == null || primaryRevision == null || isEmpty(secondaryRevisions)) {
                secondariesSafeTimestamp = 0;
            } else {
                secondariesSafeTimestamp = getSecondariesSafeTimestamp(primaryRevision, secondaryRevisions);
            }
        }

        LOG.debug("Minimum root revisions: {}. Current lag: {}", rootRevisions, getLag());
        nodeCollections.retain(secondaries);
    }

    /**
     * Find the oldest revision which hasn't been replicated from primary to
     * secondary yet and return its timestamp. If all revisions has been already
     * replicated, return the date of the measurement.
     *
     * @return the point in time to which the secondary instances has been synchronized
     */
    private long getSecondariesSafeTimestamp(Timestamped<RevisionVector> primary, Iterable<Timestamped<RevisionVector>> secondaries) {
        final RevisionVector priRev = primary.getValue();
        Long oldestNotReplicated = null;
        for (Timestamped<RevisionVector> v : secondaries) {
            RevisionVector secRev = v.getValue();
            if (secRev.compareTo(priRev) == 0) {
                continue;
            }

            for (Revision pr : priRev) {
                Revision sr = secRev.getRevision(pr.getClusterId());
                if (pr.equals(sr)) {
                    continue;
                }
                if (oldestNotReplicated == null || oldestNotReplicated > pr.getTimestamp()) {
                    oldestNotReplicated = pr.getTimestamp();
                }
            }
        }

        if (oldestNotReplicated == null) {
            long minOpTimestamp = primary.getOperationTimestamp();
            for (Timestamped<RevisionVector> v : secondaries) {
                if (v.getOperationTimestamp() < minOpTimestamp) {
                    minOpTimestamp = v.getOperationTimestamp();
                }
            }
            return minOpTimestamp;
        } else {
            return oldestNotReplicated;
        }
    }

    protected Map<String, Timestamped<RevisionVector>> getRootRevisions(Iterable<String> hosts) {
        Map<String, Future<Timestamped<RevisionVector>>> futures = new HashMap<String, Future<Timestamped<RevisionVector>>>();
        for (final String hostName : hosts) {
            futures.put(hostName, executors.submit(new GetRootRevisionsCallable(clock, hostName, nodeCollections)));
        }

        Map<String, Timestamped<RevisionVector>> result = new HashMap<String, Timestamped<RevisionVector>>();
        for (Entry<String, Future<Timestamped<RevisionVector>>> entry : futures.entrySet()) {
            try {
                result.put(entry.getKey(), entry.getValue().get());
            } catch (Exception e) {
                LOG.error("Can't connect to the Mongo instance", e);
            }
        }
        return result;
    }

    private static RevisionVector pmin(Iterable<RevisionVector> vectors) {
        RevisionVector minimum = null;
        for (RevisionVector v : vectors) {
            if (v == null) {
                return null;
            } else if (minimum == null) {
                minimum = v;
            } else {
                minimum = minimum.pmin(v);
            }
        }
        return minimum;
    }

    enum MemberState {
        STARTUP, PRIMARY, SECONDARY, RECOVERING, STARTUP2, UNKNOWN, ARBITER, DOWN, ROLLBACK, REMOVED
    }
}
