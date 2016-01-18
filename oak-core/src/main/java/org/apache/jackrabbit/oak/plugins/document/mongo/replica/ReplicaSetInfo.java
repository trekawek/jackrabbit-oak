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
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Sets.union;
import static org.apache.jackrabbit.oak.plugins.document.mongo.replica.TimestampedRevisionVector.EXTRACT;

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
import org.bson.BasicBSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.ReadPreference;

public class ReplicaSetInfo implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaSetInfo.class);

    private final DB adminDb;

    private final long pullFrequencyMillis;

    private final long maxReplicationLagMillis;

    private final ExecutorService executors = Executors.newFixedThreadPool(5);

    private final NodeCollectionProvider nodeCollections;

    private volatile RevisionVector rootRevisions;

    private volatile long oldestNotReplicated;

    private volatile boolean stop;

    private long timeDiff;

    private final Object stopMonitor = new Object();

    private final List<ReplicaSetInfoListener> listeners = new CopyOnWriteArrayList<ReplicaSetInfoListener>();

    public ReplicaSetInfo(DB db, String credentials, long pullFrequencyMillis, long maxReplicationLagMillis) {
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
        if (oldestNotReplicated == 0) {
            return maxReplicationLagMillis;
        } else {
            return System.currentTimeMillis() - oldestNotReplicated;
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

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        try {
            while (!stop) {
                long start = System.currentTimeMillis();
                CommandResult result = adminDb.command("replSetGetStatus", ReadPreference.primary());
                long end = System.currentTimeMillis();
                long midPoint = (start + end) / 2;
                timeDiff = midPoint - result.getDate("date").getTime();

                Iterable<BasicBSONObject> members = (Iterable<BasicBSONObject>) result.get("members");
                if (members == null) {
                    members = Collections.emptyList();
                }
                updateRevisions(members);

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

    void updateRevisions(Iterable<BasicBSONObject> members) {
        Set<String> secondaries = new HashSet<String>();
        boolean unknownState = false;
        String primary = null;

        for (BasicBSONObject member : members) {
            ReplicaSetMemberState state;
            try {
                state = ReplicaSetMemberState.valueOf(member.getString("stateStr"));
            } catch (IllegalArgumentException e) {
                state = ReplicaSetMemberState.UNKNOWN;
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

        if (unknownState) {
            rootRevisions = null;
            oldestNotReplicated = 0;
        } else {
            Map<String, TimestampedRevisionVector> vectors = getRootRevisions(union(secondaries, of(primary)));

            TimestampedRevisionVector primaryRevision = vectors.get(primary);
            Iterable<TimestampedRevisionVector> secondaryRevisions = filterKeys(vectors, in(secondaries)).values();

            rootRevisions = getMinimum(transform(secondaryRevisions, EXTRACT));
            oldestNotReplicated = getOldestNotReplicated(primaryRevision, secondaryRevisions);
        }

        LOG.debug("Minimum root revisions: {}", rootRevisions);
        nodeCollections.retain(secondaries);
    }

    private long getOldestNotReplicated(TimestampedRevisionVector primary, Iterable<TimestampedRevisionVector> secondaries) {
        final RevisionVector priRev = primary.getRevs();

        Long oldestNotReplicated = null;
        for (TimestampedRevisionVector v : secondaries) {
            RevisionVector secRev = v.getRevs();
            if (secRev.compareTo(priRev) == 0) {
                continue;
            }

            for (Revision pr : priRev) {
                Revision sr = secRev.getRevision(pr.getClusterId());
                if (pr.equals(sr)) {
                    continue;
                }
                long prTimestampInLocalTime = pr.getTimestamp() + timeDiff;
                if (oldestNotReplicated == null || oldestNotReplicated > prTimestampInLocalTime) {
                    oldestNotReplicated = prTimestampInLocalTime;
                }
            }
        }

        if (oldestNotReplicated == null) {
            long minOpTimestamp = primary.getOperationTimestamp();
            for (TimestampedRevisionVector v : secondaries) {
                if (v.getOperationTimestamp() < minOpTimestamp) {
                    minOpTimestamp = v.getOperationTimestamp();
                }
            }
            return minOpTimestamp;
        } else {
            return oldestNotReplicated;
        }
    }

    protected Map<String, TimestampedRevisionVector> getRootRevisions(Iterable<String> hosts) {
        Map<String, Future<TimestampedRevisionVector>> futures = new HashMap<String, Future<TimestampedRevisionVector>>();
        for (final String hostName : hosts) {
            futures.put(hostName, executors.submit(new GetRootRevisionsCallable(hostName, nodeCollections)));
        }

        Map<String, TimestampedRevisionVector> result = new HashMap<String, TimestampedRevisionVector>();
        for (Entry<String, Future<TimestampedRevisionVector>> entry : futures.entrySet()) {
            try {
                result.put(entry.getKey(), entry.getValue().get());
            } catch (Exception e) {
                LOG.error("Can't connect to the Mongo instance", e);
            }
        }
        return result;
    }

    private static RevisionVector getMinimum(Iterable<RevisionVector> vectors) {
        RevisionVector minimum = null;
        for (RevisionVector v : vectors) {
            if (v == null) {
                return null;
            } else if (minimum == null || minimum.compareTo(v) > 0) {
                minimum = v;
            }
        }
        return minimum;
    }
}
