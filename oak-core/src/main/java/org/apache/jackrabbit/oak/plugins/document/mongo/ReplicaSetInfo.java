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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import static com.google.common.collect.Sets.difference;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.bson.BasicBSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;

public class ReplicaSetInfo implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaSetInfo.class);

    private final Map<String, DBCollection> collections = new HashMap<String, DBCollection>();

    private final DB adminDb;

    private final String dbName;

    private final long pullFrequencyMillis;

    private final String credentials;

    RevisionVector rootRevisions;

    private volatile boolean stop;

    private final Object stopMonitor = new Object();

    public ReplicaSetInfo(DB db, String credentials, long pullFrequencyMillis) {
        this.adminDb = db.getSisterDB("admin");
        this.dbName = db.getName();
        this.pullFrequencyMillis = pullFrequencyMillis;
        this.credentials = credentials;
    }

    boolean isSecondarySafe(RevisionVector lastSeenRev) {
        if (rootRevisions == null) {
            return false;
        } else {
            return lastSeenRev.compareTo(rootRevisions) <= 0;
        }
    }

    @Nullable
    public synchronized RevisionVector getMinimumRootRevisions() {
        return rootRevisions;
    }

    public void stop() {
        synchronized (stopMonitor) {
            stop = true;
            stopMonitor.notify();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        try {
            while (!stop) {
                CommandResult result = adminDb.command("replSetGetStatus", ReadPreference.primary());
                Iterable<BasicBSONObject> members = (Iterable<BasicBSONObject>) result.get("members");
                if (members == null) {
                    members = Collections.emptyList();
                }
                updateRevisions(members);
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
            closeConnections(collections.keySet());
            collections.clear();
        } catch (Exception e) {
            LOG.error("Exception in the ReplicaSetInfo thread", e);
        }
    }

    void updateRevisions(Iterable<BasicBSONObject> members) {
        Set<String> secondaries = new HashSet<String>();
        boolean unknownState = false;
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
            LOG.debug("No secondaries found");
            unknownState = true;
        }

        if (!unknownState) {
            RevisionVector minRevisions = getMinimumRootRevisions(secondaries);
            if (minRevisions == null) {
                unknownState = true;
            } else {
                Long minTimestamp = null;
                for (Revision r : minRevisions) {
                    long timestamp = r.getTimestamp();
                    if (minTimestamp == null || minTimestamp > timestamp) {
                        minTimestamp = timestamp;
                    }
                }
                synchronized (this) {
                    rootRevisions = minRevisions;
                }
                LOG.debug("Minimum revisions: {}", minRevisions);
                LOG.debug("Minimum root timestamp: {}", minTimestamp);
            }
        }
        if (unknownState) {
            synchronized (this) {
                rootRevisions = null;
            }
        }

        closeConnections(difference(collections.keySet(), secondaries));
    }

    private RevisionVector getMinimumRootRevisions(Set<String> secondaries) {
        RevisionVector minRevs = null;
        for (String name : secondaries) {
            try {
                RevisionVector revs = getRootRevisions(name);
                if (minRevs == null) {
                    minRevs = revs;
                } else {
                    minRevs = revs.pmin(minRevs);
                }
            } catch (UnknownHostException e) {
                LOG.error("Can't connect to {}", name, e);
                return null;
            }
        }
        return minRevs;
    }

    protected RevisionVector getRootRevisions(String hostName) throws UnknownHostException {
        List<Revision> revisions = new ArrayList<Revision>();
        DBCollection collection = getNodeCollection(hostName);
        DBObject root = collection.findOne(new BasicDBObject(Document.ID, "0:/"));
        DBObject lastRev = (DBObject) root.get("_lastRev");
        for (String clusterId : lastRev.keySet()) {
            String rev = (String) lastRev.get(clusterId);
            revisions.add(Revision.fromString(rev));
        }
        LOG.debug("Got /_lastRev from {}: {}", hostName, lastRev);
        return new RevisionVector(revisions);
    }

    private void closeConnections(Set<String> hostNames) {
        Iterator<Entry<String, DBCollection>> it = collections.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, DBCollection> entry = it.next();
            if (hostNames.contains(entry.getKey())) {
                try {
                    entry.getValue().getDB().getMongo().close();
                    it.remove();
                } catch (MongoClientException e) {
                    LOG.error("Can't close Mongo client", e);
                }
            }
        }
    }

    @SuppressWarnings("deprecation")
    private DBCollection getNodeCollection(String hostName) throws UnknownHostException {
        if (collections.containsKey(hostName)) {
            return collections.get(hostName);
        }

        StringBuilder uriBuilder = new StringBuilder("mongodb://");
        if (credentials != null) {
            uriBuilder.append(credentials).append('@');
        }
        uriBuilder.append(hostName);

        MongoClientURI uri = new MongoClientURI(uriBuilder.toString());
        MongoClient client = new MongoClient(uri);

        DB db = client.getDB(dbName);
        db.getMongo().slaveOk();
        DBCollection collection = db.getCollection(Collection.NODES.toString());
        collections.put(hostName, collection);
        return collection;
    }

    enum ReplicaSetMemberState {
        STARTUP, PRIMARY, SECONDARY, RECOVERING, STARTUP2, UNKNOWN, ARBITER, DOWN, ROLLBACK, REMOVED
    }

}