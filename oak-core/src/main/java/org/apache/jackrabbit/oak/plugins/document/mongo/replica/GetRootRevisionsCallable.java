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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class GetRootRevisionsCallable implements Callable<TimestampedRevisionVector> {

    private static final Logger LOG = LoggerFactory.getLogger(GetRootRevisionsCallable.class);

    private final String hostName;

    private final NodeCollectionProvider nodeCollections;

    public GetRootRevisionsCallable(String hostName, NodeCollectionProvider nodeCollections) {
        this.hostName = hostName;
        this.nodeCollections = nodeCollections;
    }

    @Override
    public TimestampedRevisionVector call() throws Exception {
        List<Revision> revisions = new ArrayList<Revision>();
        DBCollection collection = nodeCollections.get(hostName);

        long start = System.currentTimeMillis();
        DBObject root = collection.findOne(new BasicDBObject(Document.ID, "0:/"));
        long duration = System.currentTimeMillis() - start;
        long mid = start + duration / 2;

        if (root == null) {
            LOG.warn("Can't get the root document on {}", hostName);
            return null;
        }
        DBObject lastRev = (DBObject) root.get("_lastRev");
        for (String clusterId : lastRev.keySet()) {
            String rev = (String) lastRev.get(clusterId);
            revisions.add(Revision.fromString(rev));
        }
        LOG.debug("Got /_lastRev from {}: {}", hostName, lastRev);
        return new TimestampedRevisionVector(new RevisionVector(revisions), mid);
    }
}