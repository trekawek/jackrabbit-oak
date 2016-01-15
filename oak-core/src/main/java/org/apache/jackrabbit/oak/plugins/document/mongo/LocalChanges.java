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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.ReplicaSetInfo.ReplicaSetInfoListener;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalChanges implements ReplicaSetInfoListener {

    private static final Logger LOG = LoggerFactory.getLogger(LocalChanges.class);

    private Map<String, RevisionVector> localChanges = new HashMap<String, RevisionVector>();

    public synchronized void add(NodeDocument doc) {
        if (localChanges.size() > 1000) {
            LOG.warn("Too many local changes: {}", localChanges.size());
            return;
        }
        RevisionVector docRevisions = new RevisionVector(doc.getLastRev().values());
        localChanges.put(doc.getId(), docRevisions);
    }

    public synchronized boolean contains(String documentId) {
        if (documentId == null) {
            return false;
        } else {
            return localChanges.containsKey(documentId);
        }
    }

    public synchronized boolean containsChildrenOf(String parentId) {
        if (parentId == null) {
            return false;
        } else {
            for (String key : localChanges.keySet()) {
                if (parentId.equals(Utils.getParentId(key))) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public synchronized void gotRootRevisions(RevisionVector rootRevision) {
        if (rootRevision == null) {
            return;
        }

        Iterator<RevisionVector> it = localChanges.values().iterator();
        while (it.hasNext()) {
            if (it.next().compareTo(rootRevision) <= 0) {
                it.remove();
            }
        }
    }

}
