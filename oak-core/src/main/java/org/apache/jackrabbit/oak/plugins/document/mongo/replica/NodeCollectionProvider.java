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

import static com.google.common.collect.Sets.difference;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientURI;

public class NodeCollectionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(NodeCollectionProvider.class);

    private final Map<String, DBCollection> collections = new ConcurrentHashMap<String, DBCollection>();

    private final String credentials;

    private final String dbName;

    public NodeCollectionProvider(String credentials, String dbName) {
        this.credentials = credentials;
        this.dbName = dbName;
    }

    public void retain(Set<String> hostNames) {
        close(difference(collections.keySet(), hostNames));
    }

    public void close() {
        close(collections.keySet());
    }

    private void close(Set<String> hostNames) {
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
    public DBCollection get(String hostName) throws UnknownHostException {
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

}
