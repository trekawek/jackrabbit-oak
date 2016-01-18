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
package org.apache.jackrabbit.oak.plugins.document.mongo.replica;

import static com.google.common.collect.Maps.transformValues;
import static org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetMemberState.PRIMARY;
import static org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetMemberState.RECOVERING;
import static org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetMemberState.SECONDARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetInfo;
import org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetMemberState;
import org.bson.BasicBSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Function;
import com.mongodb.DB;

public class ReplicaSetInfoTest {

    private ReplicaSetInfo replica;

    private ReplicationSetStatusMock replicationSet;

    @Before
    public void resetEstimator() {
        DB db = mock(DB.class);
        when(db.getName()).thenReturn("oak-db");
        when(db.getSisterDB(Mockito.anyString())).thenReturn(db);
        replica = new ReplicaSetInfo(db, null, 0l, 0l) {
            @Override
            protected Map<String, TimestampedRevisionVector> getRootRevisions(Iterable<String> hosts) {
                return transformValues(replicationSet.memberRevisions,
                        new Function<RevisionBuilder, TimestampedRevisionVector>() {
                    @Override
                    public TimestampedRevisionVector apply(RevisionBuilder input) {
                        return new TimestampedRevisionVector(input.revs, System.currentTimeMillis());
                    }
                });
            }
        };
    }

    @Test
    public void testMinimumRevision() {
        addInstance(PRIMARY, "mp").addRevisions(20, 18, 19);
        addInstance(SECONDARY, "m1").addRevisions(20, 18, 3);
        addInstance(SECONDARY, "m2").addRevisions(20, 1, 17);
        updateRevisions();

        assertEquals(20, replica.getMinimumRootRevisions().getRevision(0).getTimestamp());
        assertEquals( 1, replica.getMinimumRootRevisions().getRevision(1).getTimestamp());
        assertEquals( 3, replica.getMinimumRootRevisions().getRevision(2).getTimestamp());
    }

    @Test
    public void testIsSafeRevision() {
        addInstance(PRIMARY, "mp").addRevisions(15, 21, 22);
        addInstance(SECONDARY, "m1").addRevisions(10, 21, 11);
        addInstance(SECONDARY, "m2").addRevisions(15, 14, 13);
        addInstance(SECONDARY, "m3").addRevisions(14, 13, 22);
        updateRevisions();

        assertTrue(replica.isMoreRecentThan(lastRev(9, 13, 10)));
        assertFalse(replica.isMoreRecentThan(lastRev(11, 14, 10)));
    }

    @Test
    public void testUnknownStateIsNotSafe() {
        addInstance(SECONDARY, "m1").addRevisions(10, 21, 11);
        addInstance(RECOVERING, "m2");
        updateRevisions();

        assertNull(replica.getMinimumRootRevisions());
        assertFalse(replica.isMoreRecentThan(lastRev(1, 1, 1)));
    }

    @Test
    public void testEmptyIsNotSafe() {
        addInstance(PRIMARY, "m1");
        updateRevisions();

        assertNull(replica.getMinimumRootRevisions());
        assertFalse(replica.isMoreRecentThan(lastRev(1, 1, 1)));
    }

    private RevisionBuilder addInstance(ReplicaSetMemberState state, String name) {
        if (replicationSet == null) {
            replicationSet = new ReplicationSetStatusMock();
        }
        return replicationSet.addInstance(state, name);
    }

    private void updateRevisions() {
        replica.updateRevisions(replicationSet.getMembers());
        replicationSet = null;
    }

    private static RevisionVector lastRev(int... timestamps) {
        return new RevisionBuilder().addRevisions(timestamps).revs;
    }

    private class ReplicationSetStatusMock {

        private List<BasicBSONObject> members = new ArrayList<BasicBSONObject>();

        private Map<String, RevisionBuilder> memberRevisions = new HashMap<String, RevisionBuilder>();

        private RevisionBuilder addInstance(ReplicaSetMemberState state, String name) {
            BasicBSONObject member = new BasicBSONObject();
            member.put("stateStr", state.name());
            member.put("name", name);
            members.add(member);

            RevisionBuilder builder = new RevisionBuilder();
            memberRevisions.put(name, builder);
            return builder;
        }

        private List<BasicBSONObject> getMembers() {
            return members;
        }

        private RevisionVector revisions(String name) {
            return memberRevisions.get(name).revs;
        }
    }

    private static class RevisionBuilder {

        private RevisionVector revs = new RevisionVector();

        private RevisionBuilder addRevisions(int... timestamps) {
            for (int i = 0; i < timestamps.length; i++) {
                addRevision(timestamps[i], 0, i, false);
            }
            return this;
        }

        private RevisionBuilder addRevision(int timestamp, int counter, int clusterId, boolean branch) {
            Revision rev = new Revision(timestamp, counter, clusterId, branch);
            revs = revs.update(rev);
            return this;
        }
    }
}