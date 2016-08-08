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
package org.apache.jackrabbit.oak.plugins.document.mount;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MountedNodeStoreTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore main;

    private NodeStore priv;

    @Before
    public void setUp() throws IOException {
        priv = new MemoryNodeStore();

        MountInfoProvider provider = SimpleMountInfoProvider.newBuilder().readOnlyMount("private", "/libs").build();
        MountedNodeStore mounted = new MountedNodeStore(provider, priv, "private", NodeStateDiffer.DEFAULT_DIFFER);

        main = builderProvider.newBuilder().getNodeStore();
        main.setNodeStateCache(mounted);
    }

    @Test
    public void testReadingFromPrivate() throws CommitFailedException {
        NodeBuilder nb = priv.getRoot().builder();
        create(nb, "/libs/a", "/libs/b", "/libs/c", "/libs/a/a1", "/libs/a/a1/a2");
        nb.getChildNode("libs").setProperty("storage-type", "private", Type.STRING);
        merge(priv, nb);

        nb = main.getRoot().builder();
        create(nb, "/x", "/y", "/z", "/libs");
        nb.setProperty("storage-type", "main", Type.STRING);
        merge(main, nb);

        assertExists("/x", "/y", "/z");
        assertEquals("main", main.getRoot().getString("storage-type"));
        assertExists("/libs");
        assertEquals("private", main.getRoot().getChildNode("libs").getString("storage-type"));
        assertExists("/libs/a", "/libs/b", "/libs/c");
        assertExists("/libs/a/a1", "/libs/a/a1/a2");
    }

    @Test
    public void testPathFragment() throws CommitFailedException {
        NodeBuilder nb = priv.getRoot().builder();
        create(nb, "/content/xyz/oak:mount-private/xyz");
        merge(priv, nb);

        nb = main.getRoot().builder();
        create(nb, "/content/xyz/oak:mount-private");
        nb.getChildNode("content").getChildNode("xyz").setProperty("storage-type", "main", Type.STRING);
        merge(main, nb);

        assertExists("/content/xyz");
        assertEquals("main", getNode(main.getRoot(), "/content/xyz").getString("storage-type"));
        assertExists("/content/xyz/oak:mount-private");
        assertExists("/content/xyz/oak:mount-private/xyz");
    }

    private static NodeState create(NodeBuilder b, String... paths) {
        for (String path : paths) {
            NodeBuilder cb = b;
            for (String pathElement : PathUtils.elements(path)) {
                cb = cb.child(pathElement);
            }
        }
        return b.getNodeState();
    }

    private static NodeState merge(NodeStore ns, NodeBuilder nb) throws CommitFailedException {
        return ns.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private void assertExists(String... paths) {
        for (String p : paths) {
            assertTrue("Path [" + p + "] doesn't exist", getNode(main.getRoot(), p).exists());
        }
    }
}
