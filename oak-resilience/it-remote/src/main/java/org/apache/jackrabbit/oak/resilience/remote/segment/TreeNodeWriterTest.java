package org.apache.jackrabbit.oak.resilience.remote.segment;

import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TreeNodeWriterTest {

    private static SegmentStore store;

    private static SegmentNodeStore ns;

    @BeforeClass
    public static void setupOak() throws IOException {
        File oakDir = new File(System.getProperty("OAK_DIR"));
        store = FileStore.newFileStore(oakDir).create();
        ns = SegmentNodeStore.newSegmentNodeStore(store).create();
    }

    @AfterClass
    public static void closeOak() {
        store.close();
    }

    @Test
    public void simpleTest() {
        assertTrue(ns.getRoot().getChildNodeEntries().iterator().hasNext());

        int nodesFound = traverseRecursively(ns.getRoot(), 0);
        assertEquals(58000, nodesFound);
        System.out.println("nodes traversed");
    }

    private int traverseRecursively(NodeState node, int level) {
        int count = 0;
        List<String> childNames = new ArrayList<String>();
        for (ChildNodeEntry e : node.getChildNodeEntries()) {
            childNames.add(e.getName());
            NodeState child = e.getNodeState();
            assertEquals("xyz", child.getString("key"));
            count++;
            count += traverseRecursively(child, level + 1);
        }
        Collections.sort(childNames);
        int i = 0;
        for (String childName : childNames) {
            assertEquals(String.format("child_%d_%d", level, i), childName);
            i++;
        }
        return count;
    }
}
