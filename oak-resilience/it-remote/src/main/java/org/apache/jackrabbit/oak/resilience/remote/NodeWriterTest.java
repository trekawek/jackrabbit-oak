package org.apache.jackrabbit.oak.resilience.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.BeforeClass;
import org.junit.Test;

public class NodeWriterTest {

    private static SegmentStore store;

    private static SegmentNodeStore ns;

    @BeforeClass
    public static void setupOak() throws IOException {
        File oakDir = new File(System.getProperty("OAK_DIR"));
        store = FileStore.newFileStore(oakDir).create();
        ns = SegmentNodeStore.newSegmentNodeStore(store).create();
    }

    public static void closeOak() {
        store.close();
    }

    @Test
    public void testRead() {
        for (int i = 0; i < 5000; i++) {
            NodeState child = ns.getRoot().getChildNode("child-" + i);

            assertTrue("child-" + i + " doesn't exists", child.exists());
            assertEquals("wrong value for the 'x' property on child-" + i, (long) Long.valueOf(i), child.getLong("x"));
        }
    }
}
