package org.apache.jackrabbit.oak.resilience.remote.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MongoWriterTest {

    private static DocumentNodeStore ns;

    @BeforeClass
    public static void setupOak() throws IOException {
        DocumentMK.Builder nsBuilder = new DocumentMK.Builder();
        ns = new DocumentNodeStore(nsBuilder);
    }

    @AfterClass
    public static void closeOak() {
        ns.dispose();
    }

    @Test
    public void testRead() {
        for (int i = 0; i < 5000; i++) {
            NodeState child = ns.getRoot().getChildNode("child-" + i);

            assertTrue("child-" + i + " doesn't exists", child.exists());
            assertEquals("wrong value for the 'x' property on child-" + i, (long) Long.valueOf(i), child.getLong("x"));
        }
    }

    @Test
    public void simpleTest() {
        Assert.assertTrue(true);
    }
}
