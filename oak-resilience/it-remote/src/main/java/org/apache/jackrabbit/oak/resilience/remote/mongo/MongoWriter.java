package org.apache.jackrabbit.oak.resilience.remote.mongo;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.resilience.remote.RemoteMessageProducer;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public class MongoWriter {

    public static void main(String[] args) throws Exception {
        DocumentMK.Builder nsBuilder = new DocumentMK.Builder();
        DocumentNodeStore ns = new DocumentNodeStore(nsBuilder);
        for (int i = 0; i < 100000; i++) {
            NodeBuilder builder = ns.getRoot().builder();
            NodeBuilder child = builder.child("child-" + i);
            child.setProperty("x", i);
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            if (i % 1000 == 0) {
                System.out.println("Created node " + i);
            }

            if (i == 50000) {
                RemoteMessageProducer.getInstance().publish("go");
            }
        }
        Thread.sleep(1000 * 60);
    }

}
