package org.apache.jackrabbit.oak.resilience.remote.mongo;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.resilience.remote.RemoteMessageProducer;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class MongoWriter {

    public static void main(String[] args) throws Exception {
        DocumentMK.Builder nsBuilder = new DocumentMK.Builder();
        DB db = new MongoClient().getDB("oak-test");
        db.dropDatabase();
        nsBuilder.setMongoDB(new MongoClient().getDB("oak-test"));
        DocumentNodeStore ns = new DocumentNodeStore(nsBuilder);
        for (int i = 0; i < 1000; i++) {
            NodeBuilder builder = ns.getRoot().builder();
            NodeBuilder child = builder.child("child-" + i);
            child.setProperty("x", i);
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            if (i % 100 == 0) {
                System.out.println("Created node " + i);
            }
        }
        RemoteMessageProducer.getInstance().publish("go");
        Thread.sleep(5000);
        System.exit(0);
    }

}
