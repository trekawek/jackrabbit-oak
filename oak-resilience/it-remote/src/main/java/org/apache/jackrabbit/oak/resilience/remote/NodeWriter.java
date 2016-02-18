package org.apache.jackrabbit.oak.resilience.remote;

import java.io.File;

import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public class NodeWriter {

    public static void main(String[] args) throws Exception {
        File oakDir = new File(System.getProperty("OAK_DIR"));
        oakDir.mkdirs();

        SegmentStore store = FileStore.newFileStore(oakDir).create();
        SegmentNodeStore ns = SegmentNodeStore.newSegmentNodeStore(store).create();

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
