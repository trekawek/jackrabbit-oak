package org.apache.jackrabbit.oak.resilience.remote.segment;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.resilience.remote.RemoteMessageProducer;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class TreeNodeWriter {

    public static void main(String[] args) throws Exception {
        File oakDir = new File(System.getProperty("OAK_DIR"));
        oakDir.mkdirs();

        SegmentStore store = FileStore.newFileStore(oakDir).create();
        SegmentNodeStore ns = SegmentNodeStore.newSegmentNodeStore(store).create();

        TreeNodeWriter writer = new TreeNodeWriter(ns, 2, Integer.parseInt(args[0]));
        writer.start();

        store.close();
    }

    private final NodeStore ns;

    private final NodeBuilder rootBuilder;

    private int nodesToCreate;

    private int createdNodes;

    private int treeLevel;

    private int arity;

    private TreeNodeWriter(SegmentNodeStore ns, int arity, int nodesToCreate) {
        this.ns = ns;
        this.rootBuilder = ns.getRoot().builder();
        this.nodesToCreate = nodesToCreate;
        this.treeLevel = computeTreeLevel(arity, nodesToCreate);
        this.arity = arity;
    }

   private void start() throws CommitFailedException, IOException, InterruptedException {
       System.out.println("Create tree with " + treeLevel + " levels and " + nodesToCreate + " nodes with child arity " + arity);
       createTree(0, rootBuilder);
    }

    private void createTree(int level, NodeBuilder builder) throws CommitFailedException, IOException, InterruptedException {
       for (int i = 0; i < arity; i++) {
           NodeBuilder child = builder.child(String.format("child_%d_%d", level, i));
           createdNodes++;
           if (createdNodes % 1000 == 0) {
               ns.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
               System.out.println("Merged. Created nodes: " + createdNodes + ". Last builder: " + child);
           }
           if (createdNodes == nodesToCreate / 10) {
               System.out.println("Sending message");
               RemoteMessageProducer.getInstance().publish("go");
               Thread.sleep(5000);
           }
           if (createdNodes < nodesToCreate && level < treeLevel - 1) {
               createTree(level + 1, child);
           }
       }
    }

    private static int computeTreeLevel(int nodeChildrenCount, int nodesToCreate) {
        return (int) Math.ceil(Math.log(nodesToCreate) / Math.log(nodeChildrenCount));
    }
}
