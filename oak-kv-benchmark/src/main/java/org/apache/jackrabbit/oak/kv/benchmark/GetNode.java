package org.apache.jackrabbit.oak.kv.benchmark;

import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.emptyMap;
import static java.util.Collections.shuffle;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.jackrabbit.oak.kv.store.Value.newStringValue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.apache.jackrabbit.oak.kv.store.leveldb.LevelDBStore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class GetNode {

    @Param({"1", "10", "100", "1000"})
    public int nodes;

    @Param({"0", "1", "10", "100"})
    public int properties;

    private Path directory;

    private LevelDBStore store;

    private List<ID> ids = new ArrayList<>();

    @Setup
    public void setUp() throws Exception {
        directory = createTempDirectory("GetNode-");
        store = new LevelDBStore(directory.toFile());

        for (int i = 0; i < nodes; i++) {
            Map<String, Value> p = new HashMap<>();

            for (int j = 0; j < properties; j++) {
                p.put(format("p-%08d", j), newStringValue(format("v-%08d", j)));
            }

            ids.add(store.putNode(p, emptyMap()));
        }

        shuffle(ids);
    }

    @TearDown
    public void tearDown() throws Exception {
        try {
            store.close();
        } finally {
            deleteDirectory(directory.toFile());
        }
    }

    @Benchmark
    public void run() throws Exception {
        for (ID id : ids) {
            store.getNode(id);
        }
    }

}
