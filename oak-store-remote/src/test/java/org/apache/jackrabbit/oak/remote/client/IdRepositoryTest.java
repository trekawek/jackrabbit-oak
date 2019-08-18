package org.apache.jackrabbit.oak.remote.client;

import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IdRepositoryTest {

    IdRepository<NodeBuilderId> repository;

    @Before
    public void setup() {
        repository = new IdRepository<>(NodeBuilderId::getValue);
    }

    @Test
    public void testRepository() throws InterruptedException {
        NodeBuilderId id1 = createId(100);
        NodeBuilderId id2 = createId(101);
        NodeBuilderId id3 = createId(102);
        repository.addId(id1);
        repository.addId(id2);
        repository.addId(id3);

        id1 = null;
        System.gc();
        assertReleasedIds(100);

        id2 = null;
        id3 = null;
        System.gc();
        assertReleasedIds(101, 102);
    }

    private void assertReleasedIds(long... ids) throws InterruptedException {
        Set<Long> expected = new HashSet<>();
        for (long id : ids) {
            expected.add(id);
        }

        Set<Long> actual = null;
        for (int i = 0; i < 5; i++) {
            actual = new HashSet<>(repository.getClearList());
            if (expected.equals(actual)) {
                Assert.assertEquals(expected, actual);
                return;
            }
            Thread.sleep(100);
        }
        Assert.assertEquals(expected, actual);
    }

    private static NodeBuilderId createId(long id) {
        return NodeBuilderId.newBuilder().setValue(id).build();
    }

}
