package org.apache.jackrabbit.oak.segment.spi.state;

import java.io.Closeable;
import java.io.IOException;

public interface RevisionableNodeStore extends Closeable {

    RevisionableNodeState getNodeStateByRevision(String revision);

    void flushData() throws IOException;

    void flushJournal() throws IOException;

}
