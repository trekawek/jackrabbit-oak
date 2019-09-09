package org.apache.jackrabbit.oak.segment.spi.rev;

import java.io.Closeable;
import java.io.IOException;

/**
 * The RevRepository allows to retrieve nodes by their revisions. The changes
 * made in the node builder, derived from these nodes, will be persisted automatically
 * or on demand, with the flush methods.
 *
 * Looking at the implementation, this is an abstraction over FileStore, that allows
 * to read nodes created by the SegmentNodeStore instances.
 */
public interface RevRepository extends Closeable {

    RevNodeState getNodeStateByRevision(String revision);

    void flushData() throws IOException;

    void flushJournal() throws IOException;

}
