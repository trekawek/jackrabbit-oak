package org.apache.jackrabbit.oak.segment.spi.rev;

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This node state is able to expose its revision.
 */
public interface RevNodeState extends NodeState {

    /**
     * The revision name, uniquely identifying this node state in the {@link RevRepository}.
     * @return node's revision
     */
    String getRevision();

}
