package org.apache.jackrabbit.oak.segment.spi.state;

import org.apache.jackrabbit.oak.spi.state.NodeState;

public interface RevisionableNodeState extends NodeState {

    String getRevision();

}
