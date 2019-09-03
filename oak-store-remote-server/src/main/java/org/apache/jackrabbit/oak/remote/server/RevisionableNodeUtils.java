package org.apache.jackrabbit.oak.remote.server;

import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.RevisionableNodeState;

import static com.google.common.base.Preconditions.checkArgument;

public final class RevisionableNodeUtils {

    private RevisionableNodeUtils() {
    }

    public static String getRevision(NodeState nodeState) {
        checkArgument(nodeState instanceof RevisionableNodeState);
        return ((RevisionableNodeState) nodeState).getRevision();
    }

    public static NodeStateId getNodeStateId(NodeState nodeState) {
        return NodeStateId.newBuilder().setRevision(getRevision(nodeState)).build();
    }

}
