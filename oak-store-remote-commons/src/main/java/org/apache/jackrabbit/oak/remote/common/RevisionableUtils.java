package org.apache.jackrabbit.oak.remote.common;

import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.segment.spi.state.RevisionableNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkArgument;

public final class RevisionableUtils {

    private RevisionableUtils() {
    }

    public static String getRevision(NodeState nodeState) {
        checkArgument(nodeState instanceof RevisionableNodeState);
        return ((RevisionableNodeState) nodeState).getRevision();
    }

    public static NodeStateId getNodeStateId(NodeState nodeState) {
        return NodeStateId.newBuilder().setRevision(getRevision(nodeState)).build();
    }

    public static boolean fastEquals(NodeState nodeState1, NodeState nodeState2) {
        String rev1 = getRevision(nodeState1);
        String rev2 = getRevision(nodeState2);
        if (rev1 == null || rev2 == null) {
            return false;
        }
        return rev1.equals(rev2);
    }

}
