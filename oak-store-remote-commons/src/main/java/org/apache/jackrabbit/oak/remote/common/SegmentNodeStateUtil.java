package org.apache.jackrabbit.oak.remote.common;

import org.apache.jackrabbit.oak.remote.proto.NodeStateProtos.NodeStateId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkArgument;

public final class SegmentNodeStateUtil {

    private SegmentNodeStateUtil() {
    }

    public static String getRevision(NodeState nodeState) {
        checkArgument(nodeState instanceof SegmentNodeState);
        return ((SegmentNodeState) nodeState).getRecordId().toString();
    }

    public static NodeStateId getNodeStateId(NodeState nodeState) {
        return NodeStateId.newBuilder().setRevision(getRevision(nodeState)).build();
    }

}
