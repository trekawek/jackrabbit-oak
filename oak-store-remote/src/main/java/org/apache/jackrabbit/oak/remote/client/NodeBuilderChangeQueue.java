/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.remote.client;

import org.apache.jackrabbit.oak.remote.proto.NodeBuilderChangeProtos;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderChangeProtos.NodeBuilderChange;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderProtos.NodeBuilderId;
import org.apache.jackrabbit.oak.remote.proto.NodeBuilderServiceGrpc.NodeBuilderServiceBlockingStub;

import java.util.Deque;
import java.util.LinkedList;

public class NodeBuilderChangeQueue {

    private final Deque<NodeBuilderChange> deque = new LinkedList<>();

    private final NodeBuilderId nodeBuilderId;

    private final NodeBuilderServiceBlockingStub nodeBuilderService;

    public NodeBuilderChangeQueue(NodeBuilderServiceBlockingStub nodeBuilderService, NodeBuilderId nodeBuilderId) {
        this.nodeBuilderService = nodeBuilderService;
        this.nodeBuilderId = nodeBuilderId;
    }

    public void add(NodeBuilderChange nodeDiff) {
        deque.add(nodeDiff);
    }

    public boolean flush() {
        if (deque.isEmpty()) {
            return false;
        }
        NodeBuilderChangeProtos.NodeBuilderChangeList.Builder list = NodeBuilderChangeProtos.NodeBuilderChangeList.newBuilder();
        NodeBuilderChange change;
        while ((change = deque.poll()) != null) {
            list.addChange(change);
        }
        nodeBuilderService.apply(list.build());
        return true;
    }
}
