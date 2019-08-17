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
package org.apache.jackrabbit.oak.remote.common;

import org.apache.jackrabbit.oak.remote.proto.CommitProtos;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public final class CommitInfoUtil {

    private CommitInfoUtil() {
    }

    public static CommitProtos.CommitInfo serialize(CommitInfo info) {
        CommitProtos.CommitInfo.Builder ciBuilder = CommitProtos.CommitInfo.newBuilder();
        ciBuilder.setSessionId(info.getSessionId());
        ciBuilder.setUserId(info.getUserId());
        ciBuilder.setIsExternal(info.isExternal());
        Map<String, String> commitInfoMap = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : info.getInfo().entrySet()) {
            if (!(e.getValue() instanceof String)) {
                throw new IllegalArgumentException("Only string values are allowed in the CommitInfo; " + e.getValue().getClass() + " given for " + e.getKey());
            }
            commitInfoMap.put(e.getKey(), (String) e.getValue());
        }
        ciBuilder.putAllCommitInfo(commitInfoMap);
        return ciBuilder.build();
    }

    public static CommitInfo deserialize(CommitProtos.CommitInfo info) {
        Map<String, Object> map = new HashMap<>();
        map.putAll(info.getCommitInfoMap());
        return new CommitInfo(info.getSessionId(), info.getUserId(), map, info.getIsExternal());
    }
}
