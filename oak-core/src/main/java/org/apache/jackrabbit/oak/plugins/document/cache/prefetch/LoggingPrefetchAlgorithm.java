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
package org.apache.jackrabbit.oak.plugins.document.cache.prefetch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LoggingPrefetchAlgorithm implements PrefetchAlgorithm {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingPrefetchAlgorithm.class);

    @Override
    public PrefetchTimeSeries newSession() {
        return new PrefetchTimeSeries() {

            private final List<Request> requests = new ArrayList<>();

            @Override
            public void onRequest(Request request) {
                requests.add(request);
            }

            @Override
            public List<Request> getCandidates() {
                return Collections.emptyList();
            }

            @Override
            public void onClose() {
                if (!requests.isEmpty()) {
                    StringBuilder msg = new StringBuilder("Session summary:\n");
                    msg.append("Session: ").append(requests.get(0).getThreadName()).append('\n');
                    requests.forEach(r -> msg.append(r).append('\n'));
                    LOG.info(msg.toString());
                }
            }
        };
    }
}
