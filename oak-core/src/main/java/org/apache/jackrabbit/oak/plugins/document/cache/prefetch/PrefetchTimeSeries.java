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

import java.util.List;

/**
 * This class represents a series of related requests. Depending on the DocumentStore
 * implementation, they may be done within a single HTTP request, client session,
 * etc. The prefetch algorithm should assume that these requests belongs to the
 * same cluster.
 * <p>
 *
 */
public interface PrefetchTimeSeries {

    /**
     * The onRequest() method is invoked every time the DocumentStore client
     * requests a document. It doesn't matter whether the document is served
     * from the cache or requested from the server. This allows the underlying
     * algorithm to build the prediction model.
     *
     * @param request
     */
    void onRequest(Request request);

    /**
     * Predict the candidates for the subsequent requests.
     *
     * @return A list of potential candidates for the next request.
     */
    List<Request> getCandidates();

    /**
     * This method is called when the session is closed. There won't be any
     * more request calls related to this series.
     */
    void onClose();
}
