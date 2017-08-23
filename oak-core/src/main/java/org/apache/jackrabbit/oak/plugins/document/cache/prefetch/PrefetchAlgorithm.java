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

/**
 * Implementation of the PrefetchAlgorithm analyses the outgoing document DB
 * requests and tries to predict the future ones.
 */
public interface PrefetchAlgorithm {

    /**
     * Create a new session. The DocumentStore implementation will use this object
     * to report all the DB requests that belongs together (eg. they are bound
     * to the same thread or HTTP request) and ask it for the prefetch candidates.
     *
     * @return A new prefetch session.
     */
    PrefetchTimeSeries newSession();

}
