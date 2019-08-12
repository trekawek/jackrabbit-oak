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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class IdRepository<T> {

    private static final Logger log = LoggerFactory.getLogger(IdRepository.class);

    private final ReferenceQueue<T> queue;

    private final Map<WeakReference<T>, Long> references;

    private final Function<T, Long> idAccessor;

    public IdRepository(IdAccessor<T> idAccessor) {
        this.idAccessor = idAccessor;
        this.queue = new ReferenceQueue<>();
        this.references = new HashMap<>();
    }

    public void addId(T id) {
        WeakReference<T> weakReference = new WeakReference<T>(id, queue);
        references.put(weakReference, idAccessor.apply(id));
    }

    public List<Long> getClearList() {
        List<Long> toBeRemoved = new ArrayList<>();
        Reference<? extends T> ref;
        while ((ref = queue.poll()) != null) {
            if (references.containsKey(ref)) {
                toBeRemoved.add(references.remove(ref));
            } else {
                log.warn("Can't find id for " + ref);
            }
        }
        return toBeRemoved;
    }

    public interface IdAccessor<T> extends Function<T, Long> {
    }
}
