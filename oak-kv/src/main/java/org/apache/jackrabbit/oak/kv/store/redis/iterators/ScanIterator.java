/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.kv.store.redis.iterators;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import redis.clients.jedis.ScanResult;

import java.util.Iterator;
import java.util.function.Function;

public class ScanIterator<T> extends AbstractIterator<T> {

    private final Function<String, ScanResult<T>> scanCommand;

    private String nextCursor;

    private Iterator<T> results = Iterators.emptyIterator();

    public ScanIterator(Function<String, ScanResult<T>> scanCommand) {
        this.scanCommand = scanCommand;
    }

    @Override
    protected T computeNext() {
        while (!results.hasNext()) {
            if ("0".equals(nextCursor)) {
                return endOfData();
            }
            ScanResult<T> result = scanCommand.apply(nextCursor == null ? "0" : nextCursor);
            nextCursor = result.getStringCursor();
            results = result.getResult().iterator();
        }
        return results.next();
    }
}
