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
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.List;

public class ListIterator extends AbstractIterator<byte[]> {

    private static final int BATCH_SIZE = 16;

    private final Jedis jedis;

    private final byte[] key;

    private int offset;

    private Iterator<byte[]> results = Iterators.emptyIterator();

    public ListIterator(Jedis jedis, byte[] key, int startOffset) {
        this.jedis = jedis;
        this.key = key;
        this.offset = startOffset;
    }

    @Override
    protected byte[] computeNext() {
        if (!results.hasNext()) {
            int start = offset;
            int end = offset + BATCH_SIZE;
            offset = offset + BATCH_SIZE + 1;
            List<byte[]> range = jedis.lrange(key, start, end);
            results = range.iterator();
            if (!results.hasNext()) {
                return endOfData();
            }
        }
        return results.next();
    }
}
