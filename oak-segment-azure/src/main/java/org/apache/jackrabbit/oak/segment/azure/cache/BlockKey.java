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
package org.apache.jackrabbit.oak.segment.azure.cache;

public class BlockKey {

    private final long segmentLsb;

    private final long segmentMsb;

    private final int offset;

    public BlockKey(long segmentLsb, long segmentMsb, int offset) {
        this.segmentLsb = segmentLsb;
        this.segmentMsb = segmentMsb;
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlockKey blockKey = (BlockKey) o;
        return segmentLsb == blockKey.segmentLsb &&
                segmentMsb == blockKey.segmentMsb &&
                offset == blockKey.offset;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + Long.hashCode(segmentLsb);
        result = 31 * result + Long.hashCode(segmentMsb);
        result = 31 * result + Integer.hashCode(offset);
        return result;

    }
}
