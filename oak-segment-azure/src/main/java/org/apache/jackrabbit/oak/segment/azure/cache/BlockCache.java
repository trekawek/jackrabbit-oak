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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;

public class BlockCache {

    private final Cache<BlockKey, Block> cache;

    private final int blockSize;

    public BlockCache(int maxSize, int blockSize) {
        this.blockSize = blockSize;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .build();
    }

    public Block getBlock(long msb, long lsb, int offset, CacheByteBuffer.SegmentSupplier segmentSupplier) throws IOException {
        checkArgument(offset % blockSize == 0);

        BlockKey key = new BlockKey(msb, lsb, offset);
        Block block = cache.getIfPresent(key);
        if (block != null) {
            return block;
        }

        Block blockToReturn = loadSegmentToCache(msb, lsb, offset, segmentSupplier);
        if (blockToReturn == null) {
            throw new IOException("Can't find block. Segment: " + new UUID(msb, lsb) + ", block: " + (offset / blockSize));
        }
        return blockToReturn;
    }

    private Block loadSegmentToCache(long msb, long lsb, int offset, CacheByteBuffer.SegmentSupplier segmentSupplier) throws IOException {
        Block blockToReturn = null;
        ByteBuffer segment = segmentSupplier.getSegment();
        while (segment.hasRemaining()) {
            BlockKey newKey = new BlockKey(msb, lsb, segment.position());
            ByteBuffer blockData = segment.slice();
            blockData.limit(Integer.min(blockSize, blockData.limit()));
            Block newBlock = new Block(blockData);
            cache.put(newKey, newBlock);
            if (segment.position() == offset) {
                blockToReturn = newBlock;
            }
            segment.position(segment.position() + blockData.limit());
        }
        return blockToReturn;
    }

    public int getBlockSize() {
        return blockSize;
    }
}
