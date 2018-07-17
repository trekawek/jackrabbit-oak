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

import org.apache.jackrabbit.oak.segment.azure.AzureSegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.AbstractOakByteBuffer;
import org.apache.jackrabbit.oak.segment.spi.persistence.OakByteBuffer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public class CacheByteBuffer extends AbstractOakByteBuffer {

    private final AzureSegmentArchiveEntry indexEntry;

    private final BlockCache blockCache;

    private final SegmentSupplier segmentSupplier;

    private final int positionOffset;

    public CacheByteBuffer(BlockCache blockCache, AzureSegmentArchiveEntry indexEntry, SegmentSupplier segmentSupplier) {
        super(-1, 0, indexEntry.getLength(), indexEntry.getLength());
        this.indexEntry = indexEntry;
        this.blockCache = blockCache;
        this.segmentSupplier = segmentSupplier;
        this.positionOffset = 0;
    }

    private CacheByteBuffer(CacheByteBuffer original) {
        this(original, 0);
    }

    private CacheByteBuffer(CacheByteBuffer original, int positionOffset) {
        super(original.mark - positionOffset, 0, original.limit - positionOffset, original.capacity - positionOffset);
        this.indexEntry = original.indexEntry;
        this.blockCache = original.blockCache;
        this.segmentSupplier = original.segmentSupplier;
        this.positionOffset = positionOffset;
    }

    @Override
    public OakByteBuffer slice() {
        return new CacheByteBuffer(this, position() + positionOffset);
    }

    @Override
    public OakByteBuffer duplicate() {
        return new CacheByteBuffer(this);
    }

    @Override
    public byte get() {
        return get(position++);
    }

    @Override
    public void get(byte[] dst, int offset, int length) {
        int absoluteIndex = position + positionOffset;

        int currentOffset = offset;
        int remaining = length;
        while (remaining >= 0 && absoluteIndex < capacity + positionOffset) {
            int blockOffset = getBlockOffset(absoluteIndex);
            int positionWithinBlock = absoluteIndex - blockOffset;
            int lengthForBlock = Integer.min(remaining, blockCache.getBlockSize() - positionWithinBlock);

            OakByteBuffer block = getBlock(blockOffset).duplicate();
            block.position(positionWithinBlock);
            block.get(dst, currentOffset, lengthForBlock);

            absoluteIndex += lengthForBlock;
            currentOffset += lengthForBlock;
            remaining -= lengthForBlock;
        }

        position = absoluteIndex - positionOffset;
    }

    @Override
    public byte get(int index) {
        int absoluteIndex = positionOffset + index;
        int blockOffset = getBlockOffset(absoluteIndex);
        return getBlock(blockOffset).get(absoluteIndex - blockOffset);
    }

    private int getBlockOffset(int segmentOffset) {
        return segmentOffset - (segmentOffset % blockCache.getBlockSize());
    }

    private OakByteBuffer getBlock(int blockOffset) {
        try {
            return blockCache.getBlock(indexEntry.getMsb(), indexEntry.getLsb(), blockOffset, segmentSupplier).getBuffer();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public interface SegmentSupplier {
        ByteBuffer getSegment() throws IOException;
    }
}
