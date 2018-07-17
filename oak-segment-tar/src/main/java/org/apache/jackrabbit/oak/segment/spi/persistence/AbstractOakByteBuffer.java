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
package org.apache.jackrabbit.oak.segment.spi.persistence;

import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class AbstractOakByteBuffer implements OakByteBuffer {

    // Invariants:
    // mark <= position <= limit <= capacity
    // 0 <= position
    protected int mark = -1;

    protected int position = 0;

    protected int limit;

    protected int capacity;

    protected AbstractOakByteBuffer(int mark, int position, int limit, int capacity) {
        checkArgument(capacity >= 0);

        this.capacity = capacity;
        limit(limit);
        position(position);
        if (mark >= 0 ) {
            checkArgument(mark <= position);
            this.mark = mark;
        }
    }

    @Override
    public abstract OakByteBuffer slice();

    @Override
    public abstract OakByteBuffer duplicate();

    @Override
    public abstract byte get();

    @Override
    public abstract byte get(int index);

    @Override
    public abstract void get(byte[] dst, int offset, int length);

    @Override
    public int position() {
        return position;
    }

    @Override
    public void position(int newPosition) {
        checkArgument(newPosition <= limit);
        checkArgument(newPosition >= 0);
        position = newPosition;
        if (mark > position) {
            mark = -1;
        }
    }

    @Override
    public int limit() {
        return limit;
    }

    @Override
    public void limit(int newLimit) {
        checkArgument(newLimit <= capacity);
        checkArgument(newLimit >= 0);
        limit = newLimit;
        if (position > limit) {
            position(limit);
        }
    }

    @Override
    public boolean hasRemaining() {
        return position < limit;
    }

    @Override
    public int remaining() {
        return limit - position;
    }

    @Override
    public boolean isDirect() {
        return false;
    }

    @Override
    public void mark() {
        mark = position;
    }

    @Override
    public void reset() {
        if (mark < 0) {
            throw new InvalidMarkException();
        }
        position = mark;
    }

    @Override
    public void rewind() {
        position = 0;
        mark = -1;
    }

    @Override
    public void get(byte[] dst) {
        get(dst, 0, dst.length);
    }

    @Override
    public int getInt() {
        byte[] buffer = new byte[Integer.BYTES];
        get(buffer);
        return ByteBuffer.wrap(buffer).getInt();
    }

    @Override
    public int getInt(int index) {
        byte[] buffer = new byte[Integer.BYTES];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = get(index + i);
        }
        return ByteBuffer.wrap(buffer).getInt();
    }

    @Override
    public long getLong() {
        byte[] buffer = new byte[Long.BYTES];
        get(buffer);
        return ByteBuffer.wrap(buffer).getLong();
    }

    @Override
    public long getLong(int index) {
        byte[] buffer = new byte[Long.BYTES];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = get(index + i);
        }
        return ByteBuffer.wrap(buffer).getInt();
    }

    @Override
    public short getShort() {
        byte[] buffer = new byte[Short.BYTES];
        get(buffer);
        return ByteBuffer.wrap(buffer).getShort();
    }

    @Override
    public short getShort(int index) {
        byte[] buffer = new byte[Short.BYTES];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = get(index + i);
        }
        return ByteBuffer.wrap(buffer).getShort();
    }

    @Override
    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(remaining());
        for (int i = position; i < limit; i++) {
            buffer.put(get(i));
        }
        buffer.position(position);
        return buffer;
    }
}
