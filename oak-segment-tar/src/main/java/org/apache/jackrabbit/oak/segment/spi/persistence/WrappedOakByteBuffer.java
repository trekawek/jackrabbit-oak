package org.apache.jackrabbit.oak.segment.spi.persistence;

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
import java.nio.ByteBuffer;

public class WrappedOakByteBuffer implements OakByteBuffer {

    private final ByteBuffer delegate;

    private WrappedOakByteBuffer(ByteBuffer delegate) {
        this.delegate = delegate;
    }

    public static OakByteBuffer wrap(ByteBuffer delegate) {
        return new WrappedOakByteBuffer(delegate);
    }

    public static OakByteBuffer wrap(byte[] buffer) {
        return new WrappedOakByteBuffer(ByteBuffer.wrap(buffer));
    }

    @Override
    public OakByteBuffer slice() {
        return new WrappedOakByteBuffer(delegate.slice());
    }

    @Override
    public OakByteBuffer duplicate() {
        return new WrappedOakByteBuffer(delegate.duplicate());
    }

    @Override
    public int position() {
        return delegate.position();
    }

    @Override
    public void position(int newPosition) {
        delegate.position(newPosition);
    }

    @Override
    public int limit() {
        return delegate.limit();
    }

    @Override
    public void limit(int newLimit) {
        delegate.limit(newLimit);
    }

    @Override
    public boolean hasRemaining() {
        return delegate.hasRemaining();
    }

    @Override
    public int remaining() {
        return delegate.remaining();
    }

    @Override
    public boolean isDirect() {
        return delegate.isDirect();
    }

    @Override
    public void mark() {
        delegate.mark();
    }

    @Override
    public void rewind() {
        delegate.rewind();
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public byte get() {
        return delegate.get();
    }

    @Override
    public byte get(int index) {
        return delegate.get(index);
    }

    @Override
    public void get(byte[] dst) {
        delegate.get(dst);
    }

    @Override
    public void get(byte[] buffer, int offset, int length) {
        delegate.get(buffer, offset, length);
    }

    @Override
    public int getInt() {
        return delegate.getInt();
    }

    @Override
    public int getInt(int index) {
        return delegate.getInt(index);
    }

    @Override
    public long getLong() {
        return delegate.getLong();
    }

    @Override
    public long getLong(int index) {
        return delegate.getLong(index);
    }

    @Override
    public short getShort() {
        return delegate.getShort();
    }

    @Override
    public short getShort(int index) {
        return delegate.getShort();
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return delegate.duplicate();
    }
}
