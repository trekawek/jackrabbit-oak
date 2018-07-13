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

public interface OakByteBuffer {

    OakByteBuffer slice();

    OakByteBuffer duplicate();

    int position();

    void position(int newPosition);

    int limit();

    void limit(int newLimit);

    boolean hasRemaining();

    int remaining();

    boolean isDirect();

    void mark();

    void rewind();

    void reset();

    byte get();

    byte get(int index);

    void get(byte[] dst);

    void get(byte[] buffer, int offset, int length);

    int getInt();

    int getInt(int index);

    long getLong();

    long getLong(int index);

    short getShort();

    short getShort(int index);

    ByteBuffer toByteBuffer();

}
