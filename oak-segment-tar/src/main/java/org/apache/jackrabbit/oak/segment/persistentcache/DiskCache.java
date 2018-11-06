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
 *
 */

package org.apache.jackrabbit.oak.segment.persistentcache;

import static java.util.Collections.emptySet;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.jetbrains.annotations.NotNull;

/**
 * michid document
 */
public class DiskCache implements Closeable{

    @NotNull
    private final TarFiles tarFiles;

    public DiskCache(@NotNull TarFiles tarFiles) {
        this.tarFiles = tarFiles;
    }

    public DiskCache(@NotNull File directory) throws IOException {
        this(TarFiles.builder()
            .withDirectory(directory)
            .withMaxFileSize(FileStoreBuilder.DEFAULT_MAX_FILE_SIZE * 1024 * 1024)
            .withFileStoreMonitor(new FileStoreMonitorAdapter())
            .withIOMonitor(new IOMonitorAdapter())
            .withMemoryMapping(false)
            .withTarRecovery((uuid, data, entryRecovery) -> { })
            .build());
    }

    public ByteBuffer readSegment(long msb, long lsb) {
        return tarFiles.readSegment(msb, lsb);
    }

    public boolean containsSegment(long msb, long lsb) {
        return tarFiles.containsSegment(msb, lsb);
    }

    public void writeSegment(long msb, long lsb, byte[] data, int offset, int size) throws IOException {
        tarFiles.writeSegment(
            new UUID(msb, lsb), data, offset, size,
            newGCGeneration(0, 0, false),
            emptySet(), emptySet());  // michid skip auxiliary entries for now. Segment graph is not needed. Binary references only for BlobGC.
    }

    public void writeSegment(long msb, long lsb, ByteBuffer buffer) throws IOException {
        if (buffer.hasArray()) {
            writeSegment(msb, lsb, buffer.array(), buffer.arrayOffset(), buffer.remaining());
        } else {
            ByteBuffer dup = buffer.duplicate();
            byte[] data = new byte[dup.remaining()];
            dup.get(data);
            writeSegment(msb, lsb, data, 0, data.length);
        }
    }

    @Override
    public void close() throws IOException {
        tarFiles.close();
    }

}
