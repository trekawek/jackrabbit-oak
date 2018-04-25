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

package org.apache.jackrabbit.oak.kv;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

class KVBlob implements Blob {

    private final BlobStore blobStore;

    private final String blobId;

    KVBlob(BlobStore blobStore, String blobId) {
        this.blobStore = blobStore;
        this.blobId = blobId;
    }

    @Override
    public InputStream getNewStream() {
        try {
            return blobStore.getInputStream(blobId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long length() {
        try {
            return blobStore.getBlobLength(blobId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getReference() {
        return blobStore.getReference(blobId);
    }

    @Override
    public String getContentIdentity() {
        return blobId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        return equals((KVBlob) o);
    }

    private boolean equals(KVBlob o) {
        return Objects.equals(blobStore, o.blobStore) && Objects.equals(blobId, o.blobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(blobStore, blobId);
    }

}
