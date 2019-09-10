/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.spi.rev;

import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

import java.io.IOException;

/**
 * This is a factory, creating the RevRepository instances. It was introduced
 * not to expose oak-segment-tar implementation details in the OSGi environment.
 */
public interface RevRepositoryFactory {

    RevRepository create(Builder builder) throws IOException;

    default Builder builder() {
        return new Builder(this);
    }

    class Builder {

        private final RevRepositoryFactory factory;

        private SegmentNodeStorePersistence persistence;

        private BlobStore blobStore;

        private boolean readOnly;

        public Builder(RevRepositoryFactory factory) {
            this.factory = factory;
        }

        public Builder withPersistence(SegmentNodeStorePersistence persistence) {
            this.persistence = persistence;
            return this;
        }

        public Builder withBlobStore(BlobStore blobStore) {
            this.blobStore = blobStore;
            return this;
        }

        public Builder readOnly() {
            this.readOnly = true;
            return this;
        }

        public RevRepositoryFactory getFactory() {
            return factory;
        }

        public SegmentNodeStorePersistence getPersistence() {
            return persistence;
        }

        public BlobStore getBlobStore() {
            return blobStore;
        }

        public boolean isReadOnly() {
            return readOnly;
        }

        public RevRepository build() throws IOException {
            return factory.create(this);
        }
    }
}
