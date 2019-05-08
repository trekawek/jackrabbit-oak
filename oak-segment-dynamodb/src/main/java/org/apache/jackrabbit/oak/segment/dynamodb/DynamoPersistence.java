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
package org.apache.jackrabbit.oak.segment.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;

import java.io.IOException;

public class DynamoPersistence implements SegmentNodeStorePersistence {

    private final AmazonDynamoDB amazonDynamoDB;

    private final String tablePrefix;

    public DynamoPersistence(AmazonDynamoDB amazonDynamoDB, String tablePrefix) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.tablePrefix = tablePrefix == null ? "" : tablePrefix;
    }

    @Override
    public SegmentArchiveManager createArchiveManager(boolean mmap, boolean offHeapAccess, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor) throws IOException {
        return new DynamoArchiveManager(new DynamoDB(amazonDynamoDB), tablePrefix + "archives", tablePrefix + "segments", ioMonitor, fileStoreMonitor);
    }

    @Override
    public boolean segmentFilesExist() {
        return DynamoUtils.tableExists(new DynamoDB(amazonDynamoDB), tablePrefix + "archives");
    }

    @Override
    public JournalFile getJournalFile() {
        return new DynamoJournalFile(new DynamoDB(amazonDynamoDB), tablePrefix + "journal");
    }

    @Override
    public GCJournalFile getGCJournalFile() throws IOException {
        return new DynamoGCJournalFile(new DynamoDB(amazonDynamoDB), tablePrefix + "gc_journal");
    }

    @Override
    public ManifestFile getManifestFile() {
        return new DynamoManifestFile(new DynamoDB(amazonDynamoDB), tablePrefix + "manifest");
    }

    @Override
    public RepositoryLock lockRepository() throws IOException {
        return new DynamoRepositoryLock(amazonDynamoDB, tablePrefix + "lock");
    }

}
