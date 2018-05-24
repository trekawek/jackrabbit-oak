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

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.dynamodb.DynaliteContainer;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DynamoJournalFileTest {

    @Rule
    public DynaliteContainer dynamoDB = new DynaliteContainer();

    private DynamoJournalFile journal;

    @Before
    public void setup() {
        journal = new DynamoJournalFile(new DynamoDB(dynamoDB.getClient()), "journal");
    }

    @Test
    public void testSplitJournalFiles() throws IOException {
        assertFalse(journal.exists());

        JournalFileWriter writer = journal.openJournalWriter();
        for (int i = 0; i < 100; i++) {
            writer.writeLine("line " + i);
        }

        assertTrue(journal.exists());

        writer = journal.openJournalWriter();
        for (int i = 100; i < 200; i++) {
            writer.writeLine("line " + i);
        }

        JournalFileReader reader = journal.openJournalReader();
        for (int i = 199; i >= 0; i--) {
            assertEquals("line " + i, reader.readLine());
        }

        
    }

}
