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
package org.apache.jackrabbit.oak.segment.dynamodb;

import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.dynamodb.DynaliteContainer;

import java.io.IOException;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.fail;

public class DynamoRepositoryLockTest {

    private static final Logger log = LoggerFactory.getLogger(DynamoRepositoryLockTest.class);

    @Rule
    public DynaliteContainer dynamoDB = new DynaliteContainer();

    @Test
    public void testFailingLock() throws IOException {
        new DynamoRepositoryLock(dynamoDB.getClient(), "table");
        try {
            new DynamoRepositoryLock(dynamoDB.getClient(), "table");
            fail("The second lock should fail.");
        } catch (IOException e) {
            // it's fine
        }
    }

    @Test(timeout = 20_000)
    public void testWaitingLock() throws IOException, InterruptedException {
        Semaphore s = new Semaphore(0);
        new Thread(() -> {
            try {
                log.info("Acquiring lock");
                DynamoRepositoryLock lock = new DynamoRepositoryLock(dynamoDB.getClient(), "table");
                log.info("Lock acquired. Releasing semaphore.");
                s.release();
                log.info("Semaphore released. Sleeping for 1s.");
                Thread.sleep(1000);
                log.info("Unlocking.");
                lock.unlock();
            } catch (Exception e) {
                log.error("Can't lock or unlock the repo", e);
            }
        }).start();

        log.info("Acquiring semaphore.");
        s.acquire();
        log.info("Semaphore acquired. Acquiring lock.");
        new DynamoRepositoryLock(dynamoDB.getClient(), "table");
    }

}
