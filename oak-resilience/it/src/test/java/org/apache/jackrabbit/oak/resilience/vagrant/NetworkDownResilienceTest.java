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
package org.apache.jackrabbit.oak.resilience.vagrant;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.jackrabbit.oak.resilience.junit.JunitProcess;
import org.apache.jackrabbit.oak.resilience.remote.mongo.MongoWriter;
import org.apache.jackrabbit.oak.resilience.remote.mongo.MongoWriterTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import eu.rekawek.toxiproxy.model.Proxy;

public class NetworkDownResilienceTest {

    private VagrantVM vm;

    private RemoteJar itJar;

    private Proxy proxy;

    @BeforeClass
    public static void checkMongo() {
        assumeTrue(isMongoAvailable());
    }

    @Before
    public void setupVm() throws IOException {
        vm = new VagrantVM.Builder().setVagrantFile("src/test/resources/Vagrantfile").build();
        vm.init();
        vm.start();
        itJar = vm.uploadJar("org.apache.jackrabbit", "oak-resilience-it-remote", "1.4-SNAPSHOT");
        proxy = vm.forwardPortToGuest(27017, 27017);
    }

    @After
    public void destroyVm() throws IOException {
        vm.stop();
        vm.destroy();
    }

    @Test
    public void testWriteResilience() throws IOException, TimeoutException, InterruptedException {
        RemoteJvmProcess process = itJar.runClass(MongoWriter.class.getName(), null);
        process.waitForMessage("go", 600);

        proxy.disable();
        process.waitForFinish();
        proxy.enable();

        JunitProcess junit = itJar.runJunit(MongoWriterTest.class.getName(), null);
        assertTrue(junit.read().wasSuccessful());
    }

    private static boolean isMongoAvailable() {
        Mongo mongo = null;
        try {
            mongo = new MongoClient();
            mongo.getDatabaseNames();
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            if (mongo != null) {
                mongo.close();
            }
        }
    }
}