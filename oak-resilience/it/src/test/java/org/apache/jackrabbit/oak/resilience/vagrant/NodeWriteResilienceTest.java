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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.jackrabbit.oak.resilience.junit.JunitProcess;
import org.apache.jackrabbit.oak.resilience.remote.NodeWriter;
import org.apache.jackrabbit.oak.resilience.remote.NodeWriterTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NodeWriteResilienceTest {

    private static final Map<String, String> PROPS = Collections.singletonMap("OAK_DIR",
            "/home/vagrant/" + NodeWriteResilienceTest.class.getName());

    private VagrantVM vm;

    private RemoteJar itJar;

    @Before
    public void setupVm() throws IOException {
        vm = new VagrantVM.Builder().setVagrantFile("src/test/resources/Vagrantfile").build();
        vm.init();
        vm.start();
        itJar = vm.uploadJar("org.apache.jackrabbit", "oak-resilience-it-remote", "1.4-SNAPSHOT");
    }

    @After
    public void destroyVm() throws IOException {
        vm.stop();
        vm.destroy();
    }

    @Test
    public void testWriteResilience() throws IOException, TimeoutException {
        RemoteJvmProcess process = itJar.runClass(NodeWriter.class.getName(), PROPS);
        process.waitForMessage("go", 600);
        vm.reset();

        JunitProcess junit = itJar.runJunit(NodeWriterTest.class.getName(), PROPS);
        assertTrue(junit.read().wasSuccessful());
    }
}