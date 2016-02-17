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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.jackrabbit.oak.resilience.junit.JunitReceiver;
import org.apache.jackrabbit.oak.resilience.remote.RemoteTest;
import org.apache.jackrabbit.oak.resilience.remote.UnitTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class VagrantVMTest {

    private VagrantVM vm;

    @Before
    public void setupVm() throws IOException {
        vm = new VagrantVM.Builder().setVagrantFile("src/test/resources/Vagrantfile").build();
        vm.init();
        vm.start();
    }

    @After
    public void destroyVm() throws IOException {
        vm.stop();
        vm.destroy();
    }

    @Test
    public void test() throws IOException, TimeoutException {
        String jar = vm.copyJar("org.apache.jackrabbit", "oak-resilience-it-remote", "1.4-SNAPSHOT");
        RemoteProcess process = vm.runClass(jar, RemoteTest.class.getName(), null);
        process.waitForMessage("that's fine", 1000);
        process.waitForFinish();

        JunitReceiver junit = vm.runJunit(jar, UnitTest.class.getName(), null);
        Assert.assertTrue(junit.read().wasSuccessful());
    }
}