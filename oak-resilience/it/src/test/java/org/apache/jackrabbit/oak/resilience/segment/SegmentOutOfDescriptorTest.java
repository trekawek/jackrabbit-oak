package org.apache.jackrabbit.oak.resilience.segment;

import org.apache.jackrabbit.oak.resilience.junit.JunitProcess;
import org.apache.jackrabbit.oak.resilience.remote.segment.TreeNodeWriter;
import org.apache.jackrabbit.oak.resilience.remote.segment.TreeNodeWriterTest;
import org.apache.jackrabbit.oak.resilience.vagrant.RemoteJar;
import org.apache.jackrabbit.oak.resilience.vagrant.RemoteJvmProcess;
import org.apache.jackrabbit.oak.resilience.vagrant.VagrantVM;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SegmentOutOfDescriptorTest {
    private static final Map<String, String> PROPS = Collections.singletonMap("OAK_DIR",
            "/home/vagrant/" + SegmentOutOfDescriptorTest.class.getName());

    private VagrantVM vm;

    private RemoteJar itJar;

    @Before
    public void setupVm() throws IOException {
        vm = new VagrantVM.Builder().setVagrantFile("src/test/resources/Vagrantfile").build();
        vm.init();
        vm.start();
        itJar = vm.uploadJar("org.apache.jackrabbit", "oak-resilience-it-remote", "1.6-SNAPSHOT");
    }

    @After
    public void destroyVm() throws IOException {
        vm.stop();
        vm.destroy();
    }

    @Test
    public void testDescriptorExhaustion() throws IOException, TimeoutException, InterruptedException {
        RemoteJvmProcess process = itJar.runClass(TreeNodeWriter.class.getName(), PROPS, "5000000");
        process.waitForMessage("go", 600);
        process.setDescriptorLimit(1);
        process.exhaustDescriptors();
        process.waitForFinish();

        JunitProcess junit = itJar.runJunit(TreeNodeWriterTest.class.getName(), PROPS);
        assertTrue(junit.read().wasSuccessful());
    }

}
