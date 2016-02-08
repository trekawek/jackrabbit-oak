package org.apache.jackrabbit.oak.resilience.vagrant;

import java.io.IOException;

import org.apache.jackrabbit.oak.resilience.VM;
import org.junit.Test;

public class VagrantVMTest {

    @Test
    public void test() throws IOException {
        VM vm = new VagrantVM.Builder().build();
        vm.init();
        vm.start();
        vm.stop();
        vm.destroy();
    }

}
