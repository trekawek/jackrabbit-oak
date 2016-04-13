package org.apache.jackrabbit.oak.resilience.vagrant;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.apache.jackrabbit.oak.resilience.remote.junit.JunitClassWrapper.MQ_TEST_ID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.resilience.junit.JunitProcess;
import org.apache.jackrabbit.oak.resilience.remote.MainClassWrapper;
import org.apache.jackrabbit.oak.resilience.remote.junit.JunitClassWrapper;

public class RemoteJar {

    private final VagrantVM vm;

    private final String jarPath;

    public RemoteJar(VagrantVM vm, String jarPath) {
        this.vm = vm;
        this.jarPath = jarPath;
    }

    public RemoteJvmProcess runClass(String className, String... args) throws IOException {
        return runClass(className, null, args);
    }

    public RemoteJvmProcess runClass(String className, Map<String, String> properties, String... args)
            throws IOException {
        String mqId = format("%s-%s", className, randomUUID().toString());

        Map<String, String> allProps = new HashMap<String, String>();
        allProps.put(VagrantVM.MQ_ID, mqId);
        if (properties != null) {
            allProps.putAll(properties);
        }

        List<String> cmd = new ArrayList<String>();
        cmd.add(vm.vagrantExecutable);
        cmd.addAll(asList("ssh", "--", "java"));
        for (Entry<String, String> e : allProps.entrySet()) {
            cmd.add(String.format("-D%s=%s", e.getKey(), e.getValue()));
        }
        cmd.addAll(asList("-cp", jarPath, MainClassWrapper.class.getName(), className));
        cmd.addAll(asList(args));

        Process process = vm.execProcess(cmd.toArray(new String[0]));
        return new RemoteJvmProcess(process, vm.channel, mqId, vm);
    }

    public JunitProcess runJunit(String testClassName) throws IOException {
        return runJunit(testClassName);
    }

    public JunitProcess runJunit(String testClassName, Map<String, String> properties) throws IOException {
        String mqTestId = format("%s-%s", testClassName, randomUUID().toString());
        Map<String, String> allProps = new HashMap<String, String>();
        allProps.put(MQ_TEST_ID, mqTestId);
        if (properties != null) {
            allProps.putAll(properties);
        }
        RemoteJvmProcess process = runClass(JunitClassWrapper.class.getName(), allProps, testClassName);
        return new JunitProcess(process, vm.channel, mqTestId);
    }

}
