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

import static com.google.common.io.Files.createTempDir;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.apache.commons.io.FileUtils.deleteQuietly;
import static org.apache.commons.lang.StringUtils.join;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.resilience.VM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VagrantVM implements VM {

    private static final Logger LOG = LoggerFactory.getLogger(VagrantVM.class);

    private static final String VAGRANT_PREFIX = "/vagrant/";

    private static final String PATH = "PATH";

    public static final String MQ_FILE = "MQ_FILE";

    private final String vagrantExecutable;

    private final String mavenExecutable;

    private final String extraPath;

    private final File workDir;

    private final File vagrantFile;

    private VagrantVM(Builder builder) throws IOException {
        if (builder.workDir == null) {
            workDir = createTempDir();
        } else {
            workDir = builder.workDir;
            workDir.mkdirs();
        }
        vagrantExecutable = builder.vagrantExecutable;
        mavenExecutable = builder.mavenExecutable;
        extraPath = builder.extraPath;
        vagrantFile = builder.vagrantFile;
        if (vagrantFile == null || !vagrantFile.exists()) {
            throw new IOException("Can't find Vagrantfile: " + vagrantFile);
        }
    }

    @Override
    public void init() throws IOException {
        LOG.info("Executable: {}", vagrantExecutable);
        LOG.info("Workdir: {}", workDir);
        exec(vagrantExecutable, "init", vagrantFile.getAbsolutePath());
    }

    @Override
    public void start() throws IOException {
        exec(vagrantExecutable, "up");
    }

    @Override
    public void stop() throws IOException {
        exec(vagrantExecutable, "halt");
    }

    @Override
    public void destroy() throws IOException {
        stop();
        exec(vagrantExecutable, "destroy", "--force");
        deleteQuietly(workDir);
    }

    @Override
    public void ssh(String... command) throws IOException {
        exec(concat(a(vagrantExecutable, "ssh", "--"), command));
    }

    @Override
    public String copyJar(String groupId, String artifactId, String version) throws IOException {
        String artifact = format("%s:%s:%s", groupId, artifactId, version);
        String outputName = format("%s-%s.jar", artifactId, version);
        exec(mavenExecutable, "dependency:copy", "-Dartifact=" + artifact, "-DoutputDirectory=.");
        return outputName;
    }

    @Override
    public RemoteProcess runClass(String jar, String className, Map<String, String> properties, String... args)
            throws IOException {
        String mqFile = format("%s-%s.txt", className, randomUUID().toString());

        Map<String, String> allProps = new HashMap<String, String>();
        allProps.put(MQ_FILE, VAGRANT_PREFIX + mqFile);
        if (properties != null) {
            allProps.putAll(properties);
        }

        List<String> cmd = new ArrayList<String>();
        cmd.add(vagrantExecutable);
        cmd.addAll(asList("ssh", "--", "java"));
        for (Entry<String, String> e : allProps.entrySet()) {
            cmd.add(String.format("-D%s=%s", e.getKey(), e.getValue()));
        }
        cmd.addAll(asList("-cp", VAGRANT_PREFIX + jar, className));
        cmd.addAll(asList(args));

        Process process = execProcess(cmd.toArray(new String[0]));
        return new RemoteProcess(process, new File(workDir, mqFile));
    }

    @Override
    public RemoteProcess runJunit(String jar, String testClassName, Map<String, String> properties) throws IOException {
        return runClass(jar, "org.junit.runner.JUnitCore", properties, testClassName);
    }

    private Process execProcess(String... cmd) throws IOException {
        LOG.info("$ {}", join(cmd, ' '));
        ProcessBuilder builder = new ProcessBuilder(cmd).redirectErrorStream(true).directory(workDir);
        Map<String, String> env = builder.environment();
        if (extraPath != null) {
            env.put(PATH, env.get(PATH) + ":" + extraPath);
        }
        return builder.start();
    }

    private int exec(String... cmd) throws IOException {
        Process process = execProcess(cmd);
        BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
        try {
            String line;
            while ((line = input.readLine()) != null) {
                LOG.info(line);
            }
        } finally {
            input.close();
        }
        try {
            return process.waitFor();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private static String[] concat(String[]... arrays) {
        int length = 0;
        for (String[] array : arrays) {
            length += array.length;
        }

        String[] result = new String[length];
        int i = 0;
        for (String[] array : arrays) {
            for (String value : array) {
                result[i++] = value;
            }
        }
        return result;
    }

    private static String[] a(String... strings) {
        return strings;
    }

    public static class Builder {

        private String vagrantExecutable = "/usr/local/bin/vagrant";

        private String mavenExecutable = "/usr/local/bin/mvn";

        private String extraPath = "/usr/local/bin";

        private File workDir;

        private File vagrantFile = new File("Vagrantfile");

        public Builder setVagrantExecutable(String vagrantExecutable) {
            this.vagrantExecutable = vagrantExecutable;
            return this;
        }

        public Builder setMavenExecutable(String mavenExecutable) {
            this.mavenExecutable = mavenExecutable;
            return this;
        }

        public Builder setExtraPath(String extraPath) {
            this.extraPath = extraPath;
            return this;
        }

        public Builder setWorkDir(String workDir) {
            this.workDir = new File(workDir);
            return this;
        }

        public Builder setVagrantFile(String vagrantFile) {
            this.vagrantFile = new File(vagrantFile);
            return this;
        }

        public VagrantVM build() throws IOException {
            return new VagrantVM(this);
        }
    }
}
