package org.apache.jackrabbit.oak.resilience.vagrant;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.oak.resilience.VM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class VagrantVM implements VM {

    private static final Logger LOG = LoggerFactory.getLogger(VagrantVM.class);

    private final String vagrantExecutable;

    private final File vagrantFile;

    private final File workDir;

    private VagrantVM(Builder builder) {
        if (builder.workDir == null) {
            workDir = Files.createTempDir();
        } else {
            workDir = builder.workDir;
            workDir.mkdirs();
        }
        vagrantFile = builder.vagrantFile;
        vagrantExecutable = builder.vagrantExecutable;
    }

    @Override
    public void init() throws IOException {
        LOG.info("Executable: {}", vagrantExecutable);
        LOG.info("Workdir: {}", workDir);
        if (vagrantFile == null) {
            LOG.info("No Vagrantfile specified.");
            exec(vagrantExecutable, "init", "ubuntu/trusty64");
        } else {
            LOG.info("Vagrantfile: {}", vagrantFile);
            Files.copy(vagrantFile, new File(workDir, "Vagrantfile"));
        }
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
        FileUtils.deleteQuietly(workDir);
    }

    private int exec(String... cmd) throws IOException {
        LOG.info("$ {}", StringUtils.join(cmd, ' '));
        ProcessBuilder builder = new ProcessBuilder(cmd).redirectErrorStream(true).directory(workDir);
        Map<String, String> env = builder.environment();
        env.put("PATH", env.get("PATH") + ":/usr/local/bin");
        Process process = builder.start();

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

    public static class Builder {

        private String vagrantExecutable = "/usr/local/bin/vagrant";

        private File vagrantFile;

        private File workDir;

        public Builder setVagrantExecutable(String vagrantExecutable) {
            this.vagrantExecutable = vagrantExecutable;
            return this;
        }

        public Builder setVagrantFile(String vagrantFile) {
            this.vagrantFile = new File(vagrantFile);
            return this;
        }

        public Builder setWorkDir(String workDir) {
            this.workDir = new File(workDir);
            return this;
        }

        public VagrantVM build() {
            return new VagrantVM(this);
        }
    }
}
