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
import static java.util.UUID.randomUUID;
import static org.apache.commons.io.FileUtils.deleteQuietly;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.jackrabbit.oak.resilience.vagrant.MemoryUnit.KILOBYTE;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Proxy;

public class VagrantVM {

    private static final Logger LOG = LoggerFactory.getLogger(VagrantVM.class);

    private static final String VAGRANT_PREFIX = "/vagrant/";

    private static final String PATH = "PATH";

    public static final String MQ_ID = "MQ_ID";

    private static final Pattern PORT_PATTERN = Pattern.compile("^ *(\\d+) \\(guest\\) => (\\d+) \\(host\\)$");

    final String vagrantExecutable;

    private final String mavenExecutable;

    private final String extraPath;

    private final File vagrantFile;

    private final Map<Integer, Integer> ports;

    private File workDir;

    private Connection connection;

    Channel channel;

    private VagrantVM(Builder builder) throws IOException {
        vagrantExecutable = builder.vagrantExecutable;
        mavenExecutable = builder.mavenExecutable;
        extraPath = builder.extraPath;
        vagrantFile = builder.vagrantFile;
        ports = new HashMap<Integer, Integer>();
    }

    public void init() throws IOException {
        workDir = createTempDir();

        if (vagrantFile == null || !vagrantFile.exists()) {
            throw new IOException("Can't find Vagrantfile: " + vagrantFile);
        }
        Files.copy(vagrantFile, new File(workDir, "Vagrantfile"));

        LOG.info("Executable: {}", vagrantExecutable);
        LOG.info("Workdir: {}", workDir);
        LOG.info("Vagrantfile: {}", vagrantFile);
    }

    public void start() throws IOException {
        exec(vagrantExecutable, "up");
        discoverPorts();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(ports.get(5672));

        try {
            connection = factory.newConnection();
        } catch (TimeoutException e) {
            throw new IOException(e);
        }
        channel = connection.createChannel();
    }

    private void discoverPorts() throws IOException {
        ports.clear();

        Process process = execProcess(vagrantExecutable, "port");
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            Matcher m = PORT_PATTERN.matcher(line);
            if (m.matches()) {
                String guest = m.group(1);
                String host = m.group(2);
                ports.put(Integer.valueOf(guest), Integer.valueOf(host));
            }
        }
    }

    public Integer getHostPort(int guestPort) {
        return ports.get(guestPort);
    }

    public void stop() throws IOException {
        stop(false);
    }

    private void stop(boolean force) throws IOException {
        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (TimeoutException e) {
            throw new IOException(e);
        }
        if (connection.isOpen()) {
            connection.close();
        }
        if (force) {
            exec(vagrantExecutable, "halt", "--force");
        } else {
            exec(vagrantExecutable, "halt");
        }
    }

    public void destroy() throws IOException {
        exec(vagrantExecutable, "destroy", "--force");
        deleteQuietly(workDir);
    }

    public void reset() throws IOException {
        stop(true);
        start();
    }

    private int ssh(String... args) throws IOException {
        List<String> cmd = new ArrayList<String>();
        cmd.addAll(Arrays.asList(vagrantExecutable, "ssh", "--"));
        cmd.addAll(Arrays.asList(args));
        return exec(cmd.toArray(new String[0]));
    }

    public long freeDiskSpaceKb() throws IOException {
        Process process = execProcess(vagrantExecutable, "ssh", "--", "df", "/");
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        reader.readLine();
        List<String> output = Lists.newArrayList(Splitter.on(' ').omitEmptyStrings().split(reader.readLine()));
        return Long.parseLong(output.get(3));
    }

    public void fillDiskUntil(long freeSpaceToLeave, MemoryUnit unit) throws IOException {
        ssh("mkdir", "-p", "/tmp/zeroes");

        long currentFreeSpace = freeDiskSpaceKb();
        long toOccupyKb = currentFreeSpace - unit.toKilobyte(freeSpaceToLeave);
        if (toOccupyKb < 0) {
            return;
        }
        ssh("dd", "if=/dev/zero", "of=/tmp/zeroes/" + randomUUID().toString(), "bs=1M",
                "count=" + KILOBYTE.toMegabyte(toOccupyKb));

        currentFreeSpace = freeDiskSpaceKb();
        toOccupyKb = currentFreeSpace - unit.toKilobyte(freeSpaceToLeave);
        if (toOccupyKb < 0) {
            return;
        }
        ssh("dd", "if=/dev/zero", "of=/tmp/zeroes/" + randomUUID().toString(), "bs=1K", "count=" + toOccupyKb);
    }

    public void cleanupDisk() throws IOException {
        ssh("rm", "-rf", "/tmp/zeroes");
    }

    public RemoteJar uploadJar(String groupId, String artifactId, String version) throws IOException {
        String artifact = format("%s:%s:%s", groupId, artifactId, version);
        String outputName = format("%s-%s.jar", artifactId, version);
        exec(mavenExecutable, "dependency:copy", "-Dartifact=" + artifact, "-DoutputDirectory=.");
        return new RemoteJar(this, VAGRANT_PREFIX + outputName);
    }

    public Proxy forwardPortToGuest(int upstreamHostPort, int listenGuestPort) throws IOException {
        String hostAddr = getLocalIp().getHostAddress();
        String name = String.format("proxy_%d_%d", upstreamHostPort, listenGuestPort);
        ToxiproxyClient client = new ToxiproxyClient("localhost", getHostPort(8474));
        return client.createProxy(name, "localhost:" + listenGuestPort, hostAddr + ":" + upstreamHostPort);
    }

    private InetAddress getLocalIp() throws IOException {
        InetAddress local = InetAddress.getLocalHost();
        if (isValidAddress(local)) {
            return local;
        }
        InetAddress[] addrs = InetAddress.getAllByName(local.getCanonicalHostName());
        if (addrs != null) {
            for (InetAddress addr : addrs) {
                if (isValidAddress(addr)) {
                    return addr;
                }
            }
        }
        for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements(); ) {
            NetworkInterface networkInterface = en.nextElement();
            for (Enumeration<InetAddress> intfAddrs = networkInterface.getInetAddresses(); intfAddrs.hasMoreElements(); ) {
                InetAddress addr = intfAddrs.nextElement();
                if (isValidAddress(addr)) {
                    return addr;
                }
            }
        }
        throw new IOException("Can't find local address different than 127.0.0.1");
    }

    private boolean isValidAddress(InetAddress addr) {
        if (addr.isLoopbackAddress()) {
            return false;
        }
        return addr instanceof Inet4Address;
    }

    Process execProcess(String... cmd) throws IOException {
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

    public static class Builder {

        private String vagrantExecutable = "/usr/local/bin/vagrant";

        private String mavenExecutable = "/usr/local/bin/mvn";

        private String extraPath = "/usr/local/bin";

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

        public Builder setVagrantFile(String vagrantFile) {
            this.vagrantFile = new File(vagrantFile);
            return this;
        }

        public VagrantVM build() throws IOException {
            return new VagrantVM(this);
        }
    }
}
