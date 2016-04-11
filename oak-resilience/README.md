# oak-resilience

Simple framework for testing software resilience using virtualization.

## Introduction

oak-resilience launches a virtual machine controlled by Ubuntu and allows to run Java code (executable classes and JUnit tests) in an easy way. It also exposes an interface which may be used to control the virtual machine: restart it, fill memory, break the network connectivity, etc.

## Installation

Run following command:

    ./install_box.sh

It'll create and install Vagrant box, which will be later cloned to run the tests. After that, run

    mvn clean install

to install modules. Then invoke

    mvn test -P resilience-tests

to run the resilience integration tests.

## Modules

* oak-resilience-it - contains test classes run on the local (supervisor) machine,
* oak-resilience-remote-it - test classes run on the remote (virtual) machine,
* oak-resilience-support - the framework code.

## Sample code

Following code will create a virtual machine, upload the jar containing code to run and then start the `NodeWriter#main()` method. When the method is finished, it'll run unit tests from the `NodeWriterTest` class and check if they were successful.

    VagrantVM vm = new VagrantVM.Builder()
        .setVagrantFile("src/test/resources/Vagrantfile")
        .build();
    vm.init();
    vm.start();

    // jar will be taken from the local Maven repository
    RemoteJar jar = vm.uploadJar("org.apache.jackrabbit", "oak-resilience-it-remote", "1.6-SNAPSHOT");
    RemoteJvmProcess process = jar.runClass(NodeWriter.class.getName(), null);
    process.waitForFinish();

    JunitProcess junit = jar.runJunit(NodeWriterTest.class.getName(), null);
    assert(junit.read().wasSuccessful());

    vm.stop();
    vm.destroy();

More samples can be found in the [oak-resilience-it](/oak-resilience/it/src/test/java/org/apache/jackrabbit/oak/resilience/vagrant) module.

## Passing system properties

Both `RemoteJar#runClass()` and `#runJunit()` accepts a `Map<String, String> properties` parameter. The passed map will be exposed as system properties in the resulting Java process:

    // local code
    Map<String, String> props = new HashMap<>();
    props.put("my-key", "123");
    RemoteJvmProcess process = jar.runClass(RemoteClass.class.getName(), props);

    // RemoteClass
    String value = System.getProperty("my-key");
    
Additionally, `#runClass()` accepts a list of Strings that will be passed to the `main()` method:

    // local code
    RemoteJvmProcess process = jar.runClass(RemoteClass.class.getName(), "123");
    
    // RemoteClass
    public static void main(String[] args) {
        String value = args[0]; // "123"
    }

## Communication with the remote process

Virtual machine runs a [RabbitMQ](https://www.rabbitmq.com/) broker which can be used to communicate with the remote process. Messages are sent from the VM Java process to the local supervisor:

    RemoteMessageProducer.getInstance().publish("my message");

The supervisor code can wait for a particular message for some time or just block until any message appears:

    RemoteJvmProcess process = jar.runClass(RemoteClass.class.getName());
    // wait for 10 sec until specified message arrives
    boolean received = process.waitForMessage("my message", 10);
    
    // wait until there's any message
    String message = process.getMessage();

Other useful methods of the `RemoteJvmProcess` are `#waitForFinish()`, `#isResponding()` and `#kill()`.

## Remote JUnit tests

Besides from the executable classes described above, it's possible to run a JUnit test class, using `RemoteJar#runJunit()` method:

    JunitProcess junit = itJar.runJunit(NodeWriterTest.class.getName(), PROPS);
    org.junit.runner.Result result = junit.read();
    assertTrue(result.wasSuccessful());

The results of the remote tests are displayed on the local standard output in the realtime and the `Result` object returned from the `junit.read();` provides detailed information about the tests.

## Network

oak-resilience integrated the [Toxiproxy](https://github.com/Shopify/toxiproxy) project to provide a simple way to create and manage tunnels from the VM to the supervisor. For instance, it's possible to connect from the remote test to a local Mongo instance:

    VagrantVM vm = ...;
    Proxy proxy = vm.forwardPortToGuest(27017, 3333);

From now on, the remote test may connect to the localhost:3333 and the connection will be forwarded to the supervisor port 27017. The `Proxy` object may be used to break the connection in any time.

## Breaking the virtual machine

The may purpose of this framework is to break the virtualized environment in a different ways and then run tests to check the state of the repository. This section describes which parts of the virtual machine and remote JVM process may be spoilt.

### Restarting the VM

The whole VM may be cold-rebooted using `VagrantVM#reset()` method.

Example: [RestartResilienceTest](/oak-resilience/it/src/test/java/org/apache/jackrabbit/oak/resilience/vagrant/RestartResilienceTest.java).

### Filling the memory

It's possible to fill the JVM heap memory with the `fillMemory()`:

    RemoteJvmProcess process = ...;
    process.fillMemory(1, MemoryUnit.MEGABYTE, 100, TimeUnit.MILLISECONDS);

It'll allocate 1 MB each 0.1s until an OutOfMemory exception is thrown.

Example: [MemoryFullResilienceTest](/oak-resilience/it/src/test/java/org/apache/jackrabbit/oak/resilience/vagrant/MemoryFullResilienceTest.java).

### Filling the disk

The VM partition may be filled as well:

    VagrantVM vm = ...;
    // leave only 100 MB of the free disk space
    vm.fillDiskUntil(100, MemoryUnit.MEGABYTE);
    // ...
    vm.cleanupDisk(); // restore free disk space

Example: [DiskFullResilienceTest](/oak-resilience/it/src/test/java/org/apache/jackrabbit/oak/resilience/vagrant/DiskFullResilienceTest.java).

### Breaking the network connectivity

The `Proxy` object returned from the `VagrantVM#forwardPortToGuest()` may be used to spoil the network communication:

    Proxy mongoProxy = vm.forwardPortToGuest(27017, 3333); 
    mongoProxy.downstream().latency().enabled().setLatency(100).setJitter(15);

For more info read the [Toxiproxy](https://github.com/shopify/toxiproxy#toxics) documentation.

Example: [NetworkDownResilienceTest](/oak-resilience/it/src/test/java/org/apache/jackrabbit/oak/resilience/vagrant/NetworkDownResilienceTest.java).
