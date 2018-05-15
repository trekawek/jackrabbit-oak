package org.apache.jackrabbit.oak.kv.osgi;

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.Option;

import static org.apache.jackrabbit.oak.kv.osgi.Configuration.PID;

@ObjectClassDefinition(
        pid = {PID},
        name = "Apache Jackrabbit Oak Azure Segment Store Service",
        description = "Azure backend for the Oak Segment Node Store")
@interface Configuration {

    String PID = "org.apache.jackrabbit.oak.kv.osgi.KVNodeStoreService";

    @AttributeDefinition(
            name = "Store type",
            options = { @Option("leveldb"), @Option("redis") }
    )
    String type() default "leveldb";

    @AttributeDefinition(
            name = "Cache maximum size")
    long cacheMaximumSize() default 50000;

    @AttributeDefinition(
            name = "LevelDB path")
    String path();

    @AttributeDefinition(
            name = "Redis host")
    String redisHost() default "localhost";


    @AttributeDefinition(
            name = "Redis port")
    int redisPort() default 6379;
}