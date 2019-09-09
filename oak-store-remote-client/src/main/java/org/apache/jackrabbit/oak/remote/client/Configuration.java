package org.apache.jackrabbit.oak.remote.client;

import org.apache.jackrabbit.oak.segment.azure.AzureSegmentStoreService;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import static org.apache.jackrabbit.oak.remote.client.Configuration.PID;

@ObjectClassDefinition(
        pid = {PID},
        name = "Apache Jackrabbit Oak Azure Segment Store Service",
        description = "Azure backend for the Oak Segment Node Store")
@interface Configuration {

    String PID = "org.apache.jackrabbit.oak.remote.client.RemoteNodeStoreService";

    @AttributeDefinition(
            name = "Azure account name",
            description = "Name of the Azure Storage account to use.")
    String accountName();

    @AttributeDefinition(
            name = "Azure container name",
            description = "Name of the container to use. If it doesn't exists, it'll be created.")
    String containerName() default AzureSegmentStoreService.DEFAULT_CONTAINER_NAME;

    @AttributeDefinition(
            name = "Azure account access key",
            description = "Access key which should be used to authenticate on the account")
    String accessKey();

    @AttributeDefinition(
            name = "Root path",
            description = "Names of all the created blobs will be prefixed with this path")
    String rootPath() default AzureSegmentStoreService.DEFAULT_ROOT_PATH;

    @AttributeDefinition(
            name = "Azure connection string (optional)",
            description = "Connection string to be used to connect to the Azure Storage. " +
                    "Setting it will override the accountName and accessKey properties.")
    String connectionURL() default "";

    @AttributeDefinition(
            name = "Remote server host",
            description = "The host name of the remote server")
    String remoteHost() default "localhost";

    @AttributeDefinition(
            name = "Remote server port",
            description = "The port number of the remote server")
    int remotePort() default 12300;

    @AttributeDefinition(
            name = "NodeStoreProvider role",
            description = "Property indicating that this component will not register as a NodeStore but as a NodeStoreProvider with given role")
    String role();
}