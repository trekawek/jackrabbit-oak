package org.apache.jackrabbit.oak.segment.azure;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.oak.segment.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.file.GcJournalTest;
import org.junit.Before;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class AzureGCJournalTest extends GcJournalTest {

    private CloudBlobContainer container;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        container = AzureContainerFactory.getContainer("oak-test");
    }

    @Override
    protected SegmentNodeStorePersistence getPersistence() throws Exception {
        return new AzurePersistence(container.getDirectoryReference("oak"));
    }

}
