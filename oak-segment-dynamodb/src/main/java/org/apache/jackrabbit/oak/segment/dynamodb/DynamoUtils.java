package org.apache.jackrabbit.oak.segment.dynamodb;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public final class DynamoUtils {

    private static final Logger log = LoggerFactory.getLogger(DynamoUtils.class);

    private DynamoUtils() {
    }

    public static boolean tableExists(DynamoDB dynamoDB, String tableName) {
        try {
            dynamoDB.getTable(tableName).describe();
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        } catch (AmazonDynamoDBException e) {
            log.error("Can't check if the table exists", e);
            return false;
        }
    }

    public static UUID getUUID(Item segmentItem) {
        return new UUID(segmentItem.getLong("msb"), segmentItem.getLong("lsb"));
    }
}
