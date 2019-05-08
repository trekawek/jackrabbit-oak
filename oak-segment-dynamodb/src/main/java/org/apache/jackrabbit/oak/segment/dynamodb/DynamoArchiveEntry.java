package org.apache.jackrabbit.oak.segment.dynamodb;

import com.amazonaws.services.dynamodbv2.document.Item;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;

import java.util.UUID;

public class DynamoArchiveEntry implements SegmentArchiveEntry {

    private final Item item;

    private final UUID uuid;

    public DynamoArchiveEntry(Item item) {
        this.item = item;
        this.uuid = UUID.fromString(item.getString("uuid"));
    }

    @Override
    public long getMsb() {
        return uuid.getMostSignificantBits();
    }

    @Override
    public long getLsb() {
        return uuid.getLeastSignificantBits();
    }

    @Override
    public int getLength() {
        return item.getInt("length");
    }

    @Override
    public int getGeneration() {
        return item.getInt("generation");
    }

    @Override
    public int getFullGeneration() {
        return item.getInt("fullGeneration");
    }

    @Override
    public boolean isCompacted() {
        return item.getBoolean("isCompacted");
    }

    Item getItem() {
        return item;
    }

    UUID getUUID() {
        return uuid;
    }
}
