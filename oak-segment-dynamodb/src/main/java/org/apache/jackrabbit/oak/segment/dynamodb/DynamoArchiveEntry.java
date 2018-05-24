package org.apache.jackrabbit.oak.segment.dynamodb;

import com.amazonaws.services.dynamodbv2.document.Item;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;

public class DynamoArchiveEntry implements SegmentArchiveEntry {

    private final Item item;

    public DynamoArchiveEntry(Item item) {
        this.item = item;
    }

    @Override
    public long getMsb() {
        return item.getLong("msb");
    }

    @Override
    public long getLsb() {
        return item.getLong("lsb");
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
}
