package org.apache.jackrabbit.oak.resilience.vagrant;

public enum MemoryUnit {

    BYTE, KILOBYTE, MEGABYTE, GIGABYTE;

    public long toByte(long size) {
        long result = size;
        for (int i = 0; i < this.ordinal(); i++) {
            result *= 1024;
        }
        return result;
    }
}
