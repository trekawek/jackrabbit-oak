package org.apache.jackrabbit.oak.resilience.remote.operations;

import java.util.ArrayList;
import java.util.List;

public class FillMemoryOperation implements Runnable {

    private static final List<byte[]> CHUNKS = new ArrayList<byte[]>();

    private final int memorySize;

    private final long periodMillis;

    public FillMemoryOperation(int memorySize, long periodMillis) {
        this.memorySize = memorySize;
        this.periodMillis = periodMillis;
    }

    @Override
    public void run() {
        while (true) {
            System.out.println("Adding " + memorySize + " to heap");
            CHUNKS.add(new byte[memorySize]);
            try {
                Thread.sleep(periodMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}