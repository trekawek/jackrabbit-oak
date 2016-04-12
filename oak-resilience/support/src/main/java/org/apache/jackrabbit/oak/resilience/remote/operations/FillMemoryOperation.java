package org.apache.jackrabbit.oak.resilience.remote.operations;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FillMemoryOperation implements Runnable {

    private static final List<Object> CHUNKS = new ArrayList<Object>();

    private final int memorySize;

    private final long periodMillis;

    private final boolean onHeap;

    public FillMemoryOperation(int memorySize, long periodMillis, boolean onHeap) {
        this.memorySize = memorySize;
        this.periodMillis = periodMillis;
        this.onHeap = onHeap;
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (onHeap) {
                    allocateOnHeap();
                } else {
                    allocateOffHeap();
                }
                System.out.println("Free memory left: " + Runtime.getRuntime().freeMemory());
                Thread.sleep(periodMillis);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void allocateOnHeap() {
        System.out.println("Allocating " + memorySize + " on heap");
        CHUNKS.add(ByteBuffer.allocate(memorySize));
    }

    public void allocateOffHeap() {
        System.out.println("Allocating " + memorySize + " off heap");
        CHUNKS.add(ByteBuffer.allocateDirect(memorySize));
    }
}