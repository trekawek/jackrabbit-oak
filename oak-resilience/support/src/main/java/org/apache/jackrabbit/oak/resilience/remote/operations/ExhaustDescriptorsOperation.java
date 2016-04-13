package org.apache.jackrabbit.oak.resilience.remote.operations;

import com.google.common.io.Files;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExhaustDescriptorsOperation implements Runnable {

    private static final List<FileOutputStream> STREAMS = new ArrayList<FileOutputStream>();

    @Override
    public void run() {
        try {
            File tmp = File.createTempFile("oak-resilience.", ".descriptor-exhaust");
            while (true) {
                FileOutputStream fos = new FileOutputStream(tmp);
                fos.write(0);
                STREAMS.add(fos);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
