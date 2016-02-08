package org.apache.jackrabbit.oak.resilience;

import java.io.IOException;

public interface VM {

    void init() throws IOException;

    void start() throws IOException;

    void stop() throws IOException;

    void destroy() throws IOException;

}
