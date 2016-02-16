/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.resilience.remote;

import static org.apache.jackrabbit.oak.resilience.vagrant.VagrantVM.MQ_FILE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

import org.apache.jackrabbit.oak.resilience.vagrant.VagrantVM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProducer {

    private static final Logger LOG = LoggerFactory.getLogger(MessageProducer.class);

    public static void write(String message) {
        File file = new File(System.getProperty(MQ_FILE));
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(new FileOutputStream(file, true));
            writer.println(message);
            writer.flush();
        } catch (FileNotFoundException e) {
            LOG.error("Can't write message", e);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }
}
