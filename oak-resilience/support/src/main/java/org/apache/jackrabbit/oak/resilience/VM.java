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
package org.apache.jackrabbit.oak.resilience;

import java.io.IOException;

import org.apache.jackrabbit.oak.resilience.vagrant.RemoteProcess;

public interface VM {

    void init() throws IOException;

    void start() throws IOException;

    void stop() throws IOException;

    void destroy() throws IOException;

    void ssh(String... cmd) throws IOException;

    String copyJar(String groupId, String artifactId, String version) throws IOException;

    RemoteProcess runClass(String jar, String className, String... args) throws IOException;

    RemoteProcess runJunit(String jar, String testClassName) throws IOException;
}
