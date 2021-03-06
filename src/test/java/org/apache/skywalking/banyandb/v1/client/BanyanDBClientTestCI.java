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
 *
 */

package org.apache.skywalking.banyandb.v1.client;

import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

@Slf4j
public class BanyanDBClientTestCI {
    private static final String REGISTRY = "ghcr.io";
    private static final String IMAGE_NAME = "apache/skywalking-banyandb";
    private static final String TAG = "081c6b5f8b9d37489b54ae63a016ee8218f71cc9";

    private static final String IMAGE = REGISTRY + "/" + IMAGE_NAME + ":" + TAG;

    protected static final int BANYANDB_PORT = 17912;

    @Rule
    public GenericContainer<?> banyanDB = new GenericContainer<>(
            DockerImageName.parse(IMAGE))
            .withCommand("standalone", "--stream-root-path", "/tmp/banyandb-stream-data",
                    "--measure-root-path", "/tmp/banyand-measure-data")
            .withExposedPorts(BANYANDB_PORT)
            .waitingFor(
                    Wait.forLogMessage(".*Listening to\\*\\*\\*\\* addr::17912 module:LIAISON-GRPC\\n", 1)
            );

    protected BanyanDBClient client;

    protected void setUpConnection() throws IOException {
        log.info("create BanyanDB client and try to connect");
        client = new BanyanDBClient(banyanDB.getHost(), banyanDB.getMappedPort(BANYANDB_PORT));
        client.connect();
    }

    protected void closeClient() throws IOException {
        if (this.client != null) {
            this.client.close();
        }
    }
}
