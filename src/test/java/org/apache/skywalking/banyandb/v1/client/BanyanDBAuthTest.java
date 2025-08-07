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

import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.UnauthenticatedException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertThrows;

public class BanyanDBAuthTest {
    private static final String REGISTRY = "ghcr.io";
    private static final String IMAGE_NAME = "apache/skywalking-banyandb";
    private static final String TAG = "42ec9df7457868926eb80157b36355d94fcd6bba";

    private static final String IMAGE = REGISTRY + "/" + IMAGE_NAME + ":" + TAG;
    private static final String AUTH = Base64.getEncoder().encodeToString("admin:123456".getBytes());

    protected static final int GRPC_PORT = 17912;
    protected static final int HTTP_PORT = 17913;

    @Rule
    public GenericContainer<?> banyanDB = new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("config.yaml"),
                    "/tmp/bydb_server_config.yaml"
            )
            .withCommand("standalone",
                    "--auth-config-file", "/tmp/bydb_server_config.yaml",
                    "--enable-health-auth", "true"
            )
            .withExposedPorts(GRPC_PORT, HTTP_PORT)
            .waitingFor(Wait.forHttp("/api/healthz")
                    .forPort(HTTP_PORT)
                    .withHeader("Authorization", "Basic " + AUTH)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofSeconds(1000)));

    @Test
    public void testAuthWithCorrect() throws IOException {
        BanyanDBClient client = createClient("admin", "123456");
        client.connect();
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            // list all groups
            List<BanyandbCommon.Group> groupList = client.findGroups();
            Assert.assertEquals(1, groupList.size());
            Assert.assertEquals("_monitoring", groupList.get(0).getMetadata().getName());
        });
        client.close();
    }

    @Test
    public void testAuthWithWrong() throws IOException {
        BanyanDBClient client = createClient("admin", "123456" + "wrong");
        client.connect();
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThrows(UnauthenticatedException.class, client::getAPIVersion);
        });
        client.close();
    }

    private BanyanDBClient createClient(String username, String password) {
        Options options = new Options();
        options.setUsername(username);
        options.setPassword(password);
        String url = String.format("%s:%d", banyanDB.getHost(), banyanDB.getMappedPort(GRPC_PORT));
        return new BanyanDBClient(new String[]{url}, options);
    }
}