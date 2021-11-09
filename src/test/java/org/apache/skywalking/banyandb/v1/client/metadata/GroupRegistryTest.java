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

package org.apache.skywalking.banyandb.v1.client.metadata;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.skywalking.banyandb.database.v1.metadata.BanyandbMetadata;
import org.apache.skywalking.banyandb.database.v1.metadata.GroupRegistryServiceGrpc;
import org.apache.skywalking.banyandb.v1.Banyandb;
import org.apache.skywalking.banyandb.v1.client.BanyanDBClient;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class GroupRegistryTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private GroupRegistry client;

    // play as an in-memory registry
    private Map<String, Banyandb.Group> groupRegistry;

    private final GroupRegistryServiceGrpc.GroupRegistryServiceImplBase serviceImpl =
            mock(GroupRegistryServiceGrpc.GroupRegistryServiceImplBase.class, delegatesTo(
                    new GroupRegistryServiceGrpc.GroupRegistryServiceImplBase() {
                        @Override
                        public void create(BanyandbMetadata.GroupRegistryServiceCreateRequest request, StreamObserver<BanyandbMetadata.GroupRegistryServiceCreateResponse> responseObserver) {
                            Banyandb.Group g = Banyandb.Group.newBuilder()
                                    .setName(request.getGroup())
                                    .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            groupRegistry.put(g.getName(), g);
                            responseObserver.onNext(BanyandbMetadata.GroupRegistryServiceCreateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void delete(BanyandbMetadata.GroupRegistryServiceDeleteRequest request, StreamObserver<BanyandbMetadata.GroupRegistryServiceDeleteResponse> responseObserver) {
                            Banyandb.Group g = groupRegistry.remove(request.getGroup());
                            if (g == null) {
                                responseObserver.onError(Status.NOT_FOUND.withDescription("group does not exist").asRuntimeException());
                            } else {
                                responseObserver.onNext(BanyandbMetadata.GroupRegistryServiceDeleteResponse.newBuilder()
                                        .setDeleted(true)
                                        .build());
                                responseObserver.onCompleted();
                            }
                        }

                        @Override
                        public void exist(BanyandbMetadata.GroupRegistryServiceExistRequest request, StreamObserver<BanyandbMetadata.GroupRegistryServiceExistResponse> responseObserver) {
                            Banyandb.Group g = groupRegistry.get(request.getGroup());
                            if (g == null) {
                                responseObserver.onError(Status.NOT_FOUND.withDescription("group does not exist").asRuntimeException());
                            } else {
                                responseObserver.onNext(BanyandbMetadata.GroupRegistryServiceExistResponse.newBuilder()
                                        .setGroup(g)
                                        .build());
                                responseObserver.onCompleted();
                            }
                        }

                        @Override
                        public void list(BanyandbMetadata.GroupRegistryServiceListRequest request, StreamObserver<BanyandbMetadata.GroupRegistryServiceListResponse> responseObserver) {
                            responseObserver.onNext(BanyandbMetadata.GroupRegistryServiceListResponse.newBuilder()
                                    .addAllGroup(groupRegistry.keySet())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    }));

    @Before
    public void setUp() throws IOException {
        groupRegistry = new HashMap<>();

        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        Server server = InProcessServerBuilder
                .forName(serverName).directExecutor().addService(serviceImpl).build();
        grpcCleanup.register(server.start());

        // Create a client channel and register for automatic graceful shutdown.
        ManagedChannel channel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());
        BanyanDBClient client = new BanyanDBClient("127.0.0.1", server.getPort(), "default");

        client.connect(channel);

        this.client = client.groupRegistry();
    }

    @Test
    public void testGroupRegistry_createAndCheck() {
        this.client.create("default");
        Assert.assertTrue(this.groupRegistry.size() > 0);
        Assert.assertTrue(this.client.exist("default"));
    }

    @Test
    public void testGroupRegistry_CheckNonExist() {
        Assert.assertEquals(0, this.groupRegistry.size());
        Assert.assertFalse(this.client.exist("default"));
    }

    @Test
    public void testGroupRegistry_list() {
        List<String> groups = this.client.list();
        Assert.assertEquals(0, groups.size());
        this.client.create("default");
        groups = this.client.list();
        Assert.assertEquals(1, groups.size());
    }

    @Test
    public void testGroupRegistry_delete() {
        this.client.create("default");
        List<String> groups = this.client.list();
        Assert.assertEquals(1, groups.size());
        Assert.assertTrue(this.client.delete("default"));
        Assert.assertFalse(this.client.exist("default"));
    }

}
