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
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.skywalking.banyandb.database.v1.metadata.BanyandbMetadata;
import org.apache.skywalking.banyandb.database.v1.metadata.StreamRegistryServiceGrpc;
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
public class StreamMetadataRegistryTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private StreamMetadataRegistry client;

    // play as an in-memory registry
    private Map<String, BanyandbMetadata.Stream> streamRegistry;

    private final StreamRegistryServiceGrpc.StreamRegistryServiceImplBase serviceImpl =
            mock(StreamRegistryServiceGrpc.StreamRegistryServiceImplBase.class, delegatesTo(
                    new StreamRegistryServiceGrpc.StreamRegistryServiceImplBase() {
                        @Override
                        public void create(BanyandbMetadata.StreamRegistryServiceCreateRequest request, StreamObserver<BanyandbMetadata.StreamRegistryServiceCreateResponse> responseObserver) {
                            BanyandbMetadata.Stream s = request.getStream().toBuilder()
                                    .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            streamRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbMetadata.StreamRegistryServiceCreateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void update(BanyandbMetadata.StreamRegistryServiceUpdateRequest request, StreamObserver<BanyandbMetadata.StreamRegistryServiceUpdateResponse> responseObserver) {
                            BanyandbMetadata.Stream s = request.getStream().toBuilder()
                                    .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            streamRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbMetadata.StreamRegistryServiceUpdateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void delete(BanyandbMetadata.StreamRegistryServiceDeleteRequest request, StreamObserver<BanyandbMetadata.StreamRegistryServiceDeleteResponse> responseObserver) {
                            BanyandbMetadata.Stream oldStream = streamRegistry.remove(request.getMetadata().getName());
                            responseObserver.onNext(BanyandbMetadata.StreamRegistryServiceDeleteResponse.newBuilder()
                                    .setDeleted(oldStream != null)
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void get(BanyandbMetadata.StreamRegistryServiceGetRequest request, StreamObserver<BanyandbMetadata.StreamRegistryServiceGetResponse> responseObserver) {
                            responseObserver.onNext(BanyandbMetadata.StreamRegistryServiceGetResponse.newBuilder()
                                    .setStream(streamRegistry.get(request.getMetadata().getName()))
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void list(BanyandbMetadata.StreamRegistryServiceListRequest request, StreamObserver<BanyandbMetadata.StreamRegistryServiceListResponse> responseObserver) {
                            responseObserver.onNext(BanyandbMetadata.StreamRegistryServiceListResponse.newBuilder()
                                    .addAllStream(streamRegistry.values())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    }));

    @Before
    public void setUp() throws IOException {
        streamRegistry = new HashMap<>();

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

        this.client = client.streamRegistry();
    }

    @Test
    public void testStreamRegistry_create() {
        Stream s = new Stream("sw", 2, Duration.ofDays(30));
        this.client.create(s);
        Assert.assertEquals(streamRegistry.size(), 1);
    }

    @Test
    public void testStreamRegistry_createAndGet() {
        Stream s = new Stream("sw", 2, Duration.ofDays(30));
        s.addTagNameAsEntity("service_id").addTagNameAsEntity("service_instance_id").addTagNameAsEntity("state");
        // data
        TagFamilySpec dataFamily = new TagFamilySpec("data");
        dataFamily.addTagSpec(TagFamilySpec.TagSpec.newBinaryTag("data_binary"));
        s.addTagFamilySpec(dataFamily);
        // searchable
        TagFamilySpec searchableFamily = new TagFamilySpec("searchable");
        searchableFamily.addTagSpec(TagFamilySpec.TagSpec.newStringTag("trace_id"))
                .addTagSpec(TagFamilySpec.TagSpec.newIntTag("state"))
                .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_id"));
        s.addTagFamilySpec(searchableFamily);
        this.client.create(s);
        Stream getStream = this.client.get("sw");
        Assert.assertNotNull(getStream);
        Assert.assertEquals(s, getStream);
        Assert.assertNotNull(getStream.getUpdatedAt());
    }

    @Test
    public void testStreamRegistry_createAndList() {
        Stream s = new Stream("sw", 2, Duration.ofDays(30));
        s.addTagNameAsEntity("service_id").addTagNameAsEntity("service_instance_id").addTagNameAsEntity("state");
        // data
        TagFamilySpec dataFamily = new TagFamilySpec("data");
        dataFamily.addTagSpec(TagFamilySpec.TagSpec.newBinaryTag("data_binary"));
        s.addTagFamilySpec(dataFamily);
        // searchable
        TagFamilySpec searchableFamily = new TagFamilySpec("searchable");
        searchableFamily.addTagSpec(TagFamilySpec.TagSpec.newStringTag("trace_id"))
                .addTagSpec(TagFamilySpec.TagSpec.newIntTag("state"))
                .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_id"));
        s.addTagFamilySpec(searchableFamily);
        this.client.create(s);
        List<Stream> listStream = this.client.list();
        Assert.assertNotNull(listStream);
        Assert.assertEquals(1, listStream.size());
        Assert.assertEquals(listStream.get(0), s);
    }

    @Test
    public void testStreamRegistry_createAndDelete() {
        Stream s = new Stream("sw", 2, Duration.ofDays(30));
        s.addTagNameAsEntity("service_id").addTagNameAsEntity("service_instance_id").addTagNameAsEntity("state");
        // data
        TagFamilySpec dataFamily = new TagFamilySpec("data");
        dataFamily.addTagSpec(TagFamilySpec.TagSpec.newBinaryTag("data_binary"));
        s.addTagFamilySpec(dataFamily);
        // searchable
        TagFamilySpec searchableFamily = new TagFamilySpec("searchable");
        searchableFamily.addTagSpec(TagFamilySpec.TagSpec.newStringTag("trace_id"))
                .addTagSpec(TagFamilySpec.TagSpec.newIntTag("state"))
                .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_id"));
        s.addTagFamilySpec(searchableFamily);
        this.client.create(s);
        boolean deleted = this.client.delete("sw");
        Assert.assertTrue(deleted);
        Assert.assertEquals(0, streamRegistry.size());
    }
}
