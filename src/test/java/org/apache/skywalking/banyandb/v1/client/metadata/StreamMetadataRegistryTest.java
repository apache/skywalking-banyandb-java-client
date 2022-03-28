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

import io.grpc.stub.StreamObserver;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.database.v1.StreamRegistryServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;
import org.junit.Assert;
import org.junit.Before;
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
public class StreamMetadataRegistryTest extends BanyanDBMetadataRegistryTest {
    // play as an in-memory registry
    private Map<String, BanyandbDatabase.Stream> streamRegistry;

    private final StreamRegistryServiceGrpc.StreamRegistryServiceImplBase streamRegistryServiceImpl =
            mock(StreamRegistryServiceGrpc.StreamRegistryServiceImplBase.class, delegatesTo(
                    new StreamRegistryServiceGrpc.StreamRegistryServiceImplBase() {
                        @Override
                        public void create(BanyandbDatabase.StreamRegistryServiceCreateRequest request, StreamObserver<BanyandbDatabase.StreamRegistryServiceCreateResponse> responseObserver) {
                            BanyandbDatabase.Stream s = request.getStream().toBuilder()
                                    .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            streamRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbDatabase.StreamRegistryServiceCreateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void update(BanyandbDatabase.StreamRegistryServiceUpdateRequest request, StreamObserver<BanyandbDatabase.StreamRegistryServiceUpdateResponse> responseObserver) {
                            BanyandbDatabase.Stream s = request.getStream().toBuilder()
                                    .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            streamRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbDatabase.StreamRegistryServiceUpdateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void delete(BanyandbDatabase.StreamRegistryServiceDeleteRequest request, StreamObserver<BanyandbDatabase.StreamRegistryServiceDeleteResponse> responseObserver) {
                            BanyandbDatabase.Stream oldStream = streamRegistry.remove(request.getMetadata().getName());
                            responseObserver.onNext(BanyandbDatabase.StreamRegistryServiceDeleteResponse.newBuilder()
                                    .setDeleted(oldStream != null)
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void get(BanyandbDatabase.StreamRegistryServiceGetRequest request, StreamObserver<BanyandbDatabase.StreamRegistryServiceGetResponse> responseObserver) {
                            responseObserver.onNext(BanyandbDatabase.StreamRegistryServiceGetResponse.newBuilder()
                                    .setStream(streamRegistry.get(request.getMetadata().getName()))
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void list(BanyandbDatabase.StreamRegistryServiceListRequest request, StreamObserver<BanyandbDatabase.StreamRegistryServiceListResponse> responseObserver) {
                            responseObserver.onNext(BanyandbDatabase.StreamRegistryServiceListResponse.newBuilder()
                                    .addAllStream(streamRegistry.values())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    }));

    @Before
    public void setUp() throws IOException {
        streamRegistry = new HashMap<>();
        setUp(streamRegistryServiceImpl);
    }

    @Test
    public void testStreamRegistry_createAndGet() {
        Stream expectedStream = Stream.create("default", "sw")
                .setEntityRelativeTags("service_id", "service_instance_id", "state")
                .addTagFamily(TagFamilySpec.create("data")
                        .addTagSpec(TagFamilySpec.TagSpec.newBinaryTag("data_binary"))
                        .build())
                .addTagFamily(TagFamilySpec.create("searchable")
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("trace_id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newIntTag("state"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_instance_id"))
                        .build())
                .addIndex(IndexRule.create("trace_id", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.GLOBAL))
                .build();
        this.client.define(expectedStream);
        Assert.assertTrue(streamRegistry.containsKey("sw"));
        Stream actualStream = client.findStream("default", "sw");
        Assert.assertNotNull(actualStream);
        Assert.assertEquals(expectedStream, actualStream);
        Assert.assertNotNull(actualStream.updatedAt());
    }

    @Test
    public void testStreamRegistry_createAndList() {
        Stream expectedStream = Stream.create("default", "sw")
                .setEntityRelativeTags("service_id", "service_instance_id", "state")
                .addTagFamily(TagFamilySpec.create("data")
                        .addTagSpec(TagFamilySpec.TagSpec.newBinaryTag("data_binary"))
                        .build())
                .addTagFamily(TagFamilySpec.create("searchable")
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("trace_id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newIntTag("state"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_id"))
                        .build())
                .addIndex(IndexRule.create("trace_id", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.GLOBAL))
                .build();
        client.define(expectedStream);
        List<Stream> actualStreams = new StreamMetadataRegistry(this.channel).list("default");
        Assert.assertNotNull(actualStreams);
        Assert.assertEquals(1, actualStreams.size());
    }

    @Test
    public void testStreamRegistry_createAndDelete() {
        Stream expectedStream = Stream.create("default", "sw")
                .setEntityRelativeTags("service_id", "service_instance_id", "state")
                .addTagFamily(TagFamilySpec.create("data")
                        .addTagSpec(TagFamilySpec.TagSpec.newBinaryTag("data_binary"))
                        .build())
                .addTagFamily(TagFamilySpec.create("searchable")
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("trace_id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newIntTag("state"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_id"))
                        .build())
                .addIndex(IndexRule.create("trace_id", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.GLOBAL))
                .build();
        this.client.define(expectedStream);
        boolean deleted = new StreamMetadataRegistry(this.channel).delete("default", "sw");
        Assert.assertTrue(deleted);
        Assert.assertEquals(0, streamRegistry.size());
    }
}
