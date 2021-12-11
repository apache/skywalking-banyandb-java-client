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

import com.google.protobuf.NullValue;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.util.MutableHandlerRegistry;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.database.v1.GroupRegistryServiceGrpc;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;
import org.apache.skywalking.banyandb.stream.v1.StreamServiceGrpc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.powermock.api.mockito.PowerMockito.mock;

public class BanyanDBClientStreamWriteTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private final GroupRegistryServiceGrpc.GroupRegistryServiceImplBase groupRegistryServiceImpl =
            mock(GroupRegistryServiceGrpc.GroupRegistryServiceImplBase.class, delegatesTo(
                    new GroupRegistryServiceGrpc.GroupRegistryServiceImplBase() {
                        @Override
                        public void get(BanyandbDatabase.GroupRegistryServiceGetRequest request, StreamObserver<BanyandbDatabase.GroupRegistryServiceGetResponse> responseObserver) {
                            responseObserver.onNext(BanyandbDatabase.GroupRegistryServiceGetResponse.newBuilder()
                                    .setGroup(BanyandbCommon.Group.newBuilder()
                                            .setMetadata(BanyandbCommon.Metadata.newBuilder().setName("default").build())
                                            .setCatalog(BanyandbCommon.Catalog.CATALOG_STREAM)
                                            .setResourceOpts(BanyandbCommon.ResourceOpts.newBuilder()
                                                    .setShardNum(2)
                                                    .build())
                                            .build())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    }));

    private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();

    private BanyanDBClient client;
    private StreamBulkWriteProcessor streamBulkWriteProcessor;

    @Before
    public void setUp() throws IOException {
        String serverName = InProcessServerBuilder.generateName();

        Server server = InProcessServerBuilder
                .forName(serverName).fallbackHandlerRegistry(serviceRegistry).directExecutor()
                .addService(groupRegistryServiceImpl).build();
        grpcCleanup.register(server.start());

        ManagedChannel channel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());

        client = new BanyanDBClient("127.0.0.1", server.getPort());
        client.connect(channel);
        streamBulkWriteProcessor = client.buildStreamWriteProcessor(1000, 1, 1);
    }

    @After
    public void shutdown() throws IOException {
        streamBulkWriteProcessor.close();
    }

    @Test
    public void testWrite() throws Exception {
        final CountDownLatch allRequestsDelivered = new CountDownLatch(1);
        final List<BanyandbStream.WriteRequest> writeRequestDelivered = new ArrayList<>();

        // implement the fake service
        final StreamServiceGrpc.StreamServiceImplBase serviceImpl =
                new StreamServiceGrpc.StreamServiceImplBase() {
                    @Override
                    public StreamObserver<BanyandbStream.WriteRequest> write(StreamObserver<BanyandbStream.WriteResponse> responseObserver) {
                        return new StreamObserver<BanyandbStream.WriteRequest>() {
                            @Override
                            public void onNext(BanyandbStream.WriteRequest value) {
                                writeRequestDelivered.add(value);
                                responseObserver.onNext(BanyandbStream.WriteResponse.newBuilder().build());
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                                responseObserver.onCompleted();
                                allRequestsDelivered.countDown();
                            }
                        };
                    }
                };
        serviceRegistry.addService(serviceImpl);

        String segmentId = "1231.dfd.123123ssf";
        String traceId = "trace_id-xxfff.111323";
        String serviceId = "webapp_id";
        String serviceInstanceId = "10.0.0.1_id";
        String endpointId = "home_id";
        int latency = 200;
        int state = 1;
        Instant now = Instant.now();
        byte[] byteData = new byte[]{14};
        String broker = "172.16.10.129:9092";
        String topic = "topic_1";
        String queue = "queue_2";
        String httpStatusCode = "200";
        String dbType = "SQL";
        String dbInstance = "127.0.0.1:3306";

        StreamWrite streamWrite = new StreamWrite("default", "sw", segmentId, now.toEpochMilli(), 1, 13)
                .dataTag(0, Value.binaryTagValue(byteData))
                .searchableTag(0, Value.stringTagValue(traceId)) // 0
                .searchableTag(1, Value.stringTagValue(serviceId))
                .searchableTag(2, Value.stringTagValue(serviceInstanceId))
                .searchableTag(3, Value.stringTagValue(endpointId))
                .searchableTag(4, Value.longTagValue(latency)) // 4
                .searchableTag(5, Value.longTagValue(state))
                .searchableTag(6, Value.stringTagValue(httpStatusCode))
                .searchableTag(7, Value.nullTagValue()) // 7
                .searchableTag(8, Value.stringTagValue(dbType))
                .searchableTag(9, Value.stringTagValue(dbInstance))
                .searchableTag(10, Value.stringTagValue(broker))
                .searchableTag(11, Value.stringTagValue(topic))
                .searchableTag(12, Value.stringTagValue(queue)); // 12

        streamBulkWriteProcessor.add(streamWrite);

        if (allRequestsDelivered.await(5, TimeUnit.SECONDS)) {
            Assert.assertEquals(1, writeRequestDelivered.size());
            final BanyandbStream.WriteRequest request = writeRequestDelivered.get(0);
            Assert.assertArrayEquals(byteData, request.getElement().getTagFamilies(0).getTags(0).getBinaryData().toByteArray());
            Assert.assertEquals(13, request.getElement().getTagFamilies(1).getTagsCount());
            Assert.assertEquals(traceId, request.getElement().getTagFamilies(1).getTags(0).getStr().getValue());
            Assert.assertEquals(latency, request.getElement().getTagFamilies(1).getTags(4).getInt().getValue());
            Assert.assertEquals(request.getElement().getTagFamilies(1).getTags(7).getNull(), NullValue.NULL_VALUE);
            Assert.assertEquals(queue, request.getElement().getTagFamilies(1).getTags(12).getStr().getValue());
        } else {
            Assert.fail();
        }
    }

    @Test
    public void performSingleWrite() throws Exception {
        final CountDownLatch allRequestsDelivered = new CountDownLatch(1);
        final List<BanyandbStream.WriteRequest> writeRequestDelivered = new ArrayList<>();

        // implement the fake service
        final StreamServiceGrpc.StreamServiceImplBase serviceImpl =
                new StreamServiceGrpc.StreamServiceImplBase() {
                    @Override
                    public StreamObserver<BanyandbStream.WriteRequest> write(StreamObserver<BanyandbStream.WriteResponse> responseObserver) {
                        return new StreamObserver<BanyandbStream.WriteRequest>() {
                            @Override
                            public void onNext(BanyandbStream.WriteRequest value) {
                                writeRequestDelivered.add(value);
                                responseObserver.onNext(BanyandbStream.WriteResponse.newBuilder().build());
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                                responseObserver.onCompleted();
                                allRequestsDelivered.countDown();
                            }
                        };
                    }
                };
        serviceRegistry.addService(serviceImpl);

        String segmentId = "1231.dfd.123123ssf";
        String traceId = "trace_id-xxfff.111323";
        String serviceId = "webapp_id";
        String serviceInstanceId = "10.0.0.1_id";
        String endpointId = "home_id";
        int latency = 200;
        int state = 1;
        Instant now = Instant.now();
        byte[] byteData = new byte[]{14};
        String broker = "172.16.10.129:9092";
        String topic = "topic_1";
        String queue = "queue_2";
        String httpStatusCode = "200";
        String dbType = "SQL";
        String dbInstance = "127.0.0.1:3306";

        StreamWrite streamWrite = new StreamWrite("default", "sw", segmentId, now.toEpochMilli(), 1, 13)
                .dataTag(0, Value.binaryTagValue(byteData))
                .searchableTag(0, Value.stringTagValue(traceId)) // 0
                .searchableTag(1, Value.stringTagValue(serviceId))
                .searchableTag(2, Value.stringTagValue(serviceInstanceId))
                .searchableTag(3, Value.stringTagValue(endpointId))
                .searchableTag(4, Value.longTagValue(latency)) // 4
                .searchableTag(5, Value.longTagValue(state))
                .searchableTag(6, Value.stringTagValue(httpStatusCode))
                .searchableTag(7, Value.nullTagValue()) // 7
                .searchableTag(8, Value.stringTagValue(dbType))
                .searchableTag(9, Value.stringTagValue(dbInstance))
                .searchableTag(10, Value.stringTagValue(broker))
                .searchableTag(11, Value.stringTagValue(topic))
                .searchableTag(12, Value.stringTagValue(queue)); // 12

        client.write(streamWrite);

        if (allRequestsDelivered.await(5, TimeUnit.SECONDS)) {
            Assert.assertEquals(1, writeRequestDelivered.size());
            final BanyandbStream.WriteRequest request = writeRequestDelivered.get(0);
            Assert.assertArrayEquals(byteData, request.getElement().getTagFamilies(0).getTags(0).getBinaryData().toByteArray());
            Assert.assertEquals(13, request.getElement().getTagFamilies(1).getTagsCount());
            Assert.assertEquals(traceId, request.getElement().getTagFamilies(1).getTags(0).getStr().getValue());
            Assert.assertEquals(latency, request.getElement().getTagFamilies(1).getTags(4).getInt().getValue());
            Assert.assertEquals(request.getElement().getTagFamilies(1).getTags(7).getNull(), NullValue.NULL_VALUE);
            Assert.assertEquals(queue, request.getElement().getTagFamilies(1).getTags(12).getStr().getValue());
        } else {
            Assert.fail();
        }
    }
}
