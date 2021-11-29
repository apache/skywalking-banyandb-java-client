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
import org.apache.skywalking.banyandb.v1.stream.BanyandbStream;
import org.apache.skywalking.banyandb.v1.stream.StreamServiceGrpc;
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

public class BanyanDBClientWriteTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();

    private BanyanDBClient client;
    private StreamBulkWriteProcessor streamBulkWriteProcessor;

    @Before
    public void setUp() throws IOException {
        String serverName = InProcessServerBuilder.generateName();

        Server server = InProcessServerBuilder
                .forName(serverName).fallbackHandlerRegistry(serviceRegistry).directExecutor().build();
        grpcCleanup.register(server.start());

        ManagedChannel channel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());

        client = new BanyanDBClient("127.0.0.1", server.getPort(), "default");
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

        StreamWrite streamWrite = StreamWrite.builder()
                .elementId(segmentId)
                .dataTag(Tag.binaryField(byteData))
                .timestamp(now.toEpochMilli())
                .name("sw")
                .searchableTag(Tag.stringField(traceId)) // 0
                .searchableTag(Tag.stringField(serviceId))
                .searchableTag(Tag.stringField(serviceInstanceId))
                .searchableTag(Tag.stringField(endpointId))
                .searchableTag(Tag.longField(latency)) // 4
                .searchableTag(Tag.longField(state))
                .searchableTag(Tag.stringField(httpStatusCode))
                .searchableTag(Tag.nullField()) // 7
                .searchableTag(Tag.stringField(dbType))
                .searchableTag(Tag.stringField(dbInstance))
                .searchableTag(Tag.stringField(broker))
                .searchableTag(Tag.stringField(topic))
                .searchableTag(Tag.stringField(queue)) // 12
                .build();

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
}
