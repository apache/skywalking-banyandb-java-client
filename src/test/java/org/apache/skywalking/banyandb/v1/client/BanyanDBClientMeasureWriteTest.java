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
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.measure.v1.MeasureServiceGrpc;
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

public class BanyanDBClientMeasureWriteTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private final GroupRegistryServiceGrpc.GroupRegistryServiceImplBase groupRegistryServiceImpl =
            mock(GroupRegistryServiceGrpc.GroupRegistryServiceImplBase.class, delegatesTo(
                    new GroupRegistryServiceGrpc.GroupRegistryServiceImplBase() {
                        @Override
                        public void get(BanyandbDatabase.GroupRegistryServiceGetRequest request, StreamObserver<BanyandbDatabase.GroupRegistryServiceGetResponse> responseObserver) {
                            responseObserver.onNext(BanyandbDatabase.GroupRegistryServiceGetResponse.newBuilder()
                                    .setGroup(BanyandbCommon.Group.newBuilder()
                                            .setMetadata(BanyandbCommon.Metadata.newBuilder().setName("sw_metrics").build())
                                            .setCatalog(BanyandbCommon.Catalog.CATALOG_MEASURE)
                                            .setResourceOpts(BanyandbCommon.ResourceOpts.newBuilder()
                                                    .setShardNum(2)
                                                    .setBlockNum(12)
                                                    .setTtl("7d")
                                                    .build())
                                            .build())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    }));

    private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();

    private BanyanDBClient client;
    private MeasureBulkWriteProcessor measureBulkWriteProcessor;

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
        measureBulkWriteProcessor = client.buildMeasureWriteProcessor(1000, 1, 1);
    }

    @After
    public void shutdown() throws IOException {
        measureBulkWriteProcessor.close();
    }

    @Test
    public void testWrite() throws Exception {
        final CountDownLatch allRequestsDelivered = new CountDownLatch(1);
        final List<BanyandbMeasure.WriteRequest> writeRequestDelivered = new ArrayList<>();

        // implement the fake service
        final MeasureServiceGrpc.MeasureServiceImplBase serviceImpl =
                new MeasureServiceGrpc.MeasureServiceImplBase() {
                    @Override
                    public StreamObserver<BanyandbMeasure.WriteRequest> write(StreamObserver<BanyandbMeasure.WriteResponse> responseObserver) {
                        return new StreamObserver<BanyandbMeasure.WriteRequest>() {
                            @Override
                            public void onNext(BanyandbMeasure.WriteRequest value) {
                                writeRequestDelivered.add(value);
                                responseObserver.onNext(BanyandbMeasure.WriteResponse.newBuilder().build());
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

        Instant now = Instant.now();
        MeasureWrite measureWrite = new MeasureWrite("sw_metrics", "service_cpm_minute", now.toEpochMilli(), 2, 2);
        measureWrite.defaultTag(0, Value.idTagValue("1"))
                .defaultTag(1, Value.stringTagValue("entity_1"))
                .field(0, Value.longFieldValue(100L))
                .field(1, Value.longFieldValue(1L));

        measureBulkWriteProcessor.add(measureWrite);

        if (allRequestsDelivered.await(5, TimeUnit.SECONDS)) {
            Assert.assertEquals(1, writeRequestDelivered.size());
            final BanyandbMeasure.WriteRequest request = writeRequestDelivered.get(0);
            Assert.assertEquals(2, request.getDataPoint().getTagFamilies(0).getTagsCount());
            Assert.assertEquals("1", request.getDataPoint().getTagFamilies(0).getTags(0).getId().getValue());
            Assert.assertEquals("entity_1", request.getDataPoint().getTagFamilies(0).getTags(1).getStr().getValue());
            Assert.assertEquals(100, request.getDataPoint().getFields(0).getInt().getValue());
            Assert.assertEquals(1, request.getDataPoint().getFields(1).getInt().getValue());
        } else {
            Assert.fail();
        }
    }
}
