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

import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.database.v1.GroupRegistryServiceGrpc;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;
import org.apache.skywalking.banyandb.stream.v1.StreamServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRule;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleBinding;
import org.apache.skywalking.banyandb.v1.client.metadata.Stream;
import org.apache.skywalking.banyandb.v1.client.metadata.StreamMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.TagFamilySpec;

import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

public class BanyanDBClientStreamWriteTest extends AbstractBanyanDBClientTest {
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

    private StreamBulkWriteProcessor streamBulkWriteProcessor;
    private Stream stream;

    @Before
    public void setUp() throws IOException, BanyanDBException {
        setUp(bindService(groupRegistryServiceImpl), bindStreamRegistry());
        streamBulkWriteProcessor = client.buildStreamWriteProcessor(1000, 1, 1, 10);

        stream = Stream.create("default", "sw")
                .setEntityRelativeTags("service_id", "service_instance_id", "state")
                .addTagFamily(TagFamilySpec.create("data")
                        .addTagSpec(TagFamilySpec.TagSpec.newBinaryTag("data_binary"))
                        .build())
                .addTagFamily(TagFamilySpec.create("searchable")
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("trace_id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newIntTag("state"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_instance_id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("endpoint_id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newIntTag("duration"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("http.method"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("status_code"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("db.type"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("db.instance"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("mq.broker"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("mq.topic"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("mq.queue"))
                        .build())
                .addIndex(IndexRule.create("trace_id", IndexRule.IndexType.INVERTED))
                .build();
        this.client.define(stream);
    }

    @After
    public void shutdown() throws IOException {
        streamBulkWriteProcessor.close();
    }

    @Test
    public void testRegistry() {
        Assert.assertEquals(indexRuleBindingRegistry.size(), 1);
        Assert.assertTrue(indexRuleBindingRegistry.containsKey(IndexRuleBinding.defaultBindingRule("sw")));
        Assert.assertEquals(indexRuleBindingRegistry.get(IndexRuleBinding.defaultBindingRule("sw")).getSubject().getCatalog(),
                BanyandbCommon.Catalog.CATALOG_STREAM);
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

        StreamWrite streamWrite = client.createStreamWrite("default", "sw", segmentId, now.toEpochMilli())
                .tag("data_binary", Value.binaryTagValue(byteData))
                .tag("trace_id", Value.stringTagValue(traceId)) // 0
                .tag("state", Value.longTagValue((long) state)) // 1
                .tag("service_id", Value.stringTagValue(serviceId)) // 2
                .tag("service_instance_id", Value.stringTagValue(serviceInstanceId)) // 3
                .tag("endpoint_id", Value.stringTagValue(endpointId)) // 4
                .tag("duration", Value.longTagValue((long) latency)) // 5
                .tag("http.method", Value.stringTagValue(null)) // 6
                .tag("status_code", Value.stringTagValue(httpStatusCode)) // 7
                .tag("db.type", Value.stringTagValue(dbType)) // 8
                .tag("db.instance", null) // 9
                .tag("mq.broker", Value.stringTagValue(broker)) // 10
                .tag("mq.topic", Value.stringTagValue(topic)) // 11
                .tag("mq.queue", Value.stringTagValue(queue)); // 12

        streamBulkWriteProcessor.add(streamWrite);

        if (allRequestsDelivered.await(5, TimeUnit.SECONDS)) {
            Assert.assertEquals(1, writeRequestDelivered.size());
            final BanyandbStream.WriteRequest request = writeRequestDelivered.get(0);
            Assert.assertArrayEquals(byteData, request.getElement().getTagFamilies(0).getTags(0).getBinaryData().toByteArray());
            Assert.assertEquals(13, request.getElement().getTagFamilies(1).getTagsCount());
            Assert.assertEquals(traceId, request.getElement().getTagFamilies(1).getTags(0).getStr().getValue());
            Assert.assertEquals(latency, request.getElement().getTagFamilies(1).getTags(5).getInt().getValue());
            Assert.assertEquals(request.getElement().getTagFamilies(1).getTags(7).getNull(), NullValue.NULL_VALUE);
            Assert.assertEquals(request.getElement().getTagFamilies(1).getTags(9).getNull(), NullValue.NULL_VALUE);
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
        final ServerServiceDefinition serviceImpl = createService(allRequestsDelivered, writeRequestDelivered);
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

        StreamWrite streamWrite = client.createStreamWrite("default", "sw", segmentId, now.toEpochMilli())
                .tag("data_binary", Value.binaryTagValue(byteData))
                .tag("trace_id", Value.stringTagValue(traceId)) // 0
                .tag("state", Value.longTagValue((long) state)) // 1
                .tag("service_id", Value.stringTagValue(serviceId)) // 2
                .tag("service_instance_id", Value.stringTagValue(serviceInstanceId)) // 3
                .tag("endpoint_id", Value.stringTagValue(endpointId)) // 4
                .tag("duration", Value.longTagValue((long) latency)) // 5
                .tag("http.method", Value.stringTagValue(null)) // 6
                .tag("status_code", Value.stringTagValue(httpStatusCode)) // 7
                .tag("db.type", Value.stringTagValue(dbType)) // 8
                .tag("db.instance", Value.stringTagValue(dbInstance)) // 9
                .tag("mq.broker", Value.stringTagValue(broker)) // 10
                .tag("mq.topic", Value.stringTagValue(topic)) // 11
                .tag("mq.queue", Value.stringTagValue(queue)); // 12

        client.write(streamWrite);

        if (allRequestsDelivered.await(5, TimeUnit.SECONDS)) {
            Assert.assertEquals(1, writeRequestDelivered.size());
            final BanyandbStream.WriteRequest request = writeRequestDelivered.get(0);
            Assert.assertArrayEquals(byteData, request.getElement().getTagFamilies(0).getTags(0).getBinaryData().toByteArray());
            Assert.assertEquals(13, request.getElement().getTagFamilies(1).getTagsCount());
            Assert.assertEquals(traceId, request.getElement().getTagFamilies(1).getTags(0).getStr().getValue());
            Assert.assertEquals(latency, request.getElement().getTagFamilies(1).getTags(5).getInt().getValue());
            Assert.assertEquals(request.getElement().getTagFamilies(1).getTags(7).getNull(), NullValue.NULL_VALUE);
            Assert.assertEquals(queue, request.getElement().getTagFamilies(1).getTags(12).getStr().getValue());
            Assert.assertTrue(request.getMessageId() > 0);
        } else {
            Assert.fail();
        }
    }

    @Test
    public void testAutoRefreshSchema() throws Exception {
        CountDownLatch allRequestsDelivered = new CountDownLatch(1);
        List<BanyandbStream.WriteRequest> writeRequestDelivered = new ArrayList<>();
        ServerServiceDefinition serviceImpl = createService(allRequestsDelivered, writeRequestDelivered);
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

        StreamWrite streamWrite = client.createStreamWrite("default", "sw", segmentId, now.toEpochMilli())
                .tag("data_binary", Value.binaryTagValue(byteData))
                .tag("trace_id", Value.stringTagValue(traceId)) // 0
                .tag("state", Value.longTagValue((long) state)) // 1
                .tag("service_id", Value.stringTagValue(serviceId)) // 2
                .tag("service_instance_id", Value.stringTagValue(serviceInstanceId)) // 3
                .tag("endpoint_id", Value.stringTagValue(endpointId)) // 4
                .tag("duration", Value.longTagValue((long) latency)) // 5
                .tag("http.method", Value.stringTagValue(null)) // 6
                .tag("status_code", Value.stringTagValue(httpStatusCode)) // 7
                .tag("db.type", Value.stringTagValue(dbType)) // 8
                .tag("db.instance", Value.stringTagValue(dbInstance)) // 9
                .tag("mq.broker", Value.stringTagValue(broker)) // 10
                .tag("mq.topic", Value.stringTagValue(topic)) // 11
                .tag("mq.queue", Value.stringTagValue(queue)); // 12

        // update schema
        Stream streamUpdate = stream.toBuilder()
                .addTagFamily(TagFamilySpec.create("new_tag_family")
                        .addTagSpec(TagFamilySpec.TagSpec.newIntTag("new_tag"))
                        .build())
                .build();
        StreamMetadataRegistry streamMetadataRegistry = new StreamMetadataRegistry(checkNotNull(this.channel));
        streamMetadataRegistry.update(streamUpdate);

        streamBulkWriteProcessor.add(streamWrite);
        if (allRequestsDelivered.await(5, TimeUnit.SECONDS)) {
            Assert.assertEquals(0, writeRequestDelivered.size());
        } else {
            Assert.fail();
        }

        // rewrite
        serviceRegistry.removeService(serviceImpl);
        allRequestsDelivered = new CountDownLatch(1);
        writeRequestDelivered = new ArrayList<>();
        serviceImpl = createService(allRequestsDelivered, writeRequestDelivered);
        serviceRegistry.addService(serviceImpl);

        now = Instant.now();
        streamWrite = client.createStreamWrite("default", "sw", segmentId, now.toEpochMilli())
                .tag("data_binary", Value.binaryTagValue(byteData))
                .tag("trace_id", Value.stringTagValue(traceId)) // 0
                .tag("state", Value.longTagValue((long) state)) // 1
                .tag("service_id", Value.stringTagValue(serviceId)) // 2
                .tag("service_instance_id", Value.stringTagValue(serviceInstanceId)) // 3
                .tag("endpoint_id", Value.stringTagValue(endpointId)) // 4
                .tag("duration", Value.longTagValue((long) latency)) // 5
                .tag("http.method", Value.stringTagValue(null)) // 6
                .tag("status_code", Value.stringTagValue(httpStatusCode)) // 7
                .tag("db.type", Value.stringTagValue(dbType)) // 8
                .tag("db.instance", Value.stringTagValue(dbInstance)) // 9
                .tag("mq.broker", Value.stringTagValue(broker)) // 10
                .tag("mq.topic", Value.stringTagValue(topic)) // 11
                .tag("mq.queue", Value.stringTagValue(queue)); // 12

        streamBulkWriteProcessor.add(streamWrite);
        if (allRequestsDelivered.await(5, TimeUnit.SECONDS)) {
            Assert.assertEquals(1, writeRequestDelivered.size());
            final BanyandbStream.WriteRequest request = writeRequestDelivered.get(0);
            Assert.assertArrayEquals(byteData, request.getElement().getTagFamilies(0).getTags(0).getBinaryData().toByteArray());
            Assert.assertEquals(13, request.getElement().getTagFamilies(1).getTagsCount());
            Assert.assertEquals(traceId, request.getElement().getTagFamilies(1).getTags(0).getStr().getValue());
            Assert.assertEquals(latency, request.getElement().getTagFamilies(1).getTags(5).getInt().getValue());
            Assert.assertEquals(request.getElement().getTagFamilies(1).getTags(7).getNull(), NullValue.NULL_VALUE);
            Assert.assertEquals(queue, request.getElement().getTagFamilies(1).getTags(12).getStr().getValue());
            Assert.assertTrue(request.getMessageId() > 0);
        } else {
            Assert.fail();
        }
    }

    @SneakyThrows
    private boolean checkSchemaExpired(BanyandbStream.WriteRequest request) {
        Stream m = client.findStream(request.getMetadata().getGroup(), request.getMetadata().getName());
        return request.getMetadata().getModRevision() != m.serialize().getMetadata().getModRevision();
    }

    private ServerServiceDefinition createService(final CountDownLatch allRequestsDelivered,
                                                  final List<BanyandbStream.WriteRequest> writeRequestDelivered) {
        return new StreamServiceGrpc.StreamServiceImplBase() {
            @Override
            public StreamObserver<BanyandbStream.WriteRequest> write(StreamObserver<BanyandbStream.WriteResponse> responseObserver) {
                return new StreamObserver<BanyandbStream.WriteRequest>() {
                    @Override
                    public void onNext(BanyandbStream.WriteRequest request) {
                        if (checkSchemaExpired(request)) {
                            responseObserver.onNext(
                                    BanyandbStream.WriteResponse.newBuilder()
                                            .setMetadata(request.getMetadata())
                                            .setStatus(BanyandbModel.Status.STATUS_EXPIRED_SCHEMA)
                                            .setMessageId(request.getMessageId())
                                            .build());
                        } else {
                            writeRequestDelivered.add(request);
                            responseObserver.onNext(BanyandbStream.WriteResponse.newBuilder().setMessageId(request.getMessageId()).build());
                        }
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
        }.bindService();
    }
}
