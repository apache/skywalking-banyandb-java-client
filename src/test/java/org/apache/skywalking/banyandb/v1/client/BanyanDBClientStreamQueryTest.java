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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.database.v1.GroupRegistryServiceGrpc;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;
import org.apache.skywalking.banyandb.stream.v1.StreamServiceGrpc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class BanyanDBClientStreamQueryTest {
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

    private final StreamServiceGrpc.StreamServiceImplBase serviceImpl =
            mock(StreamServiceGrpc.StreamServiceImplBase.class, delegatesTo(
                    new StreamServiceGrpc.StreamServiceImplBase() {
                        @Override
                        public void query(BanyandbStream.QueryRequest request, StreamObserver<BanyandbStream.QueryResponse> responseObserver) {
                            responseObserver.onNext(BanyandbStream.QueryResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }
                    }));

    private BanyanDBClient client;

    @Before
    public void setUp() throws IOException {
        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        Server server = InProcessServerBuilder
                .forName(serverName).directExecutor()
                .addService(serviceImpl)
                .addService(groupRegistryServiceImpl).build();
        grpcCleanup.register(server.start());

        // Create a client channel and register for automatic graceful shutdown.
        ManagedChannel channel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());
        client = new BanyanDBClient("127.0.0.1", server.getPort());

        client.connect(channel);
    }

    @Test
    public void testNonNull() {
        Assert.assertNotNull(this.client);
    }

    @Test
    public void testQuery_tableScan() {
        ArgumentCaptor<BanyandbStream.QueryRequest> requestCaptor = ArgumentCaptor.forClass(BanyandbStream.QueryRequest.class);

        Instant end = Instant.now();
        Instant begin = end.minus(15, ChronoUnit.MINUTES);
        StreamQuery query = new StreamQuery("default", "sw",
                new TimestampRange(begin.toEpochMilli(), end.toEpochMilli()),
                ImmutableSet.of("state", "start_time", "duration", "trace_id"));
        // search for all states
        query.appendCondition(PairQueryCondition.LongQueryCondition.eq("searchable", "state", 0L));
        query.setOrderBy(new StreamQuery.OrderBy("duration", StreamQuery.OrderBy.Type.DESC));
        client.query(query);

        verify(serviceImpl).query(requestCaptor.capture(), ArgumentMatchers.any());

        final BanyandbStream.QueryRequest request = requestCaptor.getValue();
        // assert metadata
        Assert.assertEquals("sw", request.getMetadata().getName());
        Assert.assertEquals("default", request.getMetadata().getGroup());
        // assert timeRange, both seconds and the nanos
        Assert.assertEquals(begin.toEpochMilli() / 1000, request.getTimeRange().getBegin().getSeconds());
        Assert.assertEquals(TimeUnit.MILLISECONDS.toNanos(begin.toEpochMilli() % 1000), request.getTimeRange().getBegin().getNanos());
        Assert.assertEquals(end.toEpochMilli() / 1000, request.getTimeRange().getEnd().getSeconds());
        Assert.assertEquals(TimeUnit.MILLISECONDS.toNanos(end.toEpochMilli() % 1000), request.getTimeRange().getEnd().getNanos());
        // assert fields, we only have state as a condition which should be state
        Assert.assertEquals(1, request.getCriteriaCount());
        // assert orderBy, by default DESC
        Assert.assertEquals(BanyandbModel.Sort.SORT_DESC, request.getOrderBy().getSort());
        Assert.assertEquals("duration", request.getOrderBy().getIndexRuleName());
        // assert state
        Assert.assertEquals(BanyandbModel.Condition.BinaryOp.BINARY_OP_EQ, request.getCriteria(0).getConditions(0).getOp());
        Assert.assertEquals(0L, request.getCriteria(0).getConditions(0).getValue().getInt().getValue());
        // assert projections
        assertCollectionEqual(Lists.newArrayList("searchable:duration", "searchable:state", "searchable:start_time", "searchable:trace_id"),
                parseProjectionList(request.getProjection()));
    }

    @Test
    public void testQuery_indexScan() {
        ArgumentCaptor<BanyandbStream.QueryRequest> requestCaptor = ArgumentCaptor.forClass(BanyandbStream.QueryRequest.class);
        Instant begin = Instant.now().minus(5, ChronoUnit.MINUTES);
        Instant end = Instant.now();
        String serviceId = "service_id_b";
        String serviceInstanceId = "service_id_b_1";
        String endpointId = "/check_0";
        long minDuration = 10;
        long maxDuration = 100;

        StreamQuery query = new StreamQuery("default", "sw",
                new TimestampRange(begin.toEpochMilli(), end.toEpochMilli()),
                ImmutableSet.of("state", "start_time", "duration", "trace_id"));
        // search for the successful states
        query.appendCondition(PairQueryCondition.LongQueryCondition.eq("searchable", "state", 1L))
                .appendCondition(PairQueryCondition.StringQueryCondition.eq("searchable", "service_id", serviceId))
                .appendCondition(PairQueryCondition.StringQueryCondition.eq("searchable", "service_instance_id", serviceInstanceId))
                .appendCondition(PairQueryCondition.StringQueryCondition.eq("searchable", "endpoint_id", endpointId))
                .appendCondition(PairQueryCondition.LongQueryCondition.ge("searchable", "duration", minDuration))
                .appendCondition(PairQueryCondition.LongQueryCondition.le("searchable", "duration", maxDuration))
                .setOrderBy(new StreamQuery.OrderBy("start_time", StreamQuery.OrderBy.Type.ASC));

        client.query(query);

        verify(serviceImpl).query(requestCaptor.capture(), ArgumentMatchers.any());
        final BanyandbStream.QueryRequest request = requestCaptor.getValue();
        // assert metadata
        Assert.assertEquals("sw", request.getMetadata().getName());
        Assert.assertEquals("default", request.getMetadata().getGroup());
        // assert timeRange
        Assert.assertEquals(begin.getEpochSecond(), request.getTimeRange().getBegin().getSeconds());
        Assert.assertEquals(end.getEpochSecond(), request.getTimeRange().getEnd().getSeconds());
        // assert fields, we only have state as a condition
        Assert.assertEquals(6, request.getCriteria(0).getConditionsCount());
        // assert orderBy, by default DESC
        Assert.assertEquals(BanyandbModel.Sort.SORT_ASC, request.getOrderBy().getSort());
        Assert.assertEquals("start_time", request.getOrderBy().getIndexRuleName());
        // assert projections
        assertCollectionEqual(Lists.newArrayList("searchable:duration", "searchable:state", "searchable:start_time", "searchable:trace_id"), parseProjectionList(request.getProjection()));
        // assert fields
        assertCollectionEqual(request.getCriteria(0).getConditionsList(), ImmutableList.of(
                PairQueryCondition.LongQueryCondition.ge("searchable", "duration", minDuration).build(), // 1 -> duration >= minDuration
                PairQueryCondition.LongQueryCondition.le("searchable", "duration", maxDuration).build(), // 2 -> duration <= maxDuration
                PairQueryCondition.StringQueryCondition.eq("searchable", "service_id", serviceId).build(), // 3 -> service_id
                PairQueryCondition.StringQueryCondition.eq("searchable", "service_instance_id", serviceInstanceId).build(), // 4 -> service_instance_id
                PairQueryCondition.StringQueryCondition.eq("searchable", "endpoint_id", endpointId).build(), // 5 -> endpoint_id
                PairQueryCondition.LongQueryCondition.eq("searchable", "state", 1L).build() // 7 -> state
        ));
    }

    @Test
    public void testQuery_TraceIDFetch() {
        ArgumentCaptor<BanyandbStream.QueryRequest> requestCaptor = ArgumentCaptor.forClass(BanyandbStream.QueryRequest.class);
        String traceId = "1111.222.333";

        StreamQuery query = new StreamQuery("default", "sw", ImmutableSet.of("state", "start_time", "duration", "trace_id"));
        query.appendCondition(PairQueryCondition.StringQueryCondition.eq("searchable", "trace_id", traceId));
        query.setDataProjections(ImmutableSet.of("data_binary"));

        client.query(query);

        verify(serviceImpl).query(requestCaptor.capture(), ArgumentMatchers.any());
        final BanyandbStream.QueryRequest request = requestCaptor.getValue();
        // assert metadata
        Assert.assertEquals("sw", request.getMetadata().getName());
        Assert.assertEquals("default", request.getMetadata().getGroup());
        Assert.assertEquals(1, request.getCriteria(0).getConditionsCount());
        // assert fields
        assertCollectionEqual(request.getCriteria(0).getConditionsList(), ImmutableList.of(
                PairQueryCondition.StringQueryCondition.eq("searchable", "trace_id", traceId).build()
        ));
    }

    @Test
    public void testQuery_responseConversion() {
        final byte[] binaryData = new byte[]{13};
        final String elementId = "1231.dfd.123123ssf";
        final String traceId = "trace_id-xxfff.111323";
        final long duration = 200L;
        final Instant now = Instant.now();
        final BanyandbStream.QueryResponse responseObj = BanyandbStream.QueryResponse.newBuilder()
                .addElements(BanyandbStream.Element.newBuilder()
                        .setElementId(elementId)
                        .setTimestamp(Timestamp.newBuilder()
                                .setSeconds(now.toEpochMilli() / 1000)
                                .setNanos((int) TimeUnit.MILLISECONDS.toNanos(now.toEpochMilli() % 1000))
                                .build())
                        .addTagFamilies(BanyandbModel.TagFamily.newBuilder()
                                .setName("searchable")
                                .addTags(BanyandbModel.Tag.newBuilder()
                                        .setKey("trace_id")
                                        .setValue(BanyandbModel.TagValue.newBuilder()
                                                .setStr(BanyandbModel.Str.newBuilder().setValue(traceId).build()).build())
                                        .build())
                                .addTags(BanyandbModel.Tag.newBuilder()
                                        .setKey("duration")
                                        .setValue(BanyandbModel.TagValue.newBuilder()
                                                .setInt(BanyandbModel.Int.newBuilder().setValue(duration).build()).build())
                                        .build())
                                .addTags(BanyandbModel.Tag.newBuilder()
                                        .setKey("mq.broker")
                                        .setValue(BanyandbModel.TagValue.newBuilder().setNull(NullValue.NULL_VALUE).build())
                                        .build())
                                .build())
                        .addTagFamilies(BanyandbModel.TagFamily.newBuilder()
                                .setName("data")
                                .addTags(BanyandbModel.Tag.newBuilder()
                                        .setKey("data_binary")
                                        .setValue(BanyandbModel.TagValue.newBuilder()
                                                .setBinaryData(ByteString.copyFrom(binaryData)).build())
                                        .build())
                                .build())
                        .build())
                .build();
        StreamQueryResponse resp = new StreamQueryResponse(responseObj);
        Assert.assertNotNull(resp);
        Assert.assertEquals(1, resp.getElements().size());
        Assert.assertEquals(3, resp.getElements().get(0).getTags().size());
        Assert.assertEquals(traceId,
                resp.getElements().get(0).getTagValue("searchable", "trace_id"));
        Assert.assertEquals(duration,
                (Number) resp.getElements().get(0).getTagValue("searchable", "duration"));
        Assert.assertNull(resp.getElements().get(0).getTagValue("searchable", "mq.broker"));
        Assert.assertArrayEquals(binaryData,
                resp.getElements().get(0).getTagValue("data", "data_binary"));
    }

    static <T> void assertCollectionEqual(Collection<T> c1, Collection<T> c2) {
        Assert.assertTrue(c1.size() == c2.size() && c1.containsAll(c2) && c2.containsAll(c1));
    }

    static List<String> parseProjectionList(BanyandbModel.TagProjection projection) {
        List<String> projectionList = new ArrayList<>();
        for (int i = 0; i < projection.getTagFamiliesCount(); i++) {
            final BanyandbModel.TagProjection.TagFamily tagFamily = projection.getTagFamilies(i);
            for (int j = 0; j < tagFamily.getTagsCount(); j++) {
                projectionList.add(tagFamily.getName() + ":" + tagFamily.getTags(j));
            }
        }
        return projectionList;
    }
}