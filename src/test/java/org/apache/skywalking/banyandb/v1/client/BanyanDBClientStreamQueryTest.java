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
import io.grpc.stub.StreamObserver;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;
import org.apache.skywalking.banyandb.stream.v1.StreamServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRule;
import org.apache.skywalking.banyandb.v1.client.metadata.Stream;
import org.apache.skywalking.banyandb.v1.client.metadata.TagFamilySpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class BanyanDBClientStreamQueryTest extends AbstractBanyanDBClientTest {
    // query service
    private final StreamServiceGrpc.StreamServiceImplBase streamQueryServiceImpl =
            mock(StreamServiceGrpc.StreamServiceImplBase.class, delegatesTo(
                    new StreamServiceGrpc.StreamServiceImplBase() {
                        @Override
                        public void query(BanyandbStream.QueryRequest request, StreamObserver<BanyandbStream.QueryResponse> responseObserver) {
                            responseObserver.onNext(BanyandbStream.QueryResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }
                    }));

    @Before
    public void setUp() throws IOException, BanyanDBException {
        this.streamRegistry = new HashMap<>();
        setUp(bindStreamRegistry(), bindService(streamQueryServiceImpl));

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
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("endpoint_id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newIntTag("start_time"))
                        .addTagSpec(TagFamilySpec.TagSpec.newIntTag("duration"))
                        .build())
                .addIndex(IndexRule.create("trace_id", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.GLOBAL))
                .build();
        this.client.define(expectedStream);
    }

    @Test
    public void testQuery_tableScan() throws BanyanDBException {
        ArgumentCaptor<BanyandbStream.QueryRequest> requestCaptor = ArgumentCaptor.forClass(BanyandbStream.QueryRequest.class);

        Instant end = Instant.now();
        Instant begin = end.minus(15, ChronoUnit.MINUTES);
        StreamQuery query = new StreamQuery("default", "sw",
                new TimestampRange(begin.toEpochMilli(), end.toEpochMilli()),
                ImmutableSet.of("state", "start_time", "duration", "trace_id"));
        // search for all states
        query.and(PairQueryCondition.LongQueryCondition.eq("state", 0L));
        query.setOrderBy(new StreamQuery.OrderBy("duration", AbstractQuery.Sort.DESC));
        client.query(query);

        verify(streamQueryServiceImpl).query(requestCaptor.capture(), ArgumentMatchers.any());

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
    public void testQuery_indexScan() throws BanyanDBException {
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
        query.and(PairQueryCondition.LongQueryCondition.eq("state", 1L))
                .and(PairQueryCondition.StringQueryCondition.eq("service_id", serviceId))
                .and(PairQueryCondition.StringQueryCondition.eq("service_instance_id", serviceInstanceId))
                .and(PairQueryCondition.StringQueryCondition.eq("endpoint_id", endpointId))
                .and(PairQueryCondition.LongQueryCondition.ge("duration", minDuration))
                .and(PairQueryCondition.LongQueryCondition.le("duration", maxDuration))
                .setOrderBy(new StreamQuery.OrderBy("start_time", AbstractQuery.Sort.ASC));

        client.query(query);

        verify(streamQueryServiceImpl).query(requestCaptor.capture(), ArgumentMatchers.any());
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
                PairQueryCondition.LongQueryCondition.ge("duration", minDuration).build(), // 1 -> duration >= minDuration
                PairQueryCondition.LongQueryCondition.le("duration", maxDuration).build(), // 2 -> duration <= maxDuration
                PairQueryCondition.StringQueryCondition.eq("service_id", serviceId).build(), // 3 -> service_id
                PairQueryCondition.StringQueryCondition.eq("service_instance_id", serviceInstanceId).build(), // 4 -> service_instance_id
                PairQueryCondition.StringQueryCondition.eq("endpoint_id", endpointId).build(), // 5 -> endpoint_id
                PairQueryCondition.LongQueryCondition.eq("state", 1L).build() // 7 -> state
        ));
    }

    @Test
    public void testQuery_TraceIDFetch() throws BanyanDBException {
        ArgumentCaptor<BanyandbStream.QueryRequest> requestCaptor = ArgumentCaptor.forClass(BanyandbStream.QueryRequest.class);
        String traceId = "1111.222.333";

        StreamQuery query = new StreamQuery("default", "sw", ImmutableSet.of("state", "start_time", "duration", "trace_id", "data_binary"));
        query.and(PairQueryCondition.StringQueryCondition.eq("trace_id", traceId));

        client.query(query);

        verify(streamQueryServiceImpl).query(requestCaptor.capture(), ArgumentMatchers.any());
        final BanyandbStream.QueryRequest request = requestCaptor.getValue();
        // assert metadata
        Assert.assertEquals("sw", request.getMetadata().getName());
        Assert.assertEquals("default", request.getMetadata().getGroup());
        Assert.assertEquals(1, request.getCriteria(0).getConditionsCount());
        // assert fields
        assertCollectionEqual(request.getCriteria(0).getConditionsList(), ImmutableList.of(
                PairQueryCondition.StringQueryCondition.eq("trace_id", traceId).build()
        ));
    }

    @Test
    public void testQuery_responseConversion() throws BanyanDBException {
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
                resp.getElements().get(0).getTagValue("trace_id"));
        Assert.assertEquals(duration,
                (Number) resp.getElements().get(0).getTagValue("duration"));
        Assert.assertNull(resp.getElements().get(0).getTagValue("mq.broker"));
        Assert.assertArrayEquals(binaryData,
                resp.getElements().get(0).getTagValue("data_binary"));
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
