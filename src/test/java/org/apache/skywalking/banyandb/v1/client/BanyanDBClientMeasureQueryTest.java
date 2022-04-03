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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.measure.v1.MeasureServiceGrpc;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBApiException;
import org.apache.skywalking.banyandb.v1.client.metadata.Duration;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRule;
import org.apache.skywalking.banyandb.v1.client.metadata.Measure;
import org.apache.skywalking.banyandb.v1.client.metadata.TagFamilySpec;
import org.junit.Assert;
import org.junit.Before;
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
public class BanyanDBClientMeasureQueryTest extends AbstractBanyanDBClientTest {
    private final MeasureServiceGrpc.MeasureServiceImplBase measureQueryService =
            mock(MeasureServiceGrpc.MeasureServiceImplBase.class, delegatesTo(
                    new MeasureServiceGrpc.MeasureServiceImplBase() {
                        @Override
                        public void query(BanyandbMeasure.QueryRequest request, StreamObserver<BanyandbMeasure.QueryResponse> responseObserver) {
                            responseObserver.onNext(BanyandbMeasure.QueryResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }
                    }));

    @Before
    public void setUp() throws IOException, BanyanDBApiException {
        setUp(bindService(measureQueryService), bindMeasureRegistry());

        Measure m = Measure.create("sw_metric", "service_cpm_minute", Duration.ofHours(1))
                .setEntityRelativeTags("entity_id")
                .addTagFamily(TagFamilySpec.create("default")
                        .addTagSpec(TagFamilySpec.TagSpec.newIDTag("id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("entity_id"))
                        .build())
                .addField(Measure.FieldSpec.newIntField("total").compressWithZSTD().encodeWithGorilla().build())
                .addField(Measure.FieldSpec.newIntField("value").compressWithZSTD().encodeWithGorilla().build())
                .addIndex(IndexRule.create("scope", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES))
                .build();
        client.define(m);
    }

    @Test
    public void testQuery_tableScan() throws BanyanDBApiException {
        ArgumentCaptor<BanyandbMeasure.QueryRequest> requestCaptor = ArgumentCaptor.forClass(BanyandbMeasure.QueryRequest.class);

        Instant end = Instant.now();
        Instant begin = end.minus(15, ChronoUnit.MINUTES);
        MeasureQuery query = new MeasureQuery("sw_metric", "service_cpm_minute",
                new TimestampRange(begin.toEpochMilli(), end.toEpochMilli()),
                ImmutableSet.of("id", "entity_id"),
                ImmutableSet.of("total"));
        query.maxBy("total", ImmutableSet.of("entity_id"));
        // search with conditions
        query.appendCondition(PairQueryCondition.StringQueryCondition.eq("default", "service_id", "abc"));
        client.query(query);

        verify(measureQueryService).query(requestCaptor.capture(), ArgumentMatchers.any());

        final BanyandbMeasure.QueryRequest request = requestCaptor.getValue();
        // assert metadata
        Assert.assertEquals("service_cpm_minute", request.getMetadata().getName());
        Assert.assertEquals("sw_metric", request.getMetadata().getGroup());
        // assert timeRange, both seconds and the nanos
        Assert.assertEquals(begin.toEpochMilli() / 1000, request.getTimeRange().getBegin().getSeconds());
        Assert.assertEquals(TimeUnit.MILLISECONDS.toNanos(begin.toEpochMilli() % 1000), request.getTimeRange().getBegin().getNanos());
        Assert.assertEquals(end.toEpochMilli() / 1000, request.getTimeRange().getEnd().getSeconds());
        Assert.assertEquals(TimeUnit.MILLISECONDS.toNanos(end.toEpochMilli() % 1000), request.getTimeRange().getEnd().getNanos());
        // assert fields, we only have state as a condition which should be state
        Assert.assertEquals(1, request.getCriteriaCount());
        // assert state
        Assert.assertEquals(BanyandbModel.Condition.BinaryOp.BINARY_OP_EQ, request.getCriteria(0).getConditions(0).getOp());
        Assert.assertEquals(0L, request.getCriteria(0).getConditions(0).getValue().getInt().getValue());
        // assert projections
        assertCollectionEqual(Lists.newArrayList("default:id", "default:entity_id"),
                parseProjectionList(request.getTagProjection()));
        assertCollectionEqual(Lists.newArrayList("total"),
                request.getFieldProjection().getNamesList());
    }

    @Test
    public void testQuery_responseConversion() {
        final String elementId = "1231.dfd.123123ssf";
        final String serviceName = "service_name_a";
        final Instant now = Instant.now();
        final BanyandbMeasure.QueryResponse responseObj = BanyandbMeasure.QueryResponse.newBuilder()
                .addDataPoints(BanyandbMeasure.DataPoint.newBuilder()
                        .setTimestamp(Timestamp.newBuilder()
                                .setSeconds(now.toEpochMilli() / 1000)
                                .setNanos((int) TimeUnit.MILLISECONDS.toNanos(now.toEpochMilli() % 1000))
                                .build())
                        .addTagFamilies(BanyandbModel.TagFamily.newBuilder()
                                .setName("default")
                                .addTags(BanyandbModel.Tag.newBuilder()
                                        .setKey("id")
                                        .setValue(BanyandbModel.TagValue.newBuilder()
                                                .setId(BanyandbModel.ID.newBuilder().setValue(elementId).build()).build())
                                        .build())
                                .addTags(BanyandbModel.Tag.newBuilder()
                                        .setKey("service_name")
                                        .setValue(BanyandbModel.TagValue.newBuilder()
                                                .setStr(BanyandbModel.Str.newBuilder().setValue(serviceName).build()).build())
                                        .build())
                                .build())
                        .addFields(BanyandbMeasure.DataPoint.Field.newBuilder()
                                .setName("count")
                                .setValue(BanyandbModel.FieldValue.newBuilder().setInt(
                                        BanyandbModel.Int.newBuilder().setValue(10L).build()).build()
                                ).build())
                )
                .build();
        MeasureQueryResponse resp = new MeasureQueryResponse(responseObj);
        Assert.assertNotNull(resp);
        Assert.assertEquals(1, resp.getDataPoints().size());
        Assert.assertEquals(2, resp.getDataPoints().get(0).getTags().size());
        Assert.assertEquals(elementId,
                resp.getDataPoints().get(0).getTagValue("id"));
        Assert.assertEquals(serviceName, resp.getDataPoints().get(0).getTagValue("service_name"));
        Assert.assertEquals(10L,
                (Number) resp.getDataPoints().get(0).getFieldValue("count"));
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
