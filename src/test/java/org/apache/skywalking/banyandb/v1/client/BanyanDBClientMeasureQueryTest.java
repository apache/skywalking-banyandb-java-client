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
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.database.v1.GroupRegistryServiceGrpc;
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.measure.v1.MeasureServiceGrpc;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
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
public class BanyanDBClientMeasureQueryTest {
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

    private final MeasureServiceGrpc.MeasureServiceImplBase serviceImpl =
            mock(MeasureServiceGrpc.MeasureServiceImplBase.class, delegatesTo(
                    new MeasureServiceGrpc.MeasureServiceImplBase() {
                        @Override
                        public void query(BanyandbMeasure.QueryRequest request, StreamObserver<BanyandbMeasure.QueryResponse> responseObserver) {
                            responseObserver.onNext(BanyandbMeasure.QueryResponse.newBuilder().build());
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
        ArgumentCaptor<BanyandbMeasure.QueryRequest> requestCaptor = ArgumentCaptor.forClass(BanyandbMeasure.QueryRequest.class);

        Instant end = Instant.now();
        Instant begin = end.minus(15, ChronoUnit.MINUTES);
        MeasureQuery query = new MeasureQuery("sw_metrics", "service_instance_cpm_day",
                new TimestampRange(begin.toEpochMilli(), end.toEpochMilli()),
                ImmutableSet.of("id", "scope", "service_id"),
                ImmutableSet.of("total"));
        query.maxBy("total", ImmutableSet.of("service_id"));
        // search with conditions
        query.appendCondition(PairQueryCondition.StringQueryCondition.eq("default", "service_id", "abc"));
        client.query(query);

        verify(serviceImpl).query(requestCaptor.capture(), ArgumentMatchers.any());

        final BanyandbMeasure.QueryRequest request = requestCaptor.getValue();
        // assert metadata
        Assert.assertEquals("service_instance_cpm_day", request.getMetadata().getName());
        Assert.assertEquals("sw_metrics", request.getMetadata().getGroup());
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
        assertCollectionEqual(Lists.newArrayList("default:id", "default:scope", "default:service_id"),
                parseProjectionList(request.getTagProjection()));
        assertCollectionEqual(Lists.newArrayList("total"),
                request.getFieldProjection().getNamesList());
    }

    @Test
    public void testQuery_responseConversion() {
        final String elementId = "1231.dfd.123123ssf";
        final String scope = "group1";
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
                                        .setKey("scope")
                                        .setValue(BanyandbModel.TagValue.newBuilder()
                                                .setStr(BanyandbModel.Str.newBuilder().setValue(scope).build()).build())
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
        Assert.assertEquals(3, resp.getDataPoints().get(0).getTags().size());
        Assert.assertEquals(elementId,
                resp.getDataPoints().get(0).getTagValue("id"));
        Assert.assertEquals(scope, resp.getDataPoints().get(0).getTagValue("scope"));
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
