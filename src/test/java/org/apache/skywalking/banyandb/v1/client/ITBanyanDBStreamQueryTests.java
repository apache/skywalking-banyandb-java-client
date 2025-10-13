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
import io.grpc.stub.StreamObserver;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Group;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Catalog;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.IntervalRule;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.ResourceOpts;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Metadata;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.Entity;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TagFamilySpec;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TagSpec;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TagType;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.Stream;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.IndexRule;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.IndexRuleBinding;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.skywalking.banyandb.v1.client.BanyanDBClient.DEFAULT_EXPIRE_AT;
import static org.awaitility.Awaitility.await;

public class ITBanyanDBStreamQueryTests extends BanyanDBClientTestCI {

    @Before
    public void setUp() throws IOException, BanyanDBException, InterruptedException {
        this.setUpConnection();
        this.client.define(buildGroup());
        this.client.define(buildStream());
        this.client.define(buildIndexRule());
        this.client.define(buildIndexRuleBinding());
    }

    @After
    public void tearDown() throws IOException {
        this.closeClient();
    }

    @Test
    public void testStreamQuery_TraceID() throws BanyanDBException, ExecutionException, InterruptedException, TimeoutException {
        // try to write a trace
        String segmentId = "1231.dfd.123123ssf";
        String traceId = "trace_id-xxfff.111323";
        String serviceId = "webapp_id";
        String serviceInstanceId = "10.0.0.1_id";
        String endpointId = "home_id";
        long latency = 200;
        long state = 1;
        Instant now = Instant.now();
        byte[] byteData = new byte[]{14};
        String broker = "172.16.10.129:9092";
        String topic = "topic_1";
        String queue = "queue_2";
        String httpStatusCode = "200";
        String dbType = "SQL";
        String dbInstance = "127.0.0.1:3306";

        StreamWrite streamWrite = client.createStreamWrite("sw_record", "trace", segmentId)
                .tag("data_binary", Value.binaryTagValue(byteData))
                .tag("trace_id", Value.stringTagValue(traceId)) // 0
                .tag("state", Value.longTagValue(state)) // 1
                .tag("service_id", Value.stringTagValue(serviceId)) // 2
                .tag("service_instance_id", Value.stringTagValue(serviceInstanceId)) // 3
                .tag("endpoint_id", Value.stringTagValue(endpointId)) // 4
                .tag("duration", Value.longTagValue(latency)) // 5
                .tag("http.method", Value.stringTagValue(null)) // 6
                .tag("status_code", Value.stringTagValue(httpStatusCode)) // 7
                .tag("db.type", Value.stringTagValue(dbType)) // 8
                .tag("db.instance", Value.stringTagValue(dbInstance)) // 9
                .tag("mq.broker", Value.stringTagValue(broker)) // 10
                .tag("mq.topic", Value.stringTagValue(topic)) // 11
                .tag("mq.queue", Value.stringTagValue(queue)); // 12
        streamWrite.setTimestamp(now.toEpochMilli());
        StreamObserver<BanyandbStream.WriteRequest> writeObserver
            = client.getStreamServiceStub().write(new StreamObserver<BanyandbStream.WriteResponse>() {
            @Override
            public void onNext(BanyandbStream.WriteResponse writeResponse) {
                Assert.assertEquals(BanyandbModel.Status.STATUS_SUCCEED.name(), writeResponse.getStatus());
            }

            @Override
            public void onError(Throwable throwable) {
                Assert.fail("write failed: " + throwable.getMessage());
            }

            @Override
            public void onCompleted() {
            }
        });
        try {
            writeObserver.onNext(streamWrite.build());

        } finally {
            writeObserver.onCompleted();
        }

        StreamQuery query = new StreamQuery(
            Lists.newArrayList("sw_record"), "trace", ImmutableSet.of("state", "duration", "trace_id", "data_binary"));
        query.and(PairQueryCondition.StringQueryCondition.eq("trace_id", traceId));
        query.setOrderBy(new AbstractQuery.OrderBy(AbstractQuery.Sort.DESC));
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            StreamQueryResponse resp = client.query(query);
            Assert.assertNotNull(resp);
            Assert.assertEquals(resp.size(), 1);
            Assert.assertEquals(latency, (Number) resp.getElements().get(0).getTagValue("duration"));
            Assert.assertEquals(traceId, resp.getElements().get(0).getTagValue("trace_id"));
        });
    }

    private Group buildGroup() {
        return Group.newBuilder().setMetadata(Metadata.newBuilder().setName("sw_record"))
                    .setCatalog(Catalog.CATALOG_STREAM)
                    .setResourceOpts(ResourceOpts.newBuilder()
                                                 .setShardNum(2)
                                                 .setSegmentInterval(
                                                     IntervalRule.newBuilder()
                                                                 .setUnit(
                                                                     IntervalRule.Unit.UNIT_DAY)
                                                                 .setNum(
                                                                     1))
                                                 .setTtl(
                                                     IntervalRule.newBuilder()
                                                                 .setUnit(
                                                                     IntervalRule.Unit.UNIT_DAY)
                                                                 .setNum(
                                                                     3)))
                    .build();
    }

    private Stream buildStream() {
        Stream.Builder builder = Stream.newBuilder()
                                       .setMetadata(Metadata.newBuilder()
                                                            .setGroup("sw_record")
                                                            .setName("trace"))
                                       .setEntity(Entity.newBuilder().addAllTagNames(
                                           Arrays.asList("service_id", "service_instance_id", "state")))
                                       .addTagFamilies(TagFamilySpec.newBuilder()
                                                                    .setName("data")
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("data_binary")
                                                                                    .setType(TagType.TAG_TYPE_DATA_BINARY)))
                                       .addTagFamilies(TagFamilySpec.newBuilder()
                                                                    .setName("searchable")
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("trace_id")
                                                                                    .setType(TagType.TAG_TYPE_STRING))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("state")
                                                                                    .setType(TagType.TAG_TYPE_INT))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("service_id")
                                                                                    .setType(TagType.TAG_TYPE_STRING)
                                                                                    .setIndexedOnly(true))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("service_instance_id")
                                                                                    .setType(TagType.TAG_TYPE_STRING))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("endpoint_id")
                                                                                    .setType(TagType.TAG_TYPE_STRING))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("duration")
                                                                                    .setType(TagType.TAG_TYPE_INT))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("http.method")
                                                                                    .setType(TagType.TAG_TYPE_STRING))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("status_code")
                                                                                    .setType(TagType.TAG_TYPE_STRING))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("db.type")
                                                                                    .setType(TagType.TAG_TYPE_STRING))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("db.instance")
                                                                                    .setType(TagType.TAG_TYPE_STRING))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("mq.broker")
                                                                                    .setType(TagType.TAG_TYPE_STRING))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("mq.topic")
                                                                                    .setType(TagType.TAG_TYPE_STRING))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("mq.queue")
                                                                                    .setType(TagType.TAG_TYPE_STRING)));
        return builder.build();
    }

    private IndexRule buildIndexRule() {
        IndexRule.Builder builder = IndexRule.newBuilder()
                                             .setMetadata(Metadata.newBuilder()
                                                                  .setGroup("sw_record")
                                                                  .setName("trace_id"))
                                             .addTags("trace_id")
                                             .setType(IndexRule.Type.TYPE_INVERTED);
        return builder.build();
    }

    private IndexRuleBinding buildIndexRuleBinding() {
        IndexRuleBinding.Builder builder = IndexRuleBinding.newBuilder()
                                                           .setMetadata(BanyandbCommon.Metadata.newBuilder()
                                                                                               .setGroup("sw_record")
                                                                                               .setName("trace_binding"))
                                                           .setSubject(BanyandbDatabase.Subject.newBuilder()
                                                                                               .setCatalog(
                                                                                                   BanyandbCommon.Catalog.CATALOG_STREAM)
                                                                                               .setName("trace"))
                                                           .addAllRules(
                                                               Arrays.asList("trace_id"))
                                                           .setBeginAt(TimeUtils.buildTimestamp(ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)))
                                                           .setExpireAt(TimeUtils.buildTimestamp(DEFAULT_EXPIRE_AT));
        return builder.build();
    }
}
