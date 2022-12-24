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
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.metadata.Catalog;
import org.apache.skywalking.banyandb.v1.client.metadata.Group;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRule;
import org.apache.skywalking.banyandb.v1.client.metadata.IntervalRule;
import org.apache.skywalking.banyandb.v1.client.metadata.Stream;
import org.apache.skywalking.banyandb.v1.client.metadata.TagFamilySpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.await;

public class ITBanyanDBStreamQueryTests extends BanyanDBClientTestCI {
    private StreamBulkWriteProcessor processor;

    @Before
    public void setUp() throws IOException, BanyanDBException, InterruptedException {
        this.setUpConnection();
        Group expectedGroup = this.client.define(
                Group.create("default", Catalog.STREAM, 2, IntervalRule.create(IntervalRule.Unit.HOUR, 4),
                        IntervalRule.create(IntervalRule.Unit.DAY, 1),
                        IntervalRule.create(IntervalRule.Unit.DAY, 7))
        );
        Assert.assertNotNull(expectedGroup);
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
                        .addTagSpec(TagFamilySpec.TagSpec.newIntTag("duration"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("http.method"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("status_code"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("db.type"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("db.instance"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("mq.broker"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("mq.topic"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("mq.queue"))
                        .build())
                .addIndex(IndexRule.create("trace_id", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.GLOBAL))
                .addIndex(IndexRule.create("endpoint_id", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES))
                .build();
        this.client.define(expectedStream);
        Assert.assertNotNull(expectedStream);
        processor = client.buildStreamWriteProcessor(1000, 1, 1);
    }

    @After
    public void tearDown() throws IOException {
        if (processor != null) {
            this.processor.close();
        }
        this.closeClient();
    }

    @Test
    public void testStreamQuery_Str_Eq() throws BanyanDBException, ExecutionException, InterruptedException, TimeoutException {
        StreamWrite streamWrite = writeStream();
        long latency = ((Value.LongTagValue) streamWrite.getTag("duration")).getValue();
        String traceId = ((Value.StringTagValue) streamWrite.getTag("trace_id")).getValue();
        StreamQuery query = new StreamQuery("default", "sw", ImmutableSet.of("state", "duration", "trace_id", "data_binary"));
        query.and(PairQueryCondition.StringQueryCondition.eq("trace_id", traceId));

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            StreamQueryResponse resp = client.query(query);
            Assert.assertNotNull(resp);
            Assert.assertEquals(1, resp.size());
            Assert.assertEquals(latency, (Number) resp.getElements().get(0).getTagValue("duration"));
            Assert.assertEquals(traceId, resp.getElements().get(0).getTagValue("trace_id"));
        });
    }

    @Test
    public void testStreamQuery_Str_In() throws BanyanDBException, ExecutionException, InterruptedException, TimeoutException {
        int size = 2;
        List<String> endpointIDs = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            StreamWrite streamWrite = writeStream();
            String segmentID = ((Value.StringTagValue) streamWrite.getTag("endpoint_id")).getValue();
            endpointIDs.add(segmentID);
        }

        StreamQuery query = new StreamQuery("default", "sw", ImmutableSet.of("state", "duration", "trace_id", "data_binary"));
        query.and(PairQueryCondition.StringArrayQueryCondition.in("endpoint_id", endpointIDs));

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            StreamQueryResponse resp = client.query(query);
            Assert.assertNotNull(resp);
            Assert.assertEquals(2, resp.size());
        });
    }

    private StreamWrite writeStream() throws ExecutionException, InterruptedException, TimeoutException, BanyanDBException {
        // try to write a trace
        String segmentId = UUID.randomUUID().toString();
        String traceId = "trace_id-" + UUID.randomUUID();
        String serviceId = "webapp_id";
        String serviceInstanceId = "10.0.0.1_id";
        String endpointId = "endpoint_id-" + UUID.randomUUID();
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

        StreamWrite streamWrite = new StreamWrite("default", "sw", segmentId, now.toEpochMilli())
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

        CompletableFuture<Void> f = processor.add(streamWrite);
        f.exceptionally(exp -> {
            Assert.fail(exp.getMessage());
            return null;
        });
        f.get(10, TimeUnit.SECONDS);
        return streamWrite;
    }
}
