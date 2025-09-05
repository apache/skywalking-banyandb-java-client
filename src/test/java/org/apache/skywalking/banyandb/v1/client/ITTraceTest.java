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
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.skywalking.banyandb.v1.client.BanyanDBClient.DEFAULT_EXPIRE_AT;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for trace functionality.
 * Note: This test demonstrates the trace API but requires a running BanyanDB instance.
 */
public class ITTraceTest extends BanyanDBClientTestCI {
    private final String groupName = "sw_trace";
    private final String traceName = "trace_data";
    private TraceBulkWriteProcessor processor;

    @Before
    public void setUp() throws IOException, BanyanDBException, InterruptedException {
        this.setUpConnection();
        // Create trace group
        BanyandbCommon.Group traceGroup = BanyandbCommon.Group.newBuilder()
            .setMetadata(BanyandbCommon.Metadata.newBuilder().setName(groupName))
            .setCatalog(BanyandbCommon.Catalog.CATALOG_TRACE)
            .setResourceOpts(BanyandbCommon.ResourceOpts.newBuilder()
                .setShardNum(2)
                .setSegmentInterval(BanyandbCommon.IntervalRule.newBuilder()
                    .setUnit(BanyandbCommon.IntervalRule.Unit.UNIT_DAY)
                    .setNum(1))
                .setTtl(BanyandbCommon.IntervalRule.newBuilder()
                    .setUnit(BanyandbCommon.IntervalRule.Unit.UNIT_DAY)
                    .setNum(7)))
            .build();
        
        this.client.define(traceGroup);
        
        // Create trace schema
        BanyandbDatabase.Trace trace = BanyandbDatabase.Trace.newBuilder()
            .setMetadata(BanyandbCommon.Metadata.newBuilder()
                .setGroup(groupName)
                .setName(traceName))
            .addTags(BanyandbDatabase.TraceTagSpec.newBuilder()
                .setName("trace_id")
                .setType(BanyandbDatabase.TagType.TAG_TYPE_STRING))
            .addTags(BanyandbDatabase.TraceTagSpec.newBuilder()
                .setName("span_id")
                .setType(BanyandbDatabase.TagType.TAG_TYPE_STRING))
            .addTags(BanyandbDatabase.TraceTagSpec.newBuilder()
                .setName("service_name")
                .setType(BanyandbDatabase.TagType.TAG_TYPE_STRING))
            .addTags(BanyandbDatabase.TraceTagSpec.newBuilder()
                .setName("start_time")
                .setType(BanyandbDatabase.TagType.TAG_TYPE_TIMESTAMP))
            .setTraceIdTagName("trace_id")
            .setTimestampTagName("start_time")
            .build();
            
        this.client.define(trace);
        this.client.define(buildIndexRule());
        this.client.define(buildIndexRuleBinding());
        
        processor = client.buildTraceWriteProcessor(1000, 1, 1, 10);
    }

    @After
    public void tearDown() throws IOException {
        if (processor != null) {
            processor.close();
        }
        this.closeClient();
    }

    @Test
    public void testTraceSchemaOperations() throws BanyanDBException {
        // Test trace definition exists
        BanyandbDatabase.Trace retrievedTrace = client.findTrace(groupName, traceName);
        Assert.assertNotNull("Trace should exist", retrievedTrace);
        Assert.assertEquals("Trace name should match", traceName, retrievedTrace.getMetadata().getName());
        Assert.assertEquals("Trace group should match", groupName, retrievedTrace.getMetadata().getGroup());
        
        // Test trace exists
        Assert.assertTrue("Trace should exist", client.existTrace(groupName, traceName).hasResource());
    }

    @Test
    public void testTraceQueryByTraceId() throws BanyanDBException, ExecutionException, InterruptedException, TimeoutException {
        // Test data
        String traceId = "trace-query-test-12345";
        String spanId = "span-query-test-67890";
        String serviceName = "query-test-service";
        Instant now = Instant.now();
        byte[] spanData = "query-test-span-data".getBytes();
        
        // Create and write trace data
        TraceWrite traceWrite = client.createTraceWrite(groupName, traceName)
            .tag("trace_id", Value.stringTagValue(traceId))
            .tag("span_id", Value.stringTagValue(spanId))
            .tag("service_name", Value.stringTagValue(serviceName))
            .tag("start_time", Value.timestampTagValue(now.toEpochMilli()))
            .span(spanData)
            .version(1L);
            
        // Write the trace via bulk processor
        CompletableFuture<Void> writeFuture = processor.add(traceWrite);
        writeFuture.exceptionally(exp -> {
            Assert.fail("Write failed: " + exp.getMessage());
            return null;
        });
        writeFuture.get(10, TimeUnit.SECONDS);
        
        // Create trace query with trace_id condition
        TraceQuery query = new TraceQuery(
            Lists.newArrayList(groupName), 
            traceName,
            Collections.emptySet()
        );
        query.and(PairQueryCondition.StringQueryCondition.eq("trace_id", traceId));

        // Execute query with conditions
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            TraceQueryResponse response = client.query(query);
            Assert.assertNotNull("Query response should not be null", response);
            Assert.assertFalse("Should have at least one result", response.isEmpty());
            Assert.assertEquals("Should have exactly one span", 1, response.size());
            
            // Verify we can access span data
            Assert.assertNotNull("Spans list should not be null", response.getSpans());
            Assert.assertEquals("Should have one span in list", 1, response.getSpans().size());
            
            // Get the first span and verify its contents
            org.apache.skywalking.banyandb.trace.v1.BanyandbTrace.Span span = response.getSpans().get(0);
            Assert.assertNotNull("Span should not be null", span);
            
            // Verify span data (binary content) - this is the main content returned
            Assert.assertNotNull("Span data should not be null", span.getSpan());
            Assert.assertFalse("Span data should not be empty", span.getSpan().isEmpty());
            Assert.assertArrayEquals("Span data should match", spanData, span.getSpan().toByteArray());
        });
    }

    @Test
    public void testTraceQueryOrderByStartTime() throws BanyanDBException, ExecutionException, InterruptedException, TimeoutException {
        // Test data with different timestamps
        String traceId = "trace-order-test-";
        String serviceName = "order-test-service";
        Instant baseTime = Instant.now().minusSeconds(60); // Start 1 minute ago
        
        // Create 3 traces with different timestamps (1 minute apart)
        TraceWrite trace1 = client.createTraceWrite(groupName, traceName)
            .tag("trace_id", Value.stringTagValue(traceId + "1"))
            .tag("span_id", Value.stringTagValue("span-1"))
            .tag("service_name", Value.stringTagValue(serviceName))
            .tag("start_time", Value.timestampTagValue(baseTime.toEpochMilli()))
            .span("span-data-1".getBytes())
            .version(1L);
            
        TraceWrite trace2 = client.createTraceWrite(groupName, traceName)
            .tag("trace_id", Value.stringTagValue(traceId + "2"))
            .tag("span_id", Value.stringTagValue("span-2"))
            .tag("service_name", Value.stringTagValue(serviceName))
            .tag("start_time", Value.timestampTagValue(baseTime.plusSeconds(60).toEpochMilli()))
            .span("span-data-2".getBytes())
            .version(1L);
            
        TraceWrite trace3 = client.createTraceWrite(groupName, traceName)
            .tag("trace_id", Value.stringTagValue(traceId + "3"))
            .tag("span_id", Value.stringTagValue("span-3"))
            .tag("service_name", Value.stringTagValue(serviceName))
            .tag("start_time", Value.timestampTagValue(baseTime.plusSeconds(120).toEpochMilli()))
            .span("span-data-3".getBytes())
            .version(1L);
        
        // Write the traces via bulk processor
        CompletableFuture<Void> future1 = processor.add(trace1);
        CompletableFuture<Void> future2 = processor.add(trace2);
        CompletableFuture<Void> future3 = processor.add(trace3);
        
        CompletableFuture.allOf(future1, future2, future3).get(10, TimeUnit.SECONDS);
        
        // Create trace query with order by start_time (no trace_id condition as it interferes with ordering)
        TraceQuery query = new TraceQuery(
            Lists.newArrayList(groupName), 
            traceName,
            new TimestampRange(baseTime.toEpochMilli(), baseTime.plusSeconds(60).toEpochMilli()),
            ImmutableSet.of("start_time")
        );
        query.setOrderBy(new AbstractQuery.OrderBy("start_time", AbstractQuery.Sort.DESC));

        // Execute query and verify ordering
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            TraceQueryResponse response = client.query(query);
            Assert.assertNotNull("Query response should not be null", response);
            Assert.assertFalse("Should have at least one result", response.isEmpty());
            Assert.assertTrue("Should have exactly 2 spans", response.size() == 2);
            
            // Verify we can access span data
            Assert.assertNotNull("Spans list should not be null", response.getSpans());
            Assert.assertTrue("Should have exactly 2 spans in list", response.getSpans().size() == 2);
            
            // Verify that span content matches expected data
            String firstSpanContent = new String(response.getSpans().get(0).getSpan().toByteArray());
            String secondSpanContent = new String(response.getSpans().get(1).getSpan().toByteArray());
            
            // Since we're ordering by start_time DESC, span-data-2 should come before span-data-1
            // (baseTime+60 > baseTime)
            Assert.assertEquals("First span should be span-data-2 (newer timestamp)", "span-data-2", firstSpanContent);
            Assert.assertEquals("Second span should be span-data-1 (older timestamp)", "span-data-1", secondSpanContent);
        });
    }

    private BanyandbDatabase.IndexRule buildIndexRule() {
        return BanyandbDatabase.IndexRule.newBuilder()
                .setMetadata(BanyandbCommon.Metadata.newBuilder()
                        .setGroup(groupName)
                        .setName("start_time"))
                .addTags("start_time")
                .setType(BanyandbDatabase.IndexRule.Type.TYPE_TREE)
                .build();
    }
    
    private BanyandbDatabase.IndexRuleBinding buildIndexRuleBinding() {
        return BanyandbDatabase.IndexRuleBinding.newBuilder()
                .setMetadata(BanyandbCommon.Metadata.newBuilder()
                        .setGroup(groupName)
                        .setName("trace_binding"))
                .setSubject(BanyandbDatabase.Subject.newBuilder()
                        .setCatalog(BanyandbCommon.Catalog.CATALOG_TRACE)
                        .setName(traceName))
                .addAllRules(Arrays.asList("start_time"))
                .setBeginAt(TimeUtils.buildTimestamp(ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)))
                .setExpireAt(TimeUtils.buildTimestamp(DEFAULT_EXPIRE_AT))
                .build();
    }
}