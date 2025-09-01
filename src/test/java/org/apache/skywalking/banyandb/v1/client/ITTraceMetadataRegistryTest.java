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

import java.io.IOException;
import java.util.List;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Metadata;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Catalog;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.IntervalRule;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.ResourceOpts;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.Trace;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TraceTagSpec;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TagType;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.metadata.ResourceExist;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ITTraceMetadataRegistryTest extends BanyanDBClientTestCI {
    private final String groupName = "sw_trace_registry";
    private final String traceName = "trace_registry_test";

    @Before
    public void setUp() throws IOException, BanyanDBException, InterruptedException {
        super.setUpConnection();
        BanyandbCommon.Group expectedGroup = buildTraceGroup();
        client.define(expectedGroup);
        Assert.assertNotNull(expectedGroup);
    }

    @After
    public void tearDown() throws IOException {
        this.closeClient();
    }

    @Test
    public void testTraceRegistry_createAndGet() throws BanyanDBException {
        Trace expectedTrace = buildTrace();
        this.client.define(expectedTrace);
        Trace actualTrace = client.findTrace(groupName, traceName);
        Assert.assertNotNull(actualTrace);
        Assert.assertNotNull(actualTrace.getUpdatedAt());
        
        // Clear timestamps and revision fields for comparison
        actualTrace = actualTrace.toBuilder()
            .clearUpdatedAt()
            .setMetadata(actualTrace.getMetadata().toBuilder()
                .clearModRevision()
                .clearCreateRevision())
            .build();
        Assert.assertEquals(expectedTrace, actualTrace);
    }

    @Test
    public void testTraceRegistry_createAndUpdate() throws BanyanDBException {
        Trace expectedTrace = buildTrace();
        this.client.define(expectedTrace);
        Trace beforeTrace = client.findTrace(groupName, traceName);
        Assert.assertNotNull(beforeTrace);
        Assert.assertNotNull(beforeTrace.getUpdatedAt());
        
        // Add an additional tag
        Trace updatedTrace = beforeTrace.toBuilder()
            .addTags(TraceTagSpec.newBuilder()
                .setName("duration")
                .setType(TagType.TAG_TYPE_INT))
            .build();
        
        this.client.update(updatedTrace);
        Trace afterTrace = client.findTrace(groupName, traceName);
        Assert.assertNotNull(afterTrace);
        Assert.assertNotNull(afterTrace.getUpdatedAt());
        Assert.assertNotEquals(beforeTrace, afterTrace);
        Assert.assertEquals(beforeTrace.getTagsCount() + 1, afterTrace.getTagsCount());
    }

    @Test
    public void testTraceRegistry_list() throws BanyanDBException {
        Trace expectedTrace = buildTrace();
        this.client.define(expectedTrace);
        
        List<Trace> traces = client.findTraces(groupName);
        Assert.assertNotNull(traces);
        Assert.assertTrue(traces.size() >= 1);
        
        boolean found = false;
        for (Trace trace : traces) {
            if (traceName.equals(trace.getMetadata().getName())) {
                found = true;
                break;
            }
        }
        Assert.assertTrue("Expected trace not found in list", found);
    }

    @Test
    public void testTraceRegistry_exist() throws BanyanDBException {
        Trace expectedTrace = buildTrace();
        this.client.define(expectedTrace);
        
        ResourceExist exist = client.existTrace(groupName, traceName);
        Assert.assertNotNull(exist);
        Assert.assertTrue(exist.hasGroup());
        Assert.assertTrue(exist.hasResource());
        
        // Test non-existent trace
        ResourceExist nonExist = client.existTrace(groupName, "non-existent-trace");
        Assert.assertNotNull(nonExist);
        Assert.assertTrue(nonExist.hasGroup());
        Assert.assertFalse(nonExist.hasResource());
    }

    @Test
    public void testTraceRegistry_delete() throws BanyanDBException {
        Trace expectedTrace = buildTrace();
        this.client.define(expectedTrace);
        
        // Verify it exists
        Assert.assertNotNull(client.findTrace(groupName, traceName));
        
        // Delete it
        boolean deleted = client.deleteTrace(groupName, traceName);
        Assert.assertTrue(deleted);
        
        // Verify it no longer exists
        Assert.assertNull(client.findTrace(groupName, traceName));
    }

    private BanyandbCommon.Group buildTraceGroup() {
        return BanyandbCommon.Group.newBuilder()
            .setMetadata(Metadata.newBuilder().setName(groupName))
            .setCatalog(Catalog.CATALOG_TRACE)
            .setResourceOpts(ResourceOpts.newBuilder()
                .setShardNum(2)
                .setSegmentInterval(IntervalRule.newBuilder()
                    .setUnit(IntervalRule.Unit.UNIT_DAY)
                    .setNum(1))
                .setTtl(IntervalRule.newBuilder()
                    .setUnit(IntervalRule.Unit.UNIT_DAY)
                    .setNum(7)))
            .build();
    }

    private Trace buildTrace() {
        return Trace.newBuilder()
            .setMetadata(Metadata.newBuilder()
                .setGroup(groupName)
                .setName(traceName))
            .addTags(TraceTagSpec.newBuilder()
                .setName("trace_id")
                .setType(TagType.TAG_TYPE_STRING))
            .addTags(TraceTagSpec.newBuilder()
                .setName("span_id")
                .setType(TagType.TAG_TYPE_STRING))
            .addTags(TraceTagSpec.newBuilder()
                .setName("service_name")
                .setType(TagType.TAG_TYPE_STRING))
            .addTags(TraceTagSpec.newBuilder()
                .setName("operation_name")
                .setType(TagType.TAG_TYPE_STRING))
            .addTags(TraceTagSpec.newBuilder()
                .setName("start_time")
                .setType(TagType.TAG_TYPE_TIMESTAMP))
            .setTraceIdTagName("trace_id")
            .setTimestampTagName("start_time")
            .build();
    }
}