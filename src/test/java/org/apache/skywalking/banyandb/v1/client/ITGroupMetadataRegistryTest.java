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
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Group;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Catalog;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.IntervalRule;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.ResourceOpts;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Metadata;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.LifecycleStage;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ITGroupMetadataRegistryTest extends BanyanDBClientTestCI {
    @Before
    public void setUp() throws IOException, BanyanDBException, InterruptedException {
        super.setUpConnection();
    }

    @After
    public void tearDown() throws IOException {
        this.closeClient();
    }

    @Test
    public void testGroupRegistry_createAndGet() throws BanyanDBException {
        Group expectedGroup = buildGroup();
        this.client.define(expectedGroup);
        Group actualGroup = client.findGroup("sw_metric");
        Assert.assertNotNull(actualGroup);
        Assert.assertNotNull(actualGroup.getUpdatedAt());
        actualGroup = actualGroup.toBuilder().setMetadata(actualGroup.getMetadata().toBuilder().clearModRevision().clearCreateRevision()).build();
        Assert.assertEquals(expectedGroup, actualGroup);
    }

    @Test
    public void testGroupRegistry_createAndUpdate() throws BanyanDBException {
        this.client.define(buildGroup());
        Group beforeGroup = client.findGroup("sw_metric");
        Assert.assertNotNull(beforeGroup);
        Assert.assertNotNull(beforeGroup.getUpdatedAt());
        Group updatedGroup = beforeGroup.toBuilder()
                                        .setResourceOpts(beforeGroup.getResourceOpts()
                                                                    .toBuilder()
                                                                    .setTtl(IntervalRule.newBuilder()
                                                                                        .setUnit(
                                                                                            IntervalRule.Unit.UNIT_DAY)
                                                                                        .setNum(3)))
                                        .build();
        this.client.update(updatedGroup);
        Group afterGroup = client.findGroup("sw_metric");
        updatedGroup = updatedGroup.toBuilder().setMetadata(updatedGroup.getMetadata().toBuilder().clearModRevision()).build();
        afterGroup = afterGroup.toBuilder().setMetadata(afterGroup.getMetadata().toBuilder().clearModRevision()).build();
        Assert.assertNotNull(afterGroup);
        Assert.assertNotNull(afterGroup.getUpdatedAt());
        Assert.assertEquals(updatedGroup, afterGroup);
    }

    @Test
    public void testGroupRegistry_createAndList() throws BanyanDBException {
        Group expectedGroup = buildGroup();
        this.client.define(buildGroup());
        List<Group> actualGroups = client.findGroups();
        Assert.assertNotNull(actualGroups);
        Assert.assertEquals(1, actualGroups.size());
        Group actualGroup = actualGroups.get(0);
        actualGroup = actualGroup.toBuilder().setMetadata(actualGroup.getMetadata().toBuilder().clearModRevision().clearCreateRevision()).build();
        Assert.assertEquals(expectedGroup, actualGroup);
    }

    @Test
    public void testGroupRegistry_createAndDelete() throws BanyanDBException {
        this.client.define(buildGroup());
        boolean deleted = this.client.deleteGroup("sw_metric");
        Assert.assertTrue(deleted);
        Assert.assertNull(client.findGroup("sw_metric"));
    }

    @Test
    public void testGroupRegistry_hotWarmCold() throws BanyanDBException {
        Group g = Group.newBuilder().setMetadata(Metadata.newBuilder().setName("sw_record"))
            .setCatalog(Catalog.CATALOG_STREAM)
            .setResourceOpts(ResourceOpts.newBuilder()
                .setShardNum(3)
                .setSegmentInterval(
                    IntervalRule.newBuilder()
                        .setUnit(IntervalRule.Unit.UNIT_DAY)
                        .setNum(1))
                .setTtl(
                    IntervalRule.newBuilder()
                        .setUnit(IntervalRule.Unit.UNIT_DAY)
                        .setNum(3))
                .addStages(LifecycleStage.newBuilder()
                    .setName("warm")
                    .setShardNum(2)
                    .setSegmentInterval(IntervalRule.newBuilder()
                        .setUnit(IntervalRule.Unit.UNIT_DAY)
                        .setNum(1))
                    .setTtl(IntervalRule.newBuilder()
                        .setUnit(IntervalRule.Unit.UNIT_DAY)
                        .setNum(7))
                    .setNodeSelector("hdd-nodes")
                    .build())
                .addStages(LifecycleStage.newBuilder()
                    .setName("cold")
                    .setShardNum(1)
                    .setSegmentInterval(IntervalRule.newBuilder()
                        .setUnit(IntervalRule.Unit.UNIT_DAY)
                        .setNum(7))
                    .setTtl(IntervalRule.newBuilder()
                        .setUnit(IntervalRule.Unit.UNIT_DAY)
                        .setNum(30))
                    .setNodeSelector("archive-nodes")
                    .setClose(true)
                    .build()))
            .build();
        this.client.define(g);
        Group actualGroup = client.findGroup("sw_record");
        Assert.assertNotNull(actualGroup);
        Assert.assertNotNull(actualGroup.getUpdatedAt());

        // Verify group exists
        Assert.assertNotNull(actualGroup);

        // Verify basic metadata
        Assert.assertEquals("sw_record", actualGroup.getMetadata().getName());
        Assert.assertEquals(Catalog.CATALOG_STREAM, actualGroup.getCatalog());

        // Verify resource options
        ResourceOpts actualOpts = actualGroup.getResourceOpts();
        Assert.assertEquals(3, actualOpts.getShardNum());
        Assert.assertEquals(IntervalRule.Unit.UNIT_DAY, actualOpts.getSegmentInterval().getUnit());
        Assert.assertEquals(1, actualOpts.getSegmentInterval().getNum());
        Assert.assertEquals(IntervalRule.Unit.UNIT_DAY, actualOpts.getTtl().getUnit());
        Assert.assertEquals(3, actualOpts.getTtl().getNum());

        // Verify stages (should have 2 stages: warm and cold)
        Assert.assertEquals(2, actualOpts.getStagesCount());

        // Verify warm stage
        LifecycleStage warmStage = actualOpts.getStages(0);
        Assert.assertEquals("warm", warmStage.getName());
        Assert.assertEquals(2, warmStage.getShardNum());
        Assert.assertEquals(1, warmStage.getSegmentInterval().getNum());
        Assert.assertEquals(IntervalRule.Unit.UNIT_DAY, warmStage.getSegmentInterval().getUnit());
        Assert.assertEquals(7, warmStage.getTtl().getNum());
        Assert.assertEquals("hdd-nodes", warmStage.getNodeSelector());

        // Verify cold stage
        LifecycleStage coldStage = actualOpts.getStages(1);
        Assert.assertEquals("cold", coldStage.getName());
        Assert.assertEquals(1, coldStage.getShardNum());
        Assert.assertEquals(7, coldStage.getSegmentInterval().getNum());
        Assert.assertEquals(30, coldStage.getTtl().getNum());
        Assert.assertEquals("archive-nodes", coldStage.getNodeSelector());
        Assert.assertTrue(coldStage.getClose());
    }

    private Group buildGroup() {
        return Group.newBuilder().setMetadata(Metadata.newBuilder().setName("sw_metric"))
                    .setCatalog(Catalog.CATALOG_MEASURE)
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
                                                                     7)))
                    .build();
    }
}
