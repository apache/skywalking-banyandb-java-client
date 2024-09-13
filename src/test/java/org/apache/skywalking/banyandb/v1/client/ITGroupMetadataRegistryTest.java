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
