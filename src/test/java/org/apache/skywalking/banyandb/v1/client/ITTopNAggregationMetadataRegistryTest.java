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
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TopNAggregation;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ITTopNAggregationMetadataRegistryTest extends BanyanDBClientTestCI {
    @Before
    public void setUp() throws IOException, BanyanDBException, InterruptedException {
        super.setUpConnection();
        BanyandbCommon.Group expectedGroup = buildMeasureGroup();
        client.define(expectedGroup);
        Assert.assertNotNull(expectedGroup);
    }

    @After
    public void tearDown() throws IOException {
        this.closeClient();
    }

    @Test
    public void testTopNAggregationRegistry_createAndGet() throws BanyanDBException {
        TopNAggregation expectedTopNAggregation = bindTopNAggregation();
        this.client.define(expectedTopNAggregation);
        TopNAggregation actualTopNAggregation = client.findTopNAggregation("sw_metric", "service_cpm_minute_topn");
        Assert.assertNotNull(actualTopNAggregation);
        Assert.assertNotNull(actualTopNAggregation.getUpdatedAt());
        actualTopNAggregation = actualTopNAggregation.toBuilder().clearUpdatedAt().setMetadata(actualTopNAggregation.getMetadata().toBuilder().clearModRevision().clearCreateRevision()).build();
        Assert.assertEquals(expectedTopNAggregation, actualTopNAggregation);
    }

    @Test
    public void testTopNAggregationRegistry_createAndUpdate() throws BanyanDBException {
        TopNAggregation expectedTopNAggregation = bindTopNAggregation();
        this.client.define(expectedTopNAggregation);
        TopNAggregation before = client.findTopNAggregation("sw_metric", "service_cpm_minute_topn");
        Assert.assertNotNull(before);
        Assert.assertNotNull(before.getUpdatedAt());
        TopNAggregation updated = before.toBuilder().setFieldValueSort(BanyandbModel.Sort.SORT_ASC).build();
        this.client.update(updated);
        TopNAggregation after = client.findTopNAggregation("sw_metric", "service_cpm_minute_topn");
        Assert.assertNotNull(after);
        Assert.assertNotNull(after.getUpdatedAt());
        Assert.assertNotEquals(before, after);
        Assert.assertEquals(BanyandbModel.Sort.SORT_ASC, after.getFieldValueSort());
    }

    @Test
    public void testTopNAggregationRegistry_createAndList() throws BanyanDBException {
        TopNAggregation expectedTopNAggregation = bindTopNAggregation();
        this.client.define(expectedTopNAggregation);
        List<TopNAggregation> actualTopNAggregations = client.findTopNAggregations("sw_metric");
        Assert.assertNotNull(actualTopNAggregations);
        Assert.assertEquals(1, actualTopNAggregations.size());
    }

    @Test
    public void testTopNAggregationRegistry_createAndDelete() throws BanyanDBException {
        TopNAggregation expectedTopNAggregation = bindTopNAggregation();
        this.client.define(expectedTopNAggregation);
        boolean deleted = this.client.deleteTopNAggregation(
            expectedTopNAggregation.getMetadata().getGroup(), expectedTopNAggregation.getMetadata().getName());
        Assert.assertTrue(deleted);
        Assert.assertNull(client.findMeasure(expectedTopNAggregation.getMetadata().getGroup(),
                                             expectedTopNAggregation.getMetadata().getName()
        ));
    }

    private TopNAggregation bindTopNAggregation() {
        TopNAggregation.Builder builder = TopNAggregation.newBuilder()
                                                         .setMetadata(Metadata.newBuilder()
                                                                              .setGroup("sw_metric")
                                                                              .setName("service_cpm_minute_topn"))
                                                         .setFieldValueSort(BanyandbModel.Sort.SORT_DESC)
                                                         .setFieldName("value")
                                                         .setSourceMeasure(Metadata.newBuilder()
                                                                                   .setGroup("sw_metric")
                                                                                   .setName("service_cpm_minute"))
                                                         .setLruSize(10)
                                                         .setCountersNumber(1000)
                                                         .addGroupByTagNames("service_id");
        return builder.build();
    }
}
