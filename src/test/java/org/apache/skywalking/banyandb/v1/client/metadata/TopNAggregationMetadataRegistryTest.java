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

package org.apache.skywalking.banyandb.v1.client.metadata;

import org.apache.skywalking.banyandb.v1.client.AbstractBanyanDBClientTest;
import org.apache.skywalking.banyandb.v1.client.AbstractQuery;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TopNAggregationMetadataRegistryTest extends AbstractBanyanDBClientTest {
    @Before
    public void setUp() throws IOException {
        this.setUp(bindTopNAggregationRegistry());
    }

    @Test
    public void testTopNAggregationRegistry_createAndGet() throws BanyanDBException {
        TopNAggregation expectedTopNAggregation = TopNAggregation.create("sw_metric", "service_cpm_minute_topn")
                .setFieldValueSort(AbstractQuery.Sort.DESC)
                .setFieldName("value")
                .setSourceMeasureName("service_cpm_minute")
                .setLruSize(10)
                .setCountersNumber(1000)
                .setGroupByTagNames("service_id")
                .build();
        this.client.define(expectedTopNAggregation);
        Assert.assertTrue(topNAggregationRegistry.containsKey("service_cpm_minute_topn"));
        TopNAggregation actualTopNAggregation = client.findTopNAggregation("sw_metric", "service_cpm_minute_topn");
        Assert.assertNotNull(actualTopNAggregation);
        Assert.assertEquals(expectedTopNAggregation, actualTopNAggregation);
        Assert.assertNotNull(actualTopNAggregation.updatedAt());
    }

    @Test
    public void testTopNAggregationRegistry_createAndList() throws BanyanDBException {
        TopNAggregation expectedTopNAggregation = TopNAggregation.create("sw_metric", "service_cpm_minute_topn")
                .setFieldValueSort(AbstractQuery.Sort.DESC)
                .setFieldName("value")
                .setSourceMeasureName("service_cpm_minute")
                .setLruSize(10)
                .setCountersNumber(1000)
                .setGroupByTagNames("service_id")
                .build();
        this.client.define(expectedTopNAggregation);
        List<TopNAggregation> actualTopNAggregations = new TopNAggregationMetadataRegistry(this.channel).list("sw_metric");
        Assert.assertNotNull(actualTopNAggregations);
        Assert.assertEquals(1, actualTopNAggregations.size());
    }

    @Test
    public void testTopNAggregationRegistry_createAndDelete() throws BanyanDBException {
        TopNAggregation expectedTopNAggregation = TopNAggregation.create("sw_metric", "service_cpm_minute_topn")
                .setFieldValueSort(AbstractQuery.Sort.DESC)
                .setFieldName("value")
                .setSourceMeasureName("service_cpm_minute")
                .setLruSize(10)
                .setCountersNumber(1000)
                .setGroupByTagNames("service_id")
                .build();
        this.client.define(expectedTopNAggregation);
        boolean deleted = new TopNAggregationMetadataRegistry(this.channel).delete("sw_metric", "service_cpm_minute_topn");
        Assert.assertTrue(deleted);
        Assert.assertEquals(0, topNAggregationRegistry.size());
    }
}
