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
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.metadata.Catalog;
import org.apache.skywalking.banyandb.v1.client.metadata.Duration;
import org.apache.skywalking.banyandb.v1.client.metadata.Group;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRule;
import org.apache.skywalking.banyandb.v1.client.metadata.IntervalRule;
import org.apache.skywalking.banyandb.v1.client.metadata.Measure;
import org.apache.skywalking.banyandb.v1.client.metadata.TagFamilySpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.await;

public class ITBanyanDBMeasureQueryTests extends BanyanDBClientTestCI {
    private MeasureBulkWriteProcessor processor;

    @Before
    public void setUp() throws IOException, BanyanDBException, InterruptedException {
        this.setUpConnection();
        Group expectedGroup = this.client.define(Group.create("sw_metric", Catalog.MEASURE, 2, IntervalRule.create(IntervalRule.Unit.HOUR, 4), IntervalRule.create(IntervalRule.Unit.DAY, 7)));
        Assert.assertNotNull(expectedGroup);
        Measure expectedMeasure = Measure.create("sw_metric", "service_cpm_minute", Duration.ofMinutes(1)).setEntityRelativeTags("entity_id").addTagFamily(TagFamilySpec.create("default").addTagSpec(TagFamilySpec.TagSpec.newStringTag("entity_id")).build()).addField(Measure.FieldSpec.newIntField("total").compressWithZSTD().encodeWithGorilla().build()).addField(Measure.FieldSpec.newIntField("value").compressWithZSTD().encodeWithGorilla().build()).addIndex(IndexRule.create("scope", IndexRule.IndexType.INVERTED)).build();
        client.define(expectedMeasure);
        Assert.assertNotNull(expectedMeasure);
        processor = client.buildMeasureWriteProcessor(1000, 1, 1, 10);
    }

    @After
    public void tearDown() throws IOException {
        if (this.processor != null) {
            this.processor.close();
        }
        this.closeClient();
    }

    @Test
    public void testMeasureQuery() throws BanyanDBException, ExecutionException, InterruptedException, TimeoutException {
        // try to write a metrics
        Instant now = Instant.now();
        Instant begin = now.minus(15, ChronoUnit.MINUTES);

        MeasureWrite measureWrite = client.createMeasureWrite("sw_metric", "service_cpm_minute", now.toEpochMilli());
        measureWrite.tag("entity_id", TagAndValue.stringTagValue("entity_1")).field("total", TagAndValue.longFieldValue(100)).field("value", TagAndValue.longFieldValue(1));

        CompletableFuture<Void> f = processor.add(measureWrite);
        f.exceptionally(exp -> {
            Assert.fail(exp.getMessage());
            return null;
        });
        f.get(10, TimeUnit.SECONDS);

        MeasureQuery query = new MeasureQuery(Lists.newArrayList("sw_metric"), "service_cpm_minute", new TimestampRange(begin.toEpochMilli(), now.plus(1, ChronoUnit.MINUTES).toEpochMilli()), ImmutableSet.of("entity_id"), // tags
                ImmutableSet.of("total")); // fields

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            MeasureQueryResponse resp = client.query(query);
            Assert.assertNotNull(resp);
            Assert.assertEquals(1, resp.size());
        });
    }
}
