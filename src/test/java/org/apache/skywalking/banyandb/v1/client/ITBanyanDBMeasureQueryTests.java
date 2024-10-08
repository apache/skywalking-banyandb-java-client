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
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Group;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Catalog;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.IntervalRule;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.ResourceOpts;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Metadata;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.CompressionMethod;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.EncodingMethod;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.Entity;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.FieldSpec;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.FieldType;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.Measure;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TagFamilySpec;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TagSpec;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TagType;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.metadata.Duration;
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
        super.setUpConnection();
        Group expectedGroup = buildGroup();
        client.define(expectedGroup);
        Assert.assertNotNull(expectedGroup);
        Measure expectedMeasure = buildMeasure();
        client.define(expectedMeasure);
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

    private Measure buildMeasure() {
        Measure.Builder builder = Measure.newBuilder()
                                         .setMetadata(Metadata.newBuilder()
                                                              .setGroup("sw_metric")
                                                              .setName("service_cpm_minute"))
                                         .setInterval(Duration.ofMinutes(1).format())
                                         .setEntity(Entity.newBuilder().addTagNames("entity_id"))
                                         .addTagFamilies(
                                             TagFamilySpec.newBuilder()
                                                          .setName("default")
                                                          .addTags(
                                                              TagSpec.newBuilder()
                                                                     .setName("entity_id")
                                                                     .setType(
                                                                         TagType.TAG_TYPE_STRING))
                                                          .addTags(
                                                              TagSpec.newBuilder()
                                                                     .setName("scope")
                                                                     .setType(
                                                                         TagType.TAG_TYPE_STRING)))
                                         .addFields(
                                             FieldSpec.newBuilder()
                                                      .setName("total")
                                                      .setFieldType(
                                                          FieldType.FIELD_TYPE_INT)
                                                      .setCompressionMethod(
                                                          CompressionMethod.COMPRESSION_METHOD_ZSTD)
                                                      .setEncodingMethod(
                                                          EncodingMethod.ENCODING_METHOD_GORILLA))
                                         .addFields(
                                             FieldSpec.newBuilder()
                                                      .setName("value")
                                                      .setFieldType(
                                                          FieldType.FIELD_TYPE_INT)
                                                      .setCompressionMethod(
                                                          CompressionMethod.COMPRESSION_METHOD_ZSTD)
                                                      .setEncodingMethod(
                                                          EncodingMethod.ENCODING_METHOD_GORILLA));
        return builder.build();
    }
}
