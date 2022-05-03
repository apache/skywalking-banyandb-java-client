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

import io.grpc.stub.StreamObserver;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.measure.v1.MeasureServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.metadata.Duration;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRule;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleBinding;
import org.apache.skywalking.banyandb.v1.client.metadata.Measure;
import org.apache.skywalking.banyandb.v1.client.metadata.TagFamilySpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BanyanDBClientMeasureWriteTest extends AbstractBanyanDBClientTest {
    private MeasureBulkWriteProcessor measureBulkWriteProcessor;

    @Before
    public void setUp() throws IOException, BanyanDBException {
        measureRegistry = new HashMap<>();
        setUp(bindMeasureRegistry());

        measureBulkWriteProcessor = client.buildMeasureWriteProcessor(1000, 1, 1);

        Measure m = Measure.create("sw_metric", "service_cpm_minute", Duration.ofHours(1))
                .setEntityRelativeTags("entity_id")
                .addTagFamily(TagFamilySpec.create("default")
                        .addTagSpec(TagFamilySpec.TagSpec.newIDTag("id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("entity_id"))
                        .build())
                .addField(Measure.FieldSpec.newIntField("total").compressWithZSTD().encodeWithGorilla().build())
                .addField(Measure.FieldSpec.newIntField("value").compressWithZSTD().encodeWithGorilla().build())
                .addIndex(IndexRule.create("scope", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES))
                .build();
        client.define(m);
    }

    @After
    public void shutdown() throws IOException {
        measureBulkWriteProcessor.close();
    }

    @Test
    public void testRegistry() {
        Assert.assertEquals(indexRuleBindingRegistry.size(), 1);
        Assert.assertTrue(indexRuleBindingRegistry.containsKey(IndexRuleBinding.defaultBindingRule("service_cpm_minute")));
        Assert.assertEquals(indexRuleBindingRegistry.get(IndexRuleBinding.defaultBindingRule("service_cpm_minute")).getSubject().getCatalog(),
                BanyandbCommon.Catalog.CATALOG_MEASURE);
    }

    @Test
    public void testWrite() throws Exception {
        final CountDownLatch allRequestsDelivered = new CountDownLatch(1);
        final List<BanyandbMeasure.WriteRequest> writeRequestDelivered = new ArrayList<>();

        // implement the fake service
        final MeasureServiceGrpc.MeasureServiceImplBase serviceImpl =
                new MeasureServiceGrpc.MeasureServiceImplBase() {
                    @Override
                    public StreamObserver<BanyandbMeasure.WriteRequest> write(StreamObserver<BanyandbMeasure.WriteResponse> responseObserver) {
                        return new StreamObserver<BanyandbMeasure.WriteRequest>() {
                            @Override
                            public void onNext(BanyandbMeasure.WriteRequest value) {
                                writeRequestDelivered.add(value);
                                responseObserver.onNext(BanyandbMeasure.WriteResponse.newBuilder().build());
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                                responseObserver.onCompleted();
                                allRequestsDelivered.countDown();
                            }
                        };
                    }
                };
        serviceRegistry.addService(serviceImpl);

        Instant now = Instant.now();
        MeasureWrite measureWrite = new MeasureWrite("sw_metric", "service_cpm_minute", now.toEpochMilli());
        measureWrite.tag("id", TagAndValue.idTagValue("1"))
                .tag("entity_id", TagAndValue.stringTagValue("entity_1"))
                .field("total", TagAndValue.longFieldValue(100))
                .field("value", TagAndValue.longFieldValue(1));

        measureBulkWriteProcessor.add(measureWrite);

        if (allRequestsDelivered.await(5, TimeUnit.SECONDS)) {
            Assert.assertEquals(1, writeRequestDelivered.size());
            final BanyandbMeasure.WriteRequest request = writeRequestDelivered.get(0);
            Assert.assertEquals(2, request.getDataPoint().getTagFamilies(0).getTagsCount());
            Assert.assertEquals("1", request.getDataPoint().getTagFamilies(0).getTags(0).getId().getValue());
            Assert.assertEquals("entity_1", request.getDataPoint().getTagFamilies(0).getTags(1).getStr().getValue());
            Assert.assertEquals(100, request.getDataPoint().getFields(0).getInt().getValue());
            Assert.assertEquals(1, request.getDataPoint().getFields(1).getInt().getValue());
        } else {
            Assert.fail();
        }
    }
}
