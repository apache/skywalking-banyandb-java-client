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
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class MeasureMetadataRegistryTest extends AbstractBanyanDBClientTest {
    @Before
    public void setUp() throws IOException {
        this.setUp(bindMeasureRegistry());
    }

    @Test
    public void testMeasureRegistry_createAndGet() throws BanyanDBException {
        Measure expectedMeasure = Measure.create("sw_metric", "service_cpm_minute", Duration.ofHours(1))
                .setEntityRelativeTags("entity_id")
                .addTagFamily(TagFamilySpec.create("default")
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("entity_id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("scope"))
                        .build())
                .addField(Measure.FieldSpec.newIntField("total").compressWithZSTD().encodeWithGorilla().build())
                .addField(Measure.FieldSpec.newIntField("value").compressWithZSTD().encodeWithGorilla().build())
                .addField(Measure.FieldSpec.newFloatField("sum").compressWithZSTD().encodeWithGorilla().build())
                .addIndex(IndexRule.create("scope", IndexRule.IndexType.INVERTED))
                .build();
        this.client.define(expectedMeasure);
        Assert.assertTrue(measureRegistry.containsKey("service_cpm_minute"));
        Measure actualMeasure = client.findMeasure("sw_metric", "service_cpm_minute");
        Assert.assertNotNull(actualMeasure);
        Assert.assertEquals(expectedMeasure, actualMeasure);
        Assert.assertNotNull(actualMeasure.updatedAt());
        Assert.assertNotNull(actualMeasure.modRevision());
    }

    @Test
    public void testMeasureRegistry_createAndList() throws BanyanDBException {
        Measure expectedMeasure = Measure.create("sw_metric", "service_cpm_minute", Duration.ofHours(1))
                .setEntityRelativeTags("entity_id")
                .addTagFamily(TagFamilySpec.create("default")
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("entity_id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("scope"))
                        .build())
                .addField(Measure.FieldSpec.newIntField("total").compressWithZSTD().encodeWithGorilla().build())
                .addField(Measure.FieldSpec.newIntField("value").compressWithZSTD().encodeWithGorilla().build())
                .addField(Measure.FieldSpec.newFloatField("sum").compressWithZSTD().encodeWithGorilla().build())
                .addIndex(IndexRule.create("scope", IndexRule.IndexType.INVERTED))
                .build();
        this.client.define(expectedMeasure);
        List<Measure> actualMeasures = new MeasureMetadataRegistry(this.channel).list("sw_metric");
        Assert.assertNotNull(actualMeasures);
        Assert.assertEquals(1, actualMeasures.size());
        actualMeasures.forEach(measure -> Assert.assertNotNull(measure.modRevision()));
    }

    @Test
    public void testMeasureRegistry_createAndDelete() throws BanyanDBException {
        Measure expectedMeasure = Measure.create("sw_metric", "service_cpm_minute", Duration.ofHours(1))
                .setEntityRelativeTags("entity_id")
                .addTagFamily(TagFamilySpec.create("default")
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("entity_id"))
                        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("scope"))
                        .build())
                .addField(Measure.FieldSpec.newIntField("total").compressWithZSTD().encodeWithGorilla().build())
                .addField(Measure.FieldSpec.newIntField("value").compressWithZSTD().encodeWithGorilla().build())
                .addField(Measure.FieldSpec.newFloatField("sum").compressWithZSTD().encodeWithGorilla().build())
                .addIndex(IndexRule.create("scope", IndexRule.IndexType.INVERTED))
                .build();
        this.client.define(expectedMeasure);
        boolean deleted = this.client.delete(expectedMeasure);
        Assert.assertTrue(deleted);
        Assert.assertEquals(0, measureRegistry.size());
    }
}
