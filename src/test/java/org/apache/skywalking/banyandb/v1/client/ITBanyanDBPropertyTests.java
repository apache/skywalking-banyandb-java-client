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

import org.apache.skywalking.banyandb.model.v1.BanyandbModel.Tag;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel.TagValue;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel.Str;
import org.apache.skywalking.banyandb.property.v1.BanyandbProperty;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Group;
import org.apache.skywalking.banyandb.property.v1.BanyandbProperty.Property;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Metadata;

import static org.awaitility.Awaitility.await;

public class ITBanyanDBPropertyTests extends BanyanDBClientTestCI {
    @Before
    public void setUp() throws IOException, BanyanDBException, InterruptedException {
        super.setUpConnection();
        Group expectedGroup =
            Group.newBuilder().setMetadata(Metadata.newBuilder()
                            .setName("default"))
                    .setCatalog(BanyandbCommon.Catalog.CATALOG_PROPERTY)
                    .setResourceOpts(BanyandbCommon.ResourceOpts.newBuilder()
                            .setShardNum(2))
                    .build();
        client.define(expectedGroup);
        Assert.assertNotNull(expectedGroup);
    }

    @After
    public void tearDown() throws IOException {
        this.closeClient();
    }

    @Test
    public void test_PropertyCreateAndGet() throws BanyanDBException {
        Property property = buildProperty("default", "sw", "ui_template").toBuilder().addTags(
            Tag.newBuilder().setKey("name").setValue(
                TagValue.newBuilder().setStr(Str.newBuilder().setValue("hello")))).build();
        Assert.assertTrue(this.client.apply(property).getCreated());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            BanyandbProperty.QueryResponse resp = client.query(BanyandbProperty.QueryRequest.newBuilder()
                            .addGroups("default")
                            .setContainer("sw")
                            .addIds("ui_template")
                    .build());
            Assert.assertEquals(1, resp.getPropertiesCount());
            Property gotProperty = resp.getProperties(0);
            Assert.assertNotNull(gotProperty);
            Assert.assertEquals(property.getTagsList(), gotProperty.getTagsList());
        });
    }

    @Test
    public void test_PropertyCreateDeleteAndGet() throws BanyanDBException {
        Property property = buildProperty("default", "sw", "ui_template").toBuilder().addTags(
            Tag.newBuilder().setKey("name").setValue(
                TagValue.newBuilder().setStr(Str.newBuilder().setValue("hello")))).build();
        Assert.assertTrue(this.client.apply(property).getCreated());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            BanyandbProperty.QueryResponse resp = client.query(BanyandbProperty.QueryRequest.newBuilder()
                    .addGroups("default")
                    .setContainer("sw")
                    .addIds("ui_template")
                    .build());
            Assert.assertEquals(1, resp.getPropertiesCount());
            Property gotProperty = resp.getProperties(0);
            Assert.assertNotNull(gotProperty);
            Assert.assertEquals(property.getTagsList(), gotProperty.getTagsList());
        });

        Assert.assertTrue(this.client.deleteProperty("default", "sw", "ui_template").getDeleted());
        BanyandbProperty.QueryResponse resp = client.query(BanyandbProperty.QueryRequest.newBuilder()
                .addGroups("default")
                .setContainer("sw")
                .addIds("ui_template")
                .build());
        Assert.assertEquals(0, resp.getPropertiesCount());
    }

    @Test
    public void test_PropertyCreateUpdateAndGet() throws BanyanDBException {
        Property property1 = buildProperty("default", "sw", "ui_template").toBuilder().addTags(
            Tag.newBuilder().setKey("name").setValue(
                TagValue.newBuilder().setStr(Str.newBuilder().setValue("hello")))).build();
        Assert.assertTrue(this.client.apply(property1).getCreated());

        Property property2 = buildProperty("default", "sw", "ui_template").toBuilder().addTags(
            Tag.newBuilder().setKey("name").setValue(
                TagValue.newBuilder().setStr(Str.newBuilder().setValue("word")))).build();
        Assert.assertFalse(this.client.apply(property2).getCreated());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            BanyandbProperty.QueryResponse resp = client.query(BanyandbProperty.QueryRequest.newBuilder()
                    .addGroups("default")
                    .setContainer("sw")
                    .addIds("ui_template")
                    .build());
            Assert.assertEquals(1, resp.getPropertiesCount());
            Property gotProperty = resp.getProperties(0);
            Assert.assertNotNull(gotProperty);
            Assert.assertEquals(property2.getTagsList(), gotProperty.getTagsList());
        });
    }

    @Test
    public void test_PropertyList() throws BanyanDBException {
        Property property = buildProperty("default", "sw", "id1").toBuilder().addTags(
            Tag.newBuilder().setKey("name").setValue(
                TagValue.newBuilder().setStr(Str.newBuilder().setValue("bar")))).build();
        Assert.assertTrue(this.client.apply(property).getCreated());
        property = buildProperty("default", "sw", "id2").toBuilder().addTags(
            Tag.newBuilder().setKey("name").setValue(
                TagValue.newBuilder().setStr(Str.newBuilder().setValue("foo")))).build();
        Assert.assertTrue(this.client.apply(property).getCreated());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            BanyandbProperty.QueryResponse resp = client.query(BanyandbProperty.QueryRequest.newBuilder()
                    .addGroups("default")
                    .setContainer("sw")
                    .build());
            Assert.assertEquals(2, resp.getPropertiesCount());
        });
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            BanyandbProperty.QueryResponse resp = client.query(BanyandbProperty.QueryRequest.newBuilder()
                    .addGroups("default")
                    .setContainer("sw")
                    .addIds("id1")
                    .addIds("id2")
                    .build());
            Assert.assertEquals(2, resp.getPropertiesCount());
        });
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            BanyandbProperty.QueryResponse resp = client.query(BanyandbProperty.QueryRequest.newBuilder()
                    .addGroups("default")
                    .setContainer("sw")
                    .addIds("id2")
                    .build());
            Assert.assertEquals(1, resp.getPropertiesCount());
        });
    }

    private BanyandbProperty.Property buildProperty(String group, String name, String id) {
        BanyandbProperty.Property.Builder builder = BanyandbProperty.Property.newBuilder()
                                                                             .setMetadata(
                                                                                 BanyandbProperty.Metadata.newBuilder()
                                                                                                          .setContainer(
                                                                                                              BanyandbCommon.Metadata.newBuilder()
                                                                                                                                     .setGroup(
                                                                                                                                         group)
                                                                                                                                     .setName(
                                                                                                                                         name))
                                                                                                          .setId(id));
        return builder.build();
    }
}
