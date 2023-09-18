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

import io.grpc.Status;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.metadata.Group;
import org.apache.skywalking.banyandb.v1.client.metadata.Property;
import org.apache.skywalking.banyandb.v1.client.metadata.PropertyStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class ITBanyanDBPropertyTests extends BanyanDBClientTestCI {
    @Before
    public void setUp() throws IOException, BanyanDBException, InterruptedException {
        super.setUpConnection();
        Group expectedGroup = this.client.define(Group.create("default"));
        Assert.assertNotNull(expectedGroup);
    }

    @After
    public void tearDown() throws IOException {
        this.closeClient();
    }

    @Test
    public void test_PropertyCreateAndGet() throws BanyanDBException {
        Property property = Property.create("default", "sw", "ui_template")
                .addTag(TagAndValue.newStringTag("name", "hello"))
                .build();
        Assert.assertTrue(this.client.apply(property).created());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Property gotProperty = client.findProperty("default", "sw", "ui_template");
            Assert.assertNotNull(gotProperty);
            Assert.assertEquals(property, gotProperty);
        });
    }

    @Test
    public void test_PropertyCreateDeleteAndGet() throws BanyanDBException {
        Property property = Property.create("default", "sw", "ui_template")
                .addTag(TagAndValue.newStringTag("name", "hello"))
                .build();
        Assert.assertTrue(this.client.apply(property).created());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Property gotProperty = client.findProperty("default", "sw", "ui_template");
            Assert.assertNotNull(gotProperty);
            Assert.assertEquals(property, gotProperty);
        });

        Assert.assertTrue(this.client.deleteProperty("default", "sw", "ui_template").deleted());

        try {
            client.findProperty("default", "sw", "ui_template");
            Assert.fail();
        } catch (BanyanDBException ex) {
            Assert.assertEquals(Status.Code.NOT_FOUND, ex.getStatus());
        }
    }

    @Test
    public void test_PropertyCreateUpdateAndGet() throws BanyanDBException {
        Property property1 = Property.create("default", "sw", "ui_template")
                .addTag(TagAndValue.newStringTag("name", "hello"))
                .build();
        Assert.assertTrue(this.client.apply(property1).created());

        Property property2 = Property.create("default", "sw", "ui_template")
                .addTag(TagAndValue.newStringTag("name", "world"))
                .build();
        Assert.assertFalse(this.client.apply(property2).created());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Property gotProperty = client.findProperty("default", "sw", "ui_template");
            Assert.assertNotNull(gotProperty);
            Assert.assertEquals(property2, gotProperty);
        });
    }

    @Test
    public void test_PropertyList() throws BanyanDBException {
        Property property = Property.create("default", "sw", "id1")
                .addTag(TagAndValue.newStringTag("name", "bar"))
                .build();
        Assert.assertTrue(this.client.apply(property).created());
        property = Property.create("default", "sw", "id2")
                .addTag(TagAndValue.newStringTag("name", "foo"))
                .build();
        Assert.assertTrue(this.client.apply(property).created());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Property> gotProperties = client.findProperties("default", "sw");
            Assert.assertEquals(2, gotProperties.size());
        });
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Property> gotProperties = client.findProperties("default", "sw", Arrays.asList("id1", "id2"), null);
            Assert.assertEquals(2, gotProperties.size());
        });
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Property> gotProperties = client.findProperties("default", "sw", Arrays.asList("id2"), null);
            Assert.assertEquals(1, gotProperties.size());
        });
    }

    @Test
    public void test_PropertyKeepAlive() throws BanyanDBException {
        Property property = Property.create("default", "sw", "id1")
                .setTtl("30m")
                .addTag(TagAndValue.newStringTag("name", "bar"))
                .build();
        PropertyStore.ApplyResult resp = this.client.apply(property);
        Assert.assertTrue(resp.created());
        Assert.assertTrue(resp.leaseId() > 0);
        this.client.keepAliveProperty(resp.leaseId());
    }

}
