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
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.Property;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TagSpec;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TagType;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ITPropertyMetadataRegistryTest extends BanyanDBClientTestCI {
    @Before
    public void setUp() throws IOException, BanyanDBException, InterruptedException {
        super.setUpConnection();
        BanyandbCommon.Group expectedGroup = buildPropertyGroup();
        client.define(expectedGroup);
        Assert.assertNotNull(expectedGroup);
    }

    @After
    public void tearDown() throws IOException {
        this.closeClient();
    }

    @Test
    public void testPropertyRegistry_createAndGet() throws BanyanDBException {
        Property expectedProperty = buildProperty();
        this.client.define(expectedProperty);
        Property actualProperty = client.findPropertyDefinition("sw_config", "ui_template");
        Assert.assertNotNull(actualProperty);
        Assert.assertNotNull(actualProperty.getUpdatedAt());
        actualProperty = actualProperty.toBuilder().clearUpdatedAt().setMetadata(actualProperty.getMetadata().toBuilder().clearModRevision().clearCreateRevision()).build();
        Assert.assertEquals(expectedProperty, actualProperty);
    }

    @Test
    public void testPropertyRegistry_createAndList() throws BanyanDBException {
        Property expectedProperty = buildProperty();
        this.client.define(expectedProperty);
        List<Property> actualProperties = client.findPropertiesDefinition("sw_config");
        Assert.assertNotNull(actualProperties);
        Assert.assertEquals(1, actualProperties.size());
        Property actualProperty = actualProperties.get(0);
        actualProperty = actualProperty.toBuilder().clearUpdatedAt().setMetadata(actualProperty.getMetadata().toBuilder().clearModRevision().clearCreateRevision()).build();
        Assert.assertEquals(expectedProperty, actualProperty);
    }

    @Test
    public void testPropertyRegistry_createAndDelete() throws BanyanDBException {
        Property expectedProperty = buildProperty();
        this.client.define(expectedProperty);
        boolean deleted = this.client.deletePropertyDefinition(
            expectedProperty.getMetadata().getGroup(), expectedProperty.getMetadata().getName());
        Assert.assertTrue(deleted);
        Assert.assertThrows(
                org.apache.skywalking.banyandb.v1.client.grpc.exception.NotFoundException.class,
                () -> client.findPropertyDefinition(
                    expectedProperty.getMetadata().getGroup(), expectedProperty.getMetadata().getName())
            );
    }

    private Property buildProperty() {
        Property.Builder builder = Property.newBuilder()
                                         .setMetadata(Metadata.newBuilder()
                                                              .setGroup("sw_config")
                                                              .setName("ui_template"))
                                         .addTags(
                                                TagSpec.newBuilder()
                                                        .setName("type")
                                                        .setType(
                                                                TagType.TAG_TYPE_STRING))
                                                        .addTags(
                                                        TagSpec.newBuilder()
                                                                .setName("service")
                                                                .setType(
                                                                        TagType.TAG_TYPE_STRING));
        return builder.build();
    }

    private BanyandbCommon.Group buildPropertyGroup() {
        return BanyandbCommon.Group.newBuilder()
                .setMetadata(BanyandbCommon.Metadata.newBuilder()
                        .setName("sw_config")
                        .build())
                .setCatalog(BanyandbCommon.Catalog.CATALOG_PROPERTY)
                .setResourceOpts(BanyandbCommon.ResourceOpts.newBuilder()
                        .setShardNum(2))
                .build();
    }
}
