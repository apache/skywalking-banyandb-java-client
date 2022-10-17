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

import io.grpc.stub.StreamObserver;
import org.apache.skywalking.banyandb.property.v1.BanyandbProperty;
import org.apache.skywalking.banyandb.property.v1.PropertyServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.AbstractBanyanDBClientTest;
import org.apache.skywalking.banyandb.v1.client.TagAndValue;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

public class PropertyStoreTest extends AbstractBanyanDBClientTest {
    private PropertyStore store;

    private Map<String, BanyandbProperty.Property> memory;

    private final PropertyServiceGrpc.PropertyServiceImplBase propertyServiceImpl = mock(PropertyServiceGrpc.PropertyServiceImplBase.class, delegatesTo(
            new PropertyServiceGrpc.PropertyServiceImplBase() {
                @Override
                public void create(BanyandbProperty.CreateRequest request, StreamObserver<BanyandbProperty.CreateResponse> responseObserver) {
                    BanyandbProperty.Property p = request.getProperty().toBuilder()
                            .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                            .build();
                    memory.put(format(p.getMetadata()), p);
                    responseObserver.onNext(BanyandbProperty.CreateResponse.newBuilder().build());
                    responseObserver.onCompleted();
                }

                @Override
                public void update(BanyandbProperty.UpdateRequest request, StreamObserver<BanyandbProperty.UpdateResponse> responseObserver) {
                    BanyandbProperty.Property p = request.getProperty().toBuilder()
                            .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                            .build();
                    memory.put(format(p.getMetadata()), p);
                    responseObserver.onNext(BanyandbProperty.UpdateResponse.newBuilder().build());
                    responseObserver.onCompleted();
                }

                @Override
                public void delete(BanyandbProperty.DeleteRequest request, StreamObserver<BanyandbProperty.DeleteResponse> responseObserver) {
                    final BanyandbProperty.Property p = memory.remove(format(request.getMetadata()));
                    responseObserver.onNext(BanyandbProperty.DeleteResponse.newBuilder().setDeleted(p != null).build());
                    responseObserver.onCompleted();
                }

                @Override
                public void get(BanyandbProperty.GetRequest request, StreamObserver<BanyandbProperty.GetResponse> responseObserver) {
                    final BanyandbProperty.Property p = memory.get(format(request.getMetadata()));
                    responseObserver.onNext(BanyandbProperty.GetResponse.newBuilder().setProperty(p).build());
                    responseObserver.onCompleted();
                }

                @Override
                public void list(BanyandbProperty.ListRequest request, StreamObserver<BanyandbProperty.ListResponse> responseObserver) {
                    responseObserver.onNext(BanyandbProperty.ListResponse.newBuilder().addAllProperty(memory.values()).build());
                    responseObserver.onCompleted();
                }
            }));

    @Before
    public void setUp() throws IOException {
        super.setUp(bindService(propertyServiceImpl));
        this.memory = new HashMap<>();
        this.store = new PropertyStore(this.channel);
    }

    @Test
    public void testPropertyStore_create() throws BanyanDBException {
        Property property = Property.create("default", "sw", "ui_template")
                .addTag(TagAndValue.newStringTag("name", "hello"))
                .build();
        this.store.create(property);
        Assert.assertEquals(memory.size(), 1);
    }

    @Test
    public void testPropertyStore_createAndGet() throws BanyanDBException {
        Property property = Property.create("default", "sw", "ui_template")
                .addTag(TagAndValue.newStringTag("name", "hello"))
                .build();
        this.store.create(property);
        Property gotProperty = this.store.get("default", "sw", "ui_template");
        Assert.assertNotNull(gotProperty);
        Assert.assertEquals(property, gotProperty);
        Assert.assertNotNull(gotProperty.updatedAt());
    }

    @Test
    public void testPropertyStore_createAndList() throws BanyanDBException {
        Property property = Property.create("default", "sw", "ui_template")
                .addTag(TagAndValue.newStringTag("name", "hello"))
                .build();
        this.store.create(property);
        List<Property> listProperties = this.store.list("default", "sw");
        Assert.assertNotNull(listProperties);
        Assert.assertEquals(1, listProperties.size());
        Assert.assertEquals(listProperties.get(0), property);
    }

    @Test
    public void testPropertyStore_createAndDelete() throws BanyanDBException {
        Property property = Property.create("default", "sw", "ui_template")
                .addTag(TagAndValue.newStringTag("name", "hello"))
                .build();
        this.store.create(property);
        boolean deleted = this.store.delete("default", "sw", "ui_template");
        Assert.assertTrue(deleted);
        Assert.assertEquals(0, memory.size());
    }

    static String format(BanyandbProperty.Metadata metadata) {
        return metadata.getContainer().getGroup() + ":" + metadata.getContainer().getName() + "/" + metadata.getId();
    }
}
