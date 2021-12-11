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

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.database.v1.MeasureRegistryServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class MeasureMetadataRegistryTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private MeasureMetadataRegistry client;

    // play as an in-memory registry
    private Map<String, BanyandbDatabase.Measure> measureRegistry;

    private final MeasureRegistryServiceGrpc.MeasureRegistryServiceImplBase serviceImpl =
            mock(MeasureRegistryServiceGrpc.MeasureRegistryServiceImplBase.class, delegatesTo(
                    new MeasureRegistryServiceGrpc.MeasureRegistryServiceImplBase() {
                        @Override
                        public void create(BanyandbDatabase.MeasureRegistryServiceCreateRequest request, StreamObserver<BanyandbDatabase.MeasureRegistryServiceCreateResponse> responseObserver) {
                            BanyandbDatabase.Measure s = request.getMeasure().toBuilder()
                                    .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            measureRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbDatabase.MeasureRegistryServiceCreateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void update(BanyandbDatabase.MeasureRegistryServiceUpdateRequest request, StreamObserver<BanyandbDatabase.MeasureRegistryServiceUpdateResponse> responseObserver) {
                            BanyandbDatabase.Measure s = request.getMeasure().toBuilder()
                                    .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            measureRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbDatabase.MeasureRegistryServiceUpdateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void delete(BanyandbDatabase.MeasureRegistryServiceDeleteRequest request, StreamObserver<BanyandbDatabase.MeasureRegistryServiceDeleteResponse> responseObserver) {
                            BanyandbDatabase.Measure oldMeasure = measureRegistry.remove(request.getMetadata().getName());
                            responseObserver.onNext(BanyandbDatabase.MeasureRegistryServiceDeleteResponse.newBuilder()
                                    .setDeleted(oldMeasure != null)
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void get(BanyandbDatabase.MeasureRegistryServiceGetRequest request, StreamObserver<BanyandbDatabase.MeasureRegistryServiceGetResponse> responseObserver) {
                            responseObserver.onNext(BanyandbDatabase.MeasureRegistryServiceGetResponse.newBuilder()
                                    .setMeasure(measureRegistry.get(request.getMetadata().getName()))
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void list(BanyandbDatabase.MeasureRegistryServiceListRequest request, StreamObserver<BanyandbDatabase.MeasureRegistryServiceListResponse> responseObserver) {
                            responseObserver.onNext(BanyandbDatabase.MeasureRegistryServiceListResponse.newBuilder()
                                    .addAllMeasure(measureRegistry.values())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    }));

    @Before
    public void setUp() throws IOException {
        measureRegistry = new HashMap<>();

        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        Server server = InProcessServerBuilder
                .forName(serverName).directExecutor().addService(serviceImpl).build();
        grpcCleanup.register(server.start());

        // Create a client channel and register for automatic graceful shutdown.
        ManagedChannel channel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());

        this.client = new MeasureMetadataRegistry(channel);
    }

    @Test
    public void testMeasureRegistry_create() {
        Measure s = new Measure("default", "sw", Duration.ofHours(1));
        this.client.create(s);
        Assert.assertEquals(measureRegistry.size(), 1);
    }

    @Test
    public void testMeasureRegistry_createAndGet() {
        Measure m = new Measure("sw_metric", "service_cpm_minute", Duration.ofHours(1));
        m.addTagNameAsEntity("entity_id");
        // tags
        TagFamilySpec defaultFamily = new TagFamilySpec("default");
        defaultFamily.addTagSpec(TagFamilySpec.TagSpec.newIDTag("id"));
        defaultFamily.addTagSpec(TagFamilySpec.TagSpec.newStringTag("entity_id"));
        m.addTagFamilySpec(defaultFamily);
        // field spec
        m.addFieldSpec(Measure.FieldSpec.newIntField("total").compressWithZSTD().encodeWithGorilla().build())
                .addFieldSpec(Measure.FieldSpec.newIntField("value").compressWithZSTD().encodeWithGorilla().build());
        this.client.create(m);
        Measure getMeasure = this.client.get("sw_metric", "service_cpm_minute");
        Assert.assertNotNull(getMeasure);
        Assert.assertEquals(m, getMeasure);
        Assert.assertNotNull(getMeasure.getUpdatedAt());
    }

    @Test
    public void testMeasureRegistry_createAndList() {
        Measure m = new Measure("sw_metric", "service_cpm_minute", Duration.ofHours(1));
        m.addTagNameAsEntity("entity_id");
        // tags
        TagFamilySpec defaultFamily = new TagFamilySpec("default");
        defaultFamily.addTagSpec(TagFamilySpec.TagSpec.newIDTag("id"));
        defaultFamily.addTagSpec(TagFamilySpec.TagSpec.newStringTag("entity_id"));
        m.addTagFamilySpec(defaultFamily);
        // field spec
        m.addFieldSpec(Measure.FieldSpec.newIntField("total").compressWithZSTD().encodeWithGorilla().build())
                .addFieldSpec(Measure.FieldSpec.newIntField("value").compressWithZSTD().encodeWithGorilla().build());
        this.client.create(m);
        List<Measure> listMeasure = this.client.list("sw_metric");
        Assert.assertNotNull(listMeasure);
        Assert.assertEquals(1, listMeasure.size());
        Assert.assertEquals(listMeasure.get(0), m);
    }

    @Test
    public void testMeasureRegistry_createAndDelete() {
        Measure m = new Measure("sw_metric", "service_cpm_minute", Duration.ofHours(1));
        m.addTagNameAsEntity("entity_id");
        // tags
        TagFamilySpec defaultFamily = new TagFamilySpec("default");
        defaultFamily.addTagSpec(TagFamilySpec.TagSpec.newIDTag("id"));
        defaultFamily.addTagSpec(TagFamilySpec.TagSpec.newStringTag("entity_id"));
        m.addTagFamilySpec(defaultFamily);
        // field spec
        m.addFieldSpec(Measure.FieldSpec.newIntField("total").compressWithZSTD().encodeWithGorilla().build())
                .addFieldSpec(Measure.FieldSpec.newIntField("value").compressWithZSTD().encodeWithGorilla().build());
        this.client.create(m);
        boolean deleted = this.client.delete("sw_metric", "service_cpm_minute");
        Assert.assertTrue(deleted);
        Assert.assertEquals(0, measureRegistry.size());
    }
}
