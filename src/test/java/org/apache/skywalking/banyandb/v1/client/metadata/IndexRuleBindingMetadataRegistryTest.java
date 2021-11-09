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
import org.apache.skywalking.banyandb.database.v1.metadata.BanyandbMetadata;
import org.apache.skywalking.banyandb.database.v1.metadata.IndexRuleBindingRegistryServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.BanyanDBClient;
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
public class IndexRuleBindingMetadataRegistryTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private IndexRuleBindingMetadataRegistry client;

    // play as an in-memory registry
    private Map<String, BanyandbMetadata.IndexRuleBinding> indexRuleBindingRegistry;

    private final IndexRuleBindingRegistryServiceGrpc.IndexRuleBindingRegistryServiceImplBase serviceImpl =
            mock(IndexRuleBindingRegistryServiceGrpc.IndexRuleBindingRegistryServiceImplBase.class, delegatesTo(
                    new IndexRuleBindingRegistryServiceGrpc.IndexRuleBindingRegistryServiceImplBase() {
                        @Override
                        public void create(BanyandbMetadata.IndexRuleBindingRegistryServiceCreateRequest request, StreamObserver<BanyandbMetadata.IndexRuleBindingRegistryServiceCreateResponse> responseObserver) {
                            BanyandbMetadata.IndexRuleBinding s = request.getIndexRuleBinding().toBuilder()
                                    .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            indexRuleBindingRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbMetadata.IndexRuleBindingRegistryServiceCreateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void update(BanyandbMetadata.IndexRuleBindingRegistryServiceUpdateRequest request, StreamObserver<BanyandbMetadata.IndexRuleBindingRegistryServiceUpdateResponse> responseObserver) {
                            BanyandbMetadata.IndexRuleBinding s = request.getIndexRuleBinding().toBuilder()
                                    .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            indexRuleBindingRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbMetadata.IndexRuleBindingRegistryServiceUpdateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void delete(BanyandbMetadata.IndexRuleBindingRegistryServiceDeleteRequest request, StreamObserver<BanyandbMetadata.IndexRuleBindingRegistryServiceDeleteResponse> responseObserver) {
                            BanyandbMetadata.IndexRuleBinding oldIndexRuleBinding = indexRuleBindingRegistry.remove(request.getMetadata().getName());
                            responseObserver.onNext(BanyandbMetadata.IndexRuleBindingRegistryServiceDeleteResponse.newBuilder()
                                    .setDeleted(oldIndexRuleBinding != null)
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void get(BanyandbMetadata.IndexRuleBindingRegistryServiceGetRequest request, StreamObserver<BanyandbMetadata.IndexRuleBindingRegistryServiceGetResponse> responseObserver) {
                            responseObserver.onNext(BanyandbMetadata.IndexRuleBindingRegistryServiceGetResponse.newBuilder()
                                    .setIndexRuleBinding(indexRuleBindingRegistry.get(request.getMetadata().getName()))
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void list(BanyandbMetadata.IndexRuleBindingRegistryServiceListRequest request, StreamObserver<BanyandbMetadata.IndexRuleBindingRegistryServiceListResponse> responseObserver) {
                            responseObserver.onNext(BanyandbMetadata.IndexRuleBindingRegistryServiceListResponse.newBuilder()
                                    .addAllIndexRuleBinding(indexRuleBindingRegistry.values())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    }));

    @Before
    public void setUp() throws IOException {
        indexRuleBindingRegistry = new HashMap<>();

        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        Server server = InProcessServerBuilder
                .forName(serverName).directExecutor().addService(serviceImpl).build();
        grpcCleanup.register(server.start());

        // Create a client channel and register for automatic graceful shutdown.
        ManagedChannel channel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());
        BanyanDBClient client = new BanyanDBClient("127.0.0.1", server.getPort(), "default");

        client.connect(channel);

        this.client = client.indexRuleBindingRegistry();
    }

    @Test
    public void testIndexRuleBindingRegistry_create() {
        IndexRuleBinding indexRuleBinding = new IndexRuleBinding("sw-index-rule-binding", IndexRuleBinding.Subject.referToStream("sw"));
        indexRuleBinding.setBeginAt(ZonedDateTime.now().minusDays(15));
        indexRuleBinding.setExpireAt(ZonedDateTime.now().plusYears(100));
        indexRuleBinding.addRule("trace_id").addRule("duration").addRule("endpoint_id");
        this.client.create(indexRuleBinding);
        Assert.assertEquals(indexRuleBindingRegistry.size(), 1);
    }

    @Test
    public void testIndexRuleBindingRegistry_createAndGet() {
        IndexRuleBinding indexRuleBinding = new IndexRuleBinding("sw-index-rule-binding", IndexRuleBinding.Subject.referToStream("sw"));
        indexRuleBinding.setBeginAt(ZonedDateTime.now().minusDays(15));
        indexRuleBinding.setExpireAt(ZonedDateTime.now().plusYears(100));
        indexRuleBinding.addRule("trace_id").addRule("duration").addRule("endpoint_id");
        this.client.create(indexRuleBinding);
        IndexRuleBinding getIndexRuleBinding = this.client.get("sw-index-rule-binding");
        Assert.assertNotNull(getIndexRuleBinding);
        Assert.assertEquals(indexRuleBinding, getIndexRuleBinding);
        Assert.assertNotNull(getIndexRuleBinding.getUpdatedAt());
    }

    @Test
    public void testIndexRuleBindingRegistry_createAndList() {
        IndexRuleBinding indexRuleBinding = new IndexRuleBinding("sw-index-rule-binding", IndexRuleBinding.Subject.referToStream("sw"));
        indexRuleBinding.setBeginAt(ZonedDateTime.now().minusDays(15));
        indexRuleBinding.setExpireAt(ZonedDateTime.now().plusYears(100));
        indexRuleBinding.addRule("trace_id").addRule("duration").addRule("endpoint_id");
        this.client.create(indexRuleBinding);
        List<IndexRuleBinding> listIndexRuleBinding = this.client.list();
        Assert.assertNotNull(listIndexRuleBinding);
        Assert.assertEquals(1, listIndexRuleBinding.size());
        Assert.assertEquals(listIndexRuleBinding.get(0), indexRuleBinding);
    }

    @Test
    public void testIndexRuleBindingRegistry_createAndDelete() {
        IndexRuleBinding indexRuleBinding = new IndexRuleBinding("sw-index-rule-binding", IndexRuleBinding.Subject.referToStream("sw"));
        indexRuleBinding.setBeginAt(ZonedDateTime.now().minusDays(15));
        indexRuleBinding.setExpireAt(ZonedDateTime.now().plusYears(100));
        indexRuleBinding.addRule("trace_id").addRule("duration").addRule("endpoint_id");
        this.client.create(indexRuleBinding);
        boolean deleted = this.client.delete("sw-index-rule-binding");
        Assert.assertTrue(deleted);
        Assert.assertEquals(0, indexRuleBindingRegistry.size());
    }
}
