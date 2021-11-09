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
import org.apache.skywalking.banyandb.database.v1.metadata.IndexRuleRegistryServiceGrpc;
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
public class IndexRuleMetadataRegistryTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private IndexRuleMetadataRegistry client;

    // play as an in-memory registry
    private Map<String, BanyandbMetadata.IndexRule> indexRuleRegistry;

    private final IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceImplBase serviceImpl =
            mock(IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceImplBase.class, delegatesTo(
                    new IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceImplBase() {
                        @Override
                        public void create(BanyandbMetadata.IndexRuleRegistryServiceCreateRequest request, StreamObserver<BanyandbMetadata.IndexRuleRegistryServiceCreateResponse> responseObserver) {
                            BanyandbMetadata.IndexRule s = request.getIndexRule().toBuilder().setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            indexRuleRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbMetadata.IndexRuleRegistryServiceCreateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void update(BanyandbMetadata.IndexRuleRegistryServiceUpdateRequest request, StreamObserver<BanyandbMetadata.IndexRuleRegistryServiceUpdateResponse> responseObserver) {
                            BanyandbMetadata.IndexRule s = request.getIndexRule().toBuilder().setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            indexRuleRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbMetadata.IndexRuleRegistryServiceUpdateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void delete(BanyandbMetadata.IndexRuleRegistryServiceDeleteRequest request, StreamObserver<BanyandbMetadata.IndexRuleRegistryServiceDeleteResponse> responseObserver) {
                            BanyandbMetadata.IndexRule oldIndexRule = indexRuleRegistry.remove(request.getMetadata().getName());
                            responseObserver.onNext(BanyandbMetadata.IndexRuleRegistryServiceDeleteResponse.newBuilder()
                                    .setDeleted(oldIndexRule != null)
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void get(BanyandbMetadata.IndexRuleRegistryServiceGetRequest request, StreamObserver<BanyandbMetadata.IndexRuleRegistryServiceGetResponse> responseObserver) {
                            responseObserver.onNext(BanyandbMetadata.IndexRuleRegistryServiceGetResponse.newBuilder()
                                    .setIndexRule(indexRuleRegistry.get(request.getMetadata().getName()))
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void list(BanyandbMetadata.IndexRuleRegistryServiceListRequest request, StreamObserver<BanyandbMetadata.IndexRuleRegistryServiceListResponse> responseObserver) {
                            responseObserver.onNext(BanyandbMetadata.IndexRuleRegistryServiceListResponse.newBuilder()
                                    .addAllIndexRule(indexRuleRegistry.values())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    }));

    @Before
    public void setUp() throws IOException {
        indexRuleRegistry = new HashMap<>();

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

        this.client = client.indexRuleRegistry();
    }

    @Test
    public void testIndexRuleRegistry_create() {
        IndexRule indexRule = new IndexRule("db.instance", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES);
        indexRule.addTag("db.instance");
        this.client.create(indexRule);
        Assert.assertEquals(indexRuleRegistry.size(), 1);
    }

    @Test
    public void testIndexRuleRegistry_createAndGet() {
        IndexRule indexRule = new IndexRule("db.instance", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES);
        indexRule.addTag("db.instance");
        this.client.create(indexRule);
        IndexRule getIndexRule = this.client.get("db.instance");
        Assert.assertNotNull(getIndexRule);
        Assert.assertEquals(indexRule, getIndexRule);
        Assert.assertNotNull(getIndexRule.getUpdatedAt());
    }

    @Test
    public void testIndexRuleRegistry_createAndList() {
        IndexRule indexRule = new IndexRule("db.instance", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES);
        indexRule.addTag("db.instance");
        this.client.create(indexRule);
        List<IndexRule> listIndexRule = this.client.list();
        Assert.assertNotNull(listIndexRule);
        Assert.assertEquals(1, listIndexRule.size());
        Assert.assertEquals(listIndexRule.get(0), indexRule);
    }

    @Test
    public void testIndexRuleRegistry_createAndDelete() {
        IndexRule indexRule = new IndexRule("db.instance", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES);
        indexRule.addTag("db.instance");
        this.client.create(indexRule);
        boolean deleted = this.client.delete("db.instance");
        Assert.assertTrue(deleted);
        Assert.assertEquals(0, indexRuleRegistry.size());
    }
}