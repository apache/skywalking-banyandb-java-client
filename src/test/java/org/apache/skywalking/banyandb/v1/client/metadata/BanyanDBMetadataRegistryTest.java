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

import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.util.MutableHandlerRegistry;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.database.v1.IndexRuleBindingRegistryServiceGrpc;
import org.apache.skywalking.banyandb.database.v1.IndexRuleRegistryServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.BanyanDBClient;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;
import org.junit.Rule;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.powermock.api.mockito.PowerMockito.mock;

public class BanyanDBMetadataRegistryTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    // play as an in-memory registry
    protected Map<String, BanyandbDatabase.IndexRuleBinding> indexRuleBindingRegistry;

    private final IndexRuleBindingRegistryServiceGrpc.IndexRuleBindingRegistryServiceImplBase indexRuleBindingServiceImpl =
            mock(IndexRuleBindingRegistryServiceGrpc.IndexRuleBindingRegistryServiceImplBase.class, delegatesTo(
                    new IndexRuleBindingRegistryServiceGrpc.IndexRuleBindingRegistryServiceImplBase() {
                        @Override
                        public void create(BanyandbDatabase.IndexRuleBindingRegistryServiceCreateRequest request, StreamObserver<BanyandbDatabase.IndexRuleBindingRegistryServiceCreateResponse> responseObserver) {
                            BanyandbDatabase.IndexRuleBinding s = request.getIndexRuleBinding().toBuilder()
                                    .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            indexRuleBindingRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbDatabase.IndexRuleBindingRegistryServiceCreateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void update(BanyandbDatabase.IndexRuleBindingRegistryServiceUpdateRequest request, StreamObserver<BanyandbDatabase.IndexRuleBindingRegistryServiceUpdateResponse> responseObserver) {
                            BanyandbDatabase.IndexRuleBinding s = request.getIndexRuleBinding().toBuilder()
                                    .setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            indexRuleBindingRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbDatabase.IndexRuleBindingRegistryServiceUpdateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void delete(BanyandbDatabase.IndexRuleBindingRegistryServiceDeleteRequest request, StreamObserver<BanyandbDatabase.IndexRuleBindingRegistryServiceDeleteResponse> responseObserver) {
                            BanyandbDatabase.IndexRuleBinding oldIndexRuleBinding = indexRuleBindingRegistry.remove(request.getMetadata().getName());
                            responseObserver.onNext(BanyandbDatabase.IndexRuleBindingRegistryServiceDeleteResponse.newBuilder()
                                    .setDeleted(oldIndexRuleBinding != null)
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void get(BanyandbDatabase.IndexRuleBindingRegistryServiceGetRequest request, StreamObserver<BanyandbDatabase.IndexRuleBindingRegistryServiceGetResponse> responseObserver) {
                            responseObserver.onNext(BanyandbDatabase.IndexRuleBindingRegistryServiceGetResponse.newBuilder()
                                    .setIndexRuleBinding(indexRuleBindingRegistry.get(request.getMetadata().getName()))
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void list(BanyandbDatabase.IndexRuleBindingRegistryServiceListRequest request, StreamObserver<BanyandbDatabase.IndexRuleBindingRegistryServiceListResponse> responseObserver) {
                            responseObserver.onNext(BanyandbDatabase.IndexRuleBindingRegistryServiceListResponse.newBuilder()
                                    .addAllIndexRuleBinding(indexRuleBindingRegistry.values())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    }));

    // play as an in-memory registry
    protected Map<String, BanyandbDatabase.IndexRule> indexRuleRegistry;

    private final IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceImplBase indexRuleServiceImpl =
            mock(IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceImplBase.class, delegatesTo(
                    new IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceImplBase() {
                        @Override
                        public void create(BanyandbDatabase.IndexRuleRegistryServiceCreateRequest request, StreamObserver<BanyandbDatabase.IndexRuleRegistryServiceCreateResponse> responseObserver) {
                            BanyandbDatabase.IndexRule s = request.getIndexRule().toBuilder().setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            indexRuleRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbDatabase.IndexRuleRegistryServiceCreateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void update(BanyandbDatabase.IndexRuleRegistryServiceUpdateRequest request, StreamObserver<BanyandbDatabase.IndexRuleRegistryServiceUpdateResponse> responseObserver) {
                            BanyandbDatabase.IndexRule s = request.getIndexRule().toBuilder().setUpdatedAt(TimeUtils.buildTimestamp(ZonedDateTime.now()))
                                    .build();
                            indexRuleRegistry.put(s.getMetadata().getName(), s);
                            responseObserver.onNext(BanyandbDatabase.IndexRuleRegistryServiceUpdateResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void delete(BanyandbDatabase.IndexRuleRegistryServiceDeleteRequest request, StreamObserver<BanyandbDatabase.IndexRuleRegistryServiceDeleteResponse> responseObserver) {
                            BanyandbDatabase.IndexRule oldIndexRule = indexRuleRegistry.remove(request.getMetadata().getName());
                            responseObserver.onNext(BanyandbDatabase.IndexRuleRegistryServiceDeleteResponse.newBuilder()
                                    .setDeleted(oldIndexRule != null)
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void get(BanyandbDatabase.IndexRuleRegistryServiceGetRequest request, StreamObserver<BanyandbDatabase.IndexRuleRegistryServiceGetResponse> responseObserver) {
                            responseObserver.onNext(BanyandbDatabase.IndexRuleRegistryServiceGetResponse.newBuilder()
                                    .setIndexRule(indexRuleRegistry.get(request.getMetadata().getName()))
                                    .build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void list(BanyandbDatabase.IndexRuleRegistryServiceListRequest request, StreamObserver<BanyandbDatabase.IndexRuleRegistryServiceListResponse> responseObserver) {
                            responseObserver.onNext(BanyandbDatabase.IndexRuleRegistryServiceListResponse.newBuilder()
                                    .addAllIndexRule(indexRuleRegistry.values())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    }));

    protected final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();

    protected BanyanDBClient client;

    protected ManagedChannel channel;

    protected void setUp(BindableService... services) throws IOException {
        indexRuleRegistry = new HashMap<>();
        indexRuleBindingRegistry = new HashMap<>();

        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        InProcessServerBuilder serverBuilder = InProcessServerBuilder
                .forName(serverName).directExecutor()
                .fallbackHandlerRegistry(serviceRegistry)
                .addService(indexRuleBindingServiceImpl)
                .addService(indexRuleServiceImpl);
        for (final BindableService svc : services) {
            serverBuilder.addService(svc);
        }
        final Server s = serverBuilder.build();
        grpcCleanup.register(s.start());

        // Create a client channel and register for automatic graceful shutdown.
        this.channel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());

        client = new BanyanDBClient("127.0.0.1", s.getPort());
        client.connect(channel);
    }
}
