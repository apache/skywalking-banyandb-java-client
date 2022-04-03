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

package org.apache.skywalking.banyandb.v1.client.grpc.channel;

import com.google.common.collect.ImmutableList;
import io.grpc.CallOptions;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.database.v1.IndexRuleRegistryServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBApiException;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleMetadataRegistry;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.powermock.api.mockito.PowerMockito.mock;

public class ChannelManagerTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Test
    public void testAuthority() throws IOException {
        final ManagedChannel ch = Mockito.mock(ManagedChannel.class);

        Mockito.when(ch.authority()).thenReturn("myAuth");

        ChannelManager manager =
                ChannelManager.create(
                        ChannelManagerSettings.newBuilder()
                                .setRefreshInterval(30)
                                .setForceReconnectionThreshold(10).build(),
                        new FakeChannelFactory(ch));
        Assert.assertEquals("myAuth", manager.authority());
    }

    @Test
    public void channelRefreshShouldSwapChannel() throws IOException {
        ManagedChannel underlyingChannel1 = Mockito.mock(ManagedChannel.class);
        ManagedChannel underlyingChannel2 = Mockito.mock(ManagedChannel.class);

        // mock executor service to capture the runnable scheduled, so we can invoke it when we want to
        ScheduledExecutorService scheduledExecutorService =
                Mockito.mock(ScheduledExecutorService.class);

        Mockito.doReturn(null)
                .when(scheduledExecutorService)
                .schedule(
                        Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS));

        ChannelManager manager =
                new ChannelManager(
                        ChannelManagerSettings.newBuilder()
                                .setRefreshInterval(30)
                                .setForceReconnectionThreshold(1).build(),
                        new FakeChannelFactory(ImmutableList.of(underlyingChannel1, underlyingChannel2)),
                        scheduledExecutorService);
        Mockito.reset(underlyingChannel1);

        manager.newCall(FakeMethodDescriptor.<String, Integer>create(), CallOptions.DEFAULT);

        Mockito.verify(underlyingChannel1, Mockito.only())
                .newCall(Mockito.<MethodDescriptor<String, Integer>>any(), Mockito.any(CallOptions.class));

        // set status to needReconnect=true
        manager.entryRef.get().needReconnect = true;
        // and return false for connection status
        Mockito.doReturn(ConnectivityState.TRANSIENT_FAILURE)
                .when(underlyingChannel1)
                .getState(Mockito.anyBoolean());

        // swap channel
        manager.refresh();

        manager.newCall(FakeMethodDescriptor.<String, Integer>create(), CallOptions.DEFAULT);

        Mockito.verify(underlyingChannel2, Mockito.only())
                .newCall(Mockito.<MethodDescriptor<String, Integer>>any(), Mockito.any(CallOptions.class));
    }

    @Test
    public void networkErrorStatusShouldTriggerReconnect() throws IOException {
        final IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceImplBase indexRuleServiceImpl =
                mock(IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceImplBase.class, delegatesTo(
                        new IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceImplBase() {
                            @Override
                            public void get(BanyandbDatabase.IndexRuleRegistryServiceGetRequest request, StreamObserver<BanyandbDatabase.IndexRuleRegistryServiceGetResponse> responseObserver) {
                                responseObserver.onError(Status.UNAVAILABLE.asRuntimeException());
                            }
                        }));
        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        InProcessServerBuilder serverBuilder = InProcessServerBuilder
                .forName(serverName).directExecutor()
                .addService(indexRuleServiceImpl);
        final Server s = serverBuilder.build();
        grpcCleanup.register(s.start());

        // Create a client channel and register for automatic graceful shutdown.
        ManagedChannel ch = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());

        ChannelManager manager =
                ChannelManager.create(
                        ChannelManagerSettings.newBuilder()
                                .setRefreshInterval(30)
                                .setForceReconnectionThreshold(10).build(),
                        new FakeChannelFactory(ch));

        try {
            new IndexRuleMetadataRegistry(manager).get("default", "sw");
            Assert.fail();
        } catch (BanyanDBApiException ex) {
            Assert.assertEquals(ex.getStatus(), Status.Code.UNAVAILABLE);
        }

        Assert.assertTrue(manager.entryRef.get().needReconnect);
    }
}
