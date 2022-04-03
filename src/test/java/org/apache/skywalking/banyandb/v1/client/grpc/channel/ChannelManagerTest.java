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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ChannelManagerTest {
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
}
