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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolverRegistry;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRule;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleBinding;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleBindingMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.Measure;
import org.apache.skywalking.banyandb.v1.client.metadata.MeasureMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.Stream;
import org.apache.skywalking.banyandb.v1.client.metadata.StreamMetadataRegistry;
import org.apache.skywalking.banyandb.v1.stream.BanyandbStream;
import org.apache.skywalking.banyandb.v1.stream.StreamServiceGrpc;

/**
 * BanyanDBClient represents a client instance interacting with BanyanDB server. This is built on the top of BanyanDB v1
 * gRPC APIs.
 */
@Slf4j
public class BanyanDBClient implements Closeable {
    /**
     * The hostname of BanyanDB server.
     */
    private final String host;
    /**
     * The port of BanyanDB server.
     */
    private final int port;
    /**
     * The instance name.
     */
    private final String group;
    /**
     * Options for server connection.
     */
    private Options options;
    /**
     * gRPC connection.
     */
    private volatile Channel channel;
    /**
     * gRPC client stub
     */
    private volatile StreamServiceGrpc.StreamServiceStub streamServiceStub;
    /**
     * gRPC blocking stub.
     */
    private volatile StreamServiceGrpc.StreamServiceBlockingStub streamServiceBlockingStub;
    /**
     * The connection status.
     */
    private volatile boolean isConnected = false;
    /**
     * A lock to control the race condition in establishing and disconnecting network connection.
     */
    private volatile ReentrantLock connectionEstablishLock;

    /**
     * Create a BanyanDB client instance
     *
     * @param host  IP or domain name
     * @param port  Server port
     * @param group Database instance name
     */
    public BanyanDBClient(final String host, final int port, final String group) {
        this(host, port, group, new Options());
    }

    /**
     * Create a BanyanDB client instance with custom options
     *
     * @param host    IP or domain name
     * @param port    Server port
     * @param group   Database instance name
     * @param options for database connection
     */
    public BanyanDBClient(final String host,
                          final int port,
                          final String group,
                          final Options options) {
        this.host = host;
        this.port = port;
        this.group = group;
        this.options = options;
        this.connectionEstablishLock = new ReentrantLock();

        NameResolverRegistry.getDefaultRegistry().register(new DnsNameResolverProvider());
    }

    /**
     * Connect to the server.
     *
     * @throws RuntimeException if server is not reachable.
     */
    public void connect() {
        connectionEstablishLock.lock();
        try {
            if (!isConnected) {
                final ManagedChannelBuilder<?> nettyChannelBuilder = NettyChannelBuilder.forAddress(host, port).usePlaintext();
                nettyChannelBuilder.maxInboundMessageSize(options.getMaxInboundMessageSize());

                channel = nettyChannelBuilder.build();
                streamServiceStub = StreamServiceGrpc.newStub(channel);
                streamServiceBlockingStub = StreamServiceGrpc.newBlockingStub(channel);
                isConnected = true;
            }
        } finally {
            connectionEstablishLock.unlock();
        }
    }

    /**
     * Connect to the mock server.
     * Created for testing purpose.
     *
     * @param channel the channel used for communication.
     *                For tests, it is normally an in-process channel.
     */
    @VisibleForTesting
    public void connect(Channel channel) {
        connectionEstablishLock.lock();
        try {
            if (!isConnected) {
                this.channel = channel;
                streamServiceStub = StreamServiceGrpc.newStub(channel);
                streamServiceBlockingStub = StreamServiceGrpc.newBlockingStub(channel);
                isConnected = true;
            }
        } finally {
            connectionEstablishLock.unlock();
        }
    }

    /**
     * Create a build process for stream write.
     *
     * @param maxBulkSize   the max bulk size for the flush operation
     * @param flushInterval if given maxBulkSize is not reached in this period, the flush would be trigger
     *                      automatically. Unit is second
     * @param concurrency   the number of concurrency would run for the flush max
     * @return stream bulk write processor
     */
    public StreamBulkWriteProcessor buildStreamWriteProcessor(int maxBulkSize, int flushInterval, int concurrency) {
        return new StreamBulkWriteProcessor(group, streamServiceStub, maxBulkSize, flushInterval, concurrency);
    }

    /**
     * Query streams according to given conditions
     *
     * @param streamQuery condition for query
     * @return hint streams.
     */
    public StreamQueryResponse queryStreams(StreamQuery streamQuery) {
        final BanyandbStream.QueryResponse response = streamServiceBlockingStub
                .withDeadlineAfter(options.getDeadline(), TimeUnit.SECONDS)
                .query(streamQuery.build(group));
        return new StreamQueryResponse(response);
    }

    /**
     * Define a new stream
     *
     * @param stream the stream to be created
     * @return a created stream in the BanyanDB
     */
    public Stream define(Stream stream) {
        Preconditions.checkState(this.channel != null, "channel is null");
        StreamMetadataRegistry registry = new StreamMetadataRegistry(this.group, this.channel);
        registry.create(stream);
        return registry.get(stream.getName());
    }

    /**
     * Define a new measure
     *
     * @param measure the measure to be created
     * @return a created measure in the BanyanDB
     */
    public Measure define(Measure measure) {
        Preconditions.checkState(this.channel != null, "channel is null");
        MeasureMetadataRegistry registry = new MeasureMetadataRegistry(this.group, this.channel);
        registry.create(measure);
        return registry.get(measure.getName());
    }

    /**
     * Bind index rule to the stream
     *
     * @param stream     the subject of index rule binding
     * @param beginAt    the start timestamp of this rule binding
     * @param expireAt   the expiry timestamp of this rule binding
     * @param indexRules rules to be bounded
     */
    public void defineIndexRules(Stream stream, ZonedDateTime beginAt, ZonedDateTime expireAt, IndexRule... indexRules) {
        Preconditions.checkArgument(stream != null, "measure cannot be null");
        Preconditions.checkState(this.channel != null, "channel is null");
        IndexRuleMetadataRegistry irRegistry = new IndexRuleMetadataRegistry(this.group, this.channel);
        List<String> indexRuleNames = new ArrayList<>(indexRules.length);
        for (IndexRule ir : indexRules) {
            irRegistry.create(ir);
            indexRuleNames.add(ir.getName());
        }
        IndexRuleBindingMetadataRegistry irbRegistry = new IndexRuleBindingMetadataRegistry(this.group, this.channel);
        IndexRuleBinding binding = new IndexRuleBinding(stream.getName() + "-index-rule-binding",
                IndexRuleBinding.Subject.referToStream(stream.getName()));
        binding.setRules(indexRuleNames);
        binding.setBeginAt(beginAt);
        binding.setExpireAt(expireAt);
        irbRegistry.create(binding);
    }

    /**
     * Bind index rule to the measure.
     * By default, the index rule binding will be active from now, and it will never be expired.
     *
     * @param measure    the subject of index rule binding
     * @param indexRules rules to be bounded
     */
    public void defineIndexRules(Measure measure, IndexRule... indexRules) {
        Preconditions.checkArgument(measure != null, "measure cannot be null");
        Preconditions.checkState(this.channel != null, "channel is null");
        IndexRuleMetadataRegistry irRegistry = new IndexRuleMetadataRegistry(this.group, this.channel);
        List<String> indexRuleNames = new ArrayList<>(indexRules.length);
        for (IndexRule ir : indexRules) {
            irRegistry.create(ir);
            indexRuleNames.add(ir.getName());
        }
        IndexRuleBindingMetadataRegistry irbRegistry = new IndexRuleBindingMetadataRegistry(this.group, this.channel);
        IndexRuleBinding binding = new IndexRuleBinding(measure.getName() + "-index-rule-binding",
                IndexRuleBinding.Subject.referToMeasure(measure.getName()));
        binding.setRules(indexRuleNames);
        irbRegistry.create(binding);
    }

    @Override
    public void close() throws IOException {
        connectionEstablishLock.lock();
        if (!(this.channel instanceof ManagedChannel)) {
            return;
        }
        final ManagedChannel managedChannel = (ManagedChannel) this.channel;
        try {
            if (isConnected) {
                managedChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                isConnected = false;
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            log.warn("fail to wait for channel termination, shutdown now!", interruptedException);
            managedChannel.shutdownNow();
            isConnected = false;
        } finally {
            connectionEstablishLock.unlock();
        }
    }
}
