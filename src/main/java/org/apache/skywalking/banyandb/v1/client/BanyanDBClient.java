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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import io.grpc.stub.StreamObserver;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.measure.v1.MeasureServiceGrpc;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;
import org.apache.skywalking.banyandb.stream.v1.StreamServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.metadata.Group;
import org.apache.skywalking.banyandb.v1.client.metadata.GroupMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRule;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleBinding;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleBindingMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.Measure;
import org.apache.skywalking.banyandb.v1.client.metadata.MeasureMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.Stream;
import org.apache.skywalking.banyandb.v1.client.metadata.StreamMetadataRegistry;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

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
     * Options for server connection.
     */
    @Getter(value = AccessLevel.PACKAGE)
    private final Options options;
    /**
     * gRPC connection.
     */
    @Getter(value = AccessLevel.PACKAGE)
    private volatile Channel channel;
    /**
     * gRPC client stub
     */
    @Getter(value = AccessLevel.PACKAGE)
    volatile StreamServiceGrpc.StreamServiceStub streamServiceStub;
    /**
     * gRPC client stub
     */
    @Getter(value = AccessLevel.PACKAGE)
    volatile MeasureServiceGrpc.MeasureServiceStub measureServiceStub;
    /**
     * gRPC blocking stub.
     */
    @Getter(value = AccessLevel.PACKAGE)
    volatile StreamServiceGrpc.StreamServiceBlockingStub streamServiceBlockingStub;
    /**
     * gRPC blocking stub.
     */
    @Getter(value = AccessLevel.PACKAGE)
    volatile MeasureServiceGrpc.MeasureServiceBlockingStub measureServiceBlockingStub;
    /**
     * The connection status.
     */
    private volatile boolean isConnected = false;
    /**
     * A lock to control the race condition in establishing and disconnecting network connection.
     */
    private volatile ReentrantLock connectionEstablishLock;

    /**
     * Create a BanyanDB client instance with a default options.
     *
     * @param host IP or domain name
     * @param port Server port
     */
    public BanyanDBClient(String host, int port) {
        this(host, port, new Options());
    }

    /**
     * Create a BanyanDB client instance with custom options
     *
     * @param host    IP or domain name
     * @param port    Server port
     * @param options for database connection
     */
    public BanyanDBClient(final String host,
                          final int port,
                          final Options options) {
        this.host = host;
        this.port = port;
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
                measureServiceStub = MeasureServiceGrpc.newStub(channel);
                streamServiceStub = StreamServiceGrpc.newStub(channel);
                streamServiceBlockingStub = StreamServiceGrpc.newBlockingStub(channel);
                measureServiceBlockingStub = MeasureServiceGrpc.newBlockingStub(channel);
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
                measureServiceStub = MeasureServiceGrpc.newStub(channel);
                streamServiceStub = StreamServiceGrpc.newStub(channel);
                streamServiceBlockingStub = StreamServiceGrpc.newBlockingStub(channel);
                measureServiceBlockingStub = MeasureServiceGrpc.newBlockingStub(channel);
                isConnected = true;
            }
        } finally {
            connectionEstablishLock.unlock();
        }
    }

    /**
     * Perform a single write with given entity.
     *
     * @param streamWrite the entity to be written
     */
    public void write(StreamWrite streamWrite) {
        checkState(this.streamServiceStub != null, "stream service is null");

        final StreamObserver<BanyandbStream.WriteRequest> writeRequestStreamObserver
                = this.streamServiceStub
                .write(
                        new StreamObserver<BanyandbStream.WriteResponse>() {
                            @Override
                            public void onNext(BanyandbStream.WriteResponse writeResponse) {
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                log.error("Error occurs in flushing streams.", throwable);
                            }

                            @Override
                            public void onCompleted() {
                            }
                        });
        try {
            writeRequestStreamObserver.onNext(streamWrite.build());
        } finally {
            writeRequestStreamObserver.onCompleted();
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
        checkState(this.streamServiceStub != null, "stream service is null");

        return new StreamBulkWriteProcessor(this.streamServiceStub, maxBulkSize, flushInterval, concurrency);
    }

    /**
     * Create a build process for measure write.
     *
     * @param maxBulkSize   the max bulk size for the flush operation
     * @param flushInterval if given maxBulkSize is not reached in this period, the flush would be trigger
     *                      automatically. Unit is second
     * @param concurrency   the number of concurrency would run for the flush max
     * @return stream bulk write processor
     */
    public MeasureBulkWriteProcessor buildMeasureWriteProcessor(int maxBulkSize, int flushInterval, int concurrency) {
        checkState(this.measureServiceStub != null, "measure service is null");

        return new MeasureBulkWriteProcessor(this.measureServiceStub, maxBulkSize, flushInterval, concurrency);
    }

    /**
     * Query streams according to given conditions
     *
     * @param streamQuery condition for query
     * @return hint streams.
     */
    public StreamQueryResponse query(StreamQuery streamQuery) {
        checkState(this.streamServiceStub != null, "stream service is null");

        final BanyandbStream.QueryResponse response = this.streamServiceBlockingStub
                .withDeadlineAfter(this.getOptions().getDeadline(), TimeUnit.SECONDS)
                .query(streamQuery.build());
        return new StreamQueryResponse(response);
    }

    /**
     * Query measures according to given conditions
     *
     * @param measureQuery condition for query
     * @return hint measures.
     */
    public MeasureQueryResponse query(MeasureQuery measureQuery) {
        checkState(this.streamServiceStub != null, "measure service is null");

        final BanyandbMeasure.QueryResponse response = this.measureServiceBlockingStub
                .withDeadlineAfter(this.getOptions().getDeadline(), TimeUnit.SECONDS)
                .query(measureQuery.build());
        return new MeasureQueryResponse(response);
    }

    /**
     * Define a new group and attach to the current client.
     *
     * @param group the group to be created
     * @return a grouped client
     */
    public Group define(Group group) {
        GroupMetadataRegistry registry = new GroupMetadataRegistry(checkNotNull(this.channel));
        registry.create(group);
        return registry.get(null, group.name());
    }

    /**
     * Define a new stream
     *
     * @param stream the stream to be created
     * @return a created stream in the BanyanDB
     */
    public Stream define(Stream stream) {
        StreamMetadataRegistry streamRegistry = new StreamMetadataRegistry(checkNotNull(this.channel));
        streamRegistry.create(stream);
        defineIndexRules(stream, stream.indexRules());
        Stream createdStream = streamRegistry.get(stream.group(), stream.name());

        List<IndexRule> indexRules = this.findIndexRulesByGroupAndBindingName(createdStream.group(), createdStream.name() + "-index-rule-binding");
        return createdStream.withIndexRules(indexRules);
    }

    /**
     * Define a new measure
     *
     * @param measure the measure to be created
     * @return a created measure in the BanyanDB
     */
    public Measure define(Measure measure) {
        MeasureMetadataRegistry measureRegistry = new MeasureMetadataRegistry(checkNotNull(this.channel));
        measureRegistry.create(measure);
        defineIndexRules(measure, measure.indexRules());
        Measure createdMeasure = measureRegistry.get(measure.group(), measure.name());

        List<IndexRule> indexRules = this.findIndexRulesByGroupAndBindingName(createdMeasure.group(), createdMeasure.name() + "-index-rule-binding");
        return createdMeasure.withIndexRules(indexRules);
    }

    /**
     * Bind index rule to the stream
     *
     * @param stream     the subject of index rule binding
     * @param indexRules rules to be bounded
     */
    private void defineIndexRules(Stream stream, List<IndexRule> indexRules) {
        Preconditions.checkArgument(stream != null, "measure cannot be null");

        IndexRuleMetadataRegistry irRegistry = new IndexRuleMetadataRegistry(checkNotNull(this.channel));
        for (final IndexRule ir : indexRules) {
            irRegistry.create(ir);
        }

        List<String> indexRuleNames = indexRules.stream().map(IndexRule::name).collect(Collectors.toList());

        IndexRuleBindingMetadataRegistry irbRegistry = new IndexRuleBindingMetadataRegistry(checkNotNull(this.channel));
        IndexRuleBinding binding = IndexRuleBinding.create(stream.group(),
                stream.name() + "-index-rule-binding",
                IndexRuleBinding.Subject.referToStream(stream.name()),
                indexRuleNames);
        irbRegistry.create(binding);
    }

    /**
     * Bind index rule to the measure.
     * By default, the index rule binding will be active from now, and it will never be expired.
     *
     * @param measure    the subject of index rule binding
     * @param indexRules rules to be bounded
     */
    private void defineIndexRules(Measure measure, List<IndexRule> indexRules) {
        Preconditions.checkArgument(measure != null, "measure cannot be null");

        IndexRuleMetadataRegistry irRegistry = new IndexRuleMetadataRegistry(checkNotNull(this.channel));
        for (final IndexRule ir : indexRules) {
            irRegistry.create(ir);
        }

        List<String> indexRuleNames = indexRules.stream().map(IndexRule::name).collect(Collectors.toList());

        IndexRuleBindingMetadataRegistry irbRegistry = new IndexRuleBindingMetadataRegistry(checkNotNull(this.channel));
        IndexRuleBinding binding = IndexRuleBinding.create(measure.group(),
                measure.name() + "-index-rule-binding",
                IndexRuleBinding.Subject.referToStream(measure.name()),
                indexRuleNames);
        irbRegistry.create(binding);
    }

    private List<IndexRule> findIndexRulesByGroupAndBindingName(String group, String bindingName) {
        IndexRuleBindingMetadataRegistry irbRegistry = new IndexRuleBindingMetadataRegistry(checkNotNull(this.channel));

        IndexRuleBinding irb = irbRegistry.get(group, bindingName);
        if (irb == null) {
            return Collections.emptyList();
        }

        IndexRuleMetadataRegistry irRegistry = new IndexRuleMetadataRegistry(checkNotNull(this.channel));
        List<IndexRule> indexRules = new ArrayList<>(irb.rules().size());
        for (final String rule : irb.rules()) {
            indexRules.add(irRegistry.get(group, rule));
        }
        return indexRules;
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
