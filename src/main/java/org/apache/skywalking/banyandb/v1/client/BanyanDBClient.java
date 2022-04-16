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
import com.google.common.base.Strings;
import io.grpc.Channel;
import io.grpc.ManagedChannel;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.measure.v1.MeasureServiceGrpc;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;
import org.apache.skywalking.banyandb.stream.v1.StreamServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.grpc.HandleExceptionsWith;
import org.apache.skywalking.banyandb.v1.client.grpc.channel.ChannelManager;
import org.apache.skywalking.banyandb.v1.client.grpc.channel.DefaultChannelFactory;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.metadata.Group;
import org.apache.skywalking.banyandb.v1.client.metadata.GroupMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRule;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleBinding;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleBindingMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.Measure;
import org.apache.skywalking.banyandb.v1.client.metadata.MeasureMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.MetadataCache;
import org.apache.skywalking.banyandb.v1.client.metadata.Property;
import org.apache.skywalking.banyandb.v1.client.metadata.PropertyStore;
import org.apache.skywalking.banyandb.v1.client.metadata.Stream;
import org.apache.skywalking.banyandb.v1.client.metadata.StreamMetadataRegistry;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * BanyanDBClient represents a client instance interacting with BanyanDB server.
 * This is built on the top of BanyanDB v1 gRPC APIs.
 *
 * <pre>{@code
 * // use `default` group
 * client = new BanyanDBClient("127.0.0.1", 17912);
 * // to send any request, a connection to the server must be estabilished
 * client.connect();
 * }</pre>
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
    private StreamServiceGrpc.StreamServiceStub streamServiceStub;
    /**
     * gRPC client stub
     */
    @Getter(value = AccessLevel.PACKAGE)
    private MeasureServiceGrpc.MeasureServiceStub measureServiceStub;
    /**
     * gRPC future stub.
     */
    @Getter(value = AccessLevel.PACKAGE)
    private StreamServiceGrpc.StreamServiceBlockingStub streamServiceBlockingStub;
    /**
     * gRPC future stub.
     */
    @Getter(value = AccessLevel.PACKAGE)
    private MeasureServiceGrpc.MeasureServiceBlockingStub measureServiceBlockingStub;
    /**
     * The connection status.
     */
    private volatile boolean isConnected = false;
    /**
     * A lock to control the race condition in establishing and disconnecting network connection.
     */
    private final ReentrantLock connectionEstablishLock;

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
     * Create a BanyanDB client instance with a customized options.
     *
     * @param host    IP or domain name
     * @param port    Server port
     * @param options customized options
     */
    public BanyanDBClient(String host, int port, Options options) {
        this.host = host;
        this.port = port;
        this.options = options;
        this.connectionEstablishLock = new ReentrantLock();
    }

    /**
     * Construct a connection to the server.
     *
     * @throws IOException thrown if fail to create a connection
     */
    public void connect() throws IOException {
        connectionEstablishLock.lock();
        try {
            if (!isConnected) {
                this.channel = ChannelManager.create(this.options.buildChannelManagerSettings(),
                        new DefaultChannelFactory(this.host, this.port, this.options));
                streamServiceBlockingStub = StreamServiceGrpc.newBlockingStub(this.channel);
                measureServiceBlockingStub = MeasureServiceGrpc.newBlockingStub(this.channel);
                streamServiceStub = StreamServiceGrpc.newStub(this.channel);
                measureServiceStub = MeasureServiceGrpc.newStub(this.channel);
                isConnected = true;
            }
        } finally {
            connectionEstablishLock.unlock();
        }
    }

    @VisibleForTesting
    void connect(Channel channel) {
        connectionEstablishLock.lock();
        try {
            if (!isConnected) {
                this.channel = channel;
                streamServiceBlockingStub = StreamServiceGrpc.newBlockingStub(this.channel);
                measureServiceBlockingStub = MeasureServiceGrpc.newBlockingStub(this.channel);
                streamServiceStub = StreamServiceGrpc.newStub(this.channel);
                measureServiceStub = MeasureServiceGrpc.newStub(this.channel);
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
                .withDeadlineAfter(this.getOptions().getDeadline(), TimeUnit.SECONDS)
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
    public StreamQueryResponse query(StreamQuery streamQuery) throws BanyanDBException {
        checkState(this.streamServiceStub != null, "stream service is null");

        final BanyandbStream.QueryResponse response = HandleExceptionsWith.callAndTranslateApiException(() ->
                this.streamServiceBlockingStub
                        .withDeadlineAfter(this.getOptions().getDeadline(), TimeUnit.SECONDS)
                        .query(streamQuery.build()));
        return new StreamQueryResponse(response);
    }

    /**
     * Query measures according to given conditions
     *
     * @param measureQuery condition for query
     * @return hint measures.
     */
    public MeasureQueryResponse query(MeasureQuery measureQuery) throws BanyanDBException {
        checkState(this.streamServiceStub != null, "measure service is null");

        final BanyandbMeasure.QueryResponse response = HandleExceptionsWith.callAndTranslateApiException(() ->
                this.measureServiceBlockingStub
                        .withDeadlineAfter(this.getOptions().getDeadline(), TimeUnit.SECONDS)
                        .query(measureQuery.build()));
        return new MeasureQueryResponse(response);
    }

    /**
     * Define a new group and attach to the current client.
     *
     * @param group the group to be created
     * @return a grouped client
     */
    public Group define(Group group) throws BanyanDBException {
        GroupMetadataRegistry registry = new GroupMetadataRegistry(checkNotNull(this.channel));
        registry.create(group);
        return registry.get(null, group.name());
    }

    /**
     * Define a new stream
     *
     * @param stream the stream to be created
     */
    public void define(Stream stream) throws BanyanDBException {
        StreamMetadataRegistry streamRegistry = new StreamMetadataRegistry(checkNotNull(this.channel));
        streamRegistry.create(stream);
        defineIndexRules(stream, stream.indexRules());
        MetadataCache.INSTANCE.register(stream);
    }

    /**
     * Define a new measure
     *
     * @param measure the measure to be created
     */
    public void define(Measure measure) throws BanyanDBException {
        MeasureMetadataRegistry measureRegistry = new MeasureMetadataRegistry(checkNotNull(this.channel));
        measureRegistry.create(measure);
        defineIndexRules(measure, measure.indexRules());
        MetadataCache.INSTANCE.register(measure);
    }

    /**
     * Create or update the property
     *
     * @param property the property to be stored in the BanyanBD
     */
    public void save(Property property) throws BanyanDBException {
        PropertyStore store = new PropertyStore(checkNotNull(this.channel));
        try {
            store.get(property.group(), property.name(), property.id());
            store.update(property);
        } catch (BanyanDBException ex) {
            if (ex.getStatus().equals(Status.Code.NOT_FOUND)) {
                store.create(property);
                return;
            }
            throw ex;
        }
    }

    /**
     * Find property
     *
     * @param group group of the metadata
     * @param name  name of the metadata
     * @param id    identity of the property
     * @return property if it can be found
     */
    public Property findProperty(String group, String name, String id) throws BanyanDBException {
        PropertyStore store = new PropertyStore(checkNotNull(this.channel));
        return store.get(group, name, id);
    }

    /**
     * Delete property
     *
     * @param group group of the metadata
     * @param name  name of the metadata
     * @param id    identity of the property
     * @return if this property has been deleted
     */
    public boolean deleteProperty(String group, String name, String id) throws BanyanDBException {
        PropertyStore store = new PropertyStore(checkNotNull(this.channel));
        return store.delete(group, name, id);
    }

    /**
     * Bind index rule to the stream
     *
     * @param stream     the subject of index rule binding
     * @param indexRules rules to be bounded
     */
    private void defineIndexRules(Stream stream, List<IndexRule> indexRules) throws BanyanDBException {
        Preconditions.checkArgument(stream != null, "measure cannot be null");

        IndexRuleMetadataRegistry irRegistry = new IndexRuleMetadataRegistry(checkNotNull(this.channel));
        for (final IndexRule ir : indexRules) {
            irRegistry.create(ir);
        }

        List<String> indexRuleNames = indexRules.stream().map(IndexRule::name).collect(Collectors.toList());

        IndexRuleBindingMetadataRegistry irbRegistry = new IndexRuleBindingMetadataRegistry(checkNotNull(this.channel));
        IndexRuleBinding binding = IndexRuleBinding.create(stream.group(),
                IndexRuleBinding.defaultBindingRule(stream.name()),
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
    private void defineIndexRules(Measure measure, List<IndexRule> indexRules) throws BanyanDBException {
        Preconditions.checkArgument(measure != null, "measure cannot be null");

        IndexRuleMetadataRegistry irRegistry = new IndexRuleMetadataRegistry(checkNotNull(this.channel));
        for (final IndexRule ir : indexRules) {
            irRegistry.create(ir);
        }

        List<String> indexRuleNames = indexRules.stream().map(IndexRule::name).collect(Collectors.toList());

        IndexRuleBindingMetadataRegistry irbRegistry = new IndexRuleBindingMetadataRegistry(checkNotNull(this.channel));
        IndexRuleBinding binding = IndexRuleBinding.create(measure.group(),
                IndexRuleBinding.defaultBindingRule(measure.name()),
                IndexRuleBinding.Subject.referToStream(measure.name()),
                indexRuleNames);
        irbRegistry.create(binding);
    }

    /**
     * Try to find the stream from the BanyanDB with given group and name.
     *
     * @param group group of the stream
     * @param name  name of the stream
     * @return Steam with index rules if found. Otherwise, null is returned.
     */
    public Stream findStream(String group, String name) throws BanyanDBException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

        Stream s = new StreamMetadataRegistry(checkNotNull(this.channel)).get(group, name);
        return s.withIndexRules(findIndexRulesByGroupAndBindingName(group, IndexRuleBinding.defaultBindingRule(name)));
    }

    /**
     * Try to find the measure from the BanyanDB with given group and name.
     *
     * @param group group of the measure
     * @param name  name of the measure
     * @return Measure with index rules if found. Otherwise, null is returned.
     */
    public Measure findMeasure(String group, String name) throws BanyanDBException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

        Measure m = new MeasureMetadataRegistry(checkNotNull(this.channel)).get(group, name);
        return m.withIndexRules(findIndexRulesByGroupAndBindingName(group, IndexRuleBinding.defaultBindingRule(name)));
    }

    private List<IndexRule> findIndexRulesByGroupAndBindingName(String group, String bindingName) throws BanyanDBException {
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
