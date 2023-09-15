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
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.measure.v1.MeasureServiceGrpc;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;
import org.apache.skywalking.banyandb.stream.v1.StreamServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.grpc.HandleExceptionsWith;
import org.apache.skywalking.banyandb.v1.client.grpc.channel.ChannelManager;
import org.apache.skywalking.banyandb.v1.client.grpc.channel.DefaultChannelFactory;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.InternalException;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.InvalidArgumentException;
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
import org.apache.skywalking.banyandb.v1.client.metadata.ResourceExist;
import org.apache.skywalking.banyandb.v1.client.metadata.Stream;
import org.apache.skywalking.banyandb.v1.client.metadata.StreamMetadataRegistry;
import org.apache.skywalking.banyandb.v1.client.metadata.TopNAggregation;
import org.apache.skywalking.banyandb.v1.client.metadata.TopNAggregationMetadataRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

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
     * Client local metadata cache.
     */
    private final MetadataCache metadataCache;

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
        this.metadataCache = new MetadataCache();
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
     * @return a future of write result
     */
    public CompletableFuture<Void> write(StreamWrite streamWrite) {
        checkState(this.streamServiceStub != null, "stream service is null");

        CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamObserver<BanyandbStream.WriteRequest> writeRequestStreamObserver
                = this.streamServiceStub
                .withDeadlineAfter(this.getOptions().getDeadline(), TimeUnit.SECONDS)
                .write(
                        new StreamObserver<BanyandbStream.WriteResponse>() {
                            private BanyanDBException responseException;

                            @Override
                            public void onNext(BanyandbStream.WriteResponse writeResponse) {
                                switch (writeResponse.getStatus()) {
                                    case STATUS_INVALID_TIMESTAMP:
                                        responseException = new InvalidArgumentException(
                                                "Invalid timestamp: " + streamWrite.getTimestamp(), null, Status.Code.INVALID_ARGUMENT, false);
                                        break;
                                    case STATUS_NOT_FOUND:
                                        responseException = new InvalidArgumentException(
                                                "Invalid metadata: " + streamWrite.entityMetadata, null, Status.Code.INVALID_ARGUMENT, false);
                                        break;
                                    case STATUS_EXPIRED_SCHEMA:
                                        BanyandbCommon.Metadata metadata = writeResponse.getMetadata();
                                        log.warn("The schema {}.{} is expired, trying update the schema...",
                                                metadata.getGroup(), metadata.getName());
                                        try {
                                            BanyanDBClient.this.findStream(metadata.getGroup(), metadata.getName());
                                        } catch (BanyanDBException e) {
                                            String warnMessage = String.format("Failed to refresh the stream schema %s.%s",
                                                    metadata.getGroup(), metadata.getName());
                                            log.warn(warnMessage, e);
                                        }
                                        responseException = new InvalidArgumentException(
                                                "Expired revision: " + metadata.getModRevision(), null, Status.Code.INVALID_ARGUMENT, true);
                                        break;
                                    case STATUS_INTERNAL_ERROR:
                                        responseException = new InternalException(
                                                "Internal error occurs in server", null, Status.Code.INTERNAL, true);
                                        break;
                                    default:
                                }
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                log.error("Error occurs in flushing streams.", throwable);
                                future.completeExceptionally(throwable);
                            }

                            @Override
                            public void onCompleted() {
                                if (responseException == null) {
                                    future.complete(null);
                                } else {
                                    future.completeExceptionally(responseException);
                                }
                            }
                        });
        try {
            writeRequestStreamObserver.onNext(streamWrite.build());
        } finally {
            writeRequestStreamObserver.onCompleted();
        }
        return future;
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

        return new StreamBulkWriteProcessor(this, maxBulkSize, flushInterval, concurrency);
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

        return new MeasureBulkWriteProcessor(this, maxBulkSize, flushInterval, concurrency);
    }

    /**
     * Build a MeasureWrite request.
     *
     * @param group     the group of the measure
     * @param name      the name of the measure
     * @param timestamp the timestamp of the measure
     * @return the request to be built
     */
    public MeasureWrite createMeasureWrite(String group, String name, long timestamp) {
        return new MeasureWrite(this.metadataCache.findMetadata(group, name), timestamp);
    }

    /**
     * Build a StreamWrite request.
     *
     * @param group     the group of the stream
     * @param name      the name of the stream
     * @param elementId the primary key of the stream
     * @return the request to be built
     */
    public StreamWrite createStreamWrite(String group, String name, final String elementId) {
        return new StreamWrite(this.metadataCache.findMetadata(group, name), elementId);
    }

    /**
     * Build a StreamWrite request.
     *
     * @param group     the group of the stream
     * @param name      the name of the stream
     * @param elementId the primary key of the stream
     * @param timestamp the timestamp of the stream
     * @return the request to be built
     */
    public StreamWrite createStreamWrite(String group, String name, final String elementId, long timestamp) {
        return new StreamWrite(this.metadataCache.findMetadata(group, name), elementId, timestamp);
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
                        .query(streamQuery.build(this.metadataCache.findMetadata(streamQuery.group, streamQuery.name))));
        return new StreamQueryResponse(response);
    }

    /**
     * Query TopN according to given conditions
     *
     * @param topNQuery condition for query
     * @return hint topN.
     */
    public TopNQueryResponse query(TopNQuery topNQuery) throws BanyanDBException {
        checkState(this.measureServiceStub != null, "measure service is null");

        final BanyandbMeasure.TopNResponse response = HandleExceptionsWith.callAndTranslateApiException(() ->
                this.measureServiceBlockingStub
                        .withDeadlineAfter(this.getOptions().getDeadline(), TimeUnit.SECONDS)
                        .topN(topNQuery.build()));
        return new TopNQueryResponse(response);
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
                        .query(measureQuery.build(this.metadataCache.findMetadata(measureQuery.group, measureQuery.name))));
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
        long modRevision = streamRegistry.create(stream);
        defineIndexRules(stream, stream.indexRules());

        stream = stream.withModRevision(modRevision);
        this.metadataCache.register(stream);
    }

    /**
     * Delete a stream
     *
     * @param stream the stream to be deleted
     * @return true if the stream is deleted successfully
     */
    public boolean delete(Stream stream) throws BanyanDBException {
        StreamMetadataRegistry streamRegistry = new StreamMetadataRegistry(checkNotNull(this.channel));
        if (streamRegistry.delete(stream.group(), stream.name())) {
            this.metadataCache.unregister(stream);
            return true;
        }
        return false;
    }

    /**
     * Define a new measure
     *
     * @param measure the measure to be created
     */
    public void define(Measure measure) throws BanyanDBException {
        MeasureMetadataRegistry measureRegistry = new MeasureMetadataRegistry(checkNotNull(this.channel));
        long modRevision = measureRegistry.create(measure);
        defineIndexRules(measure, measure.indexRules());

        measure = measure.withModRevision(modRevision);
        this.metadataCache.register(measure);
    }

    /**
     * Delete a measure
     *
     * @param measure the measure to be deleted
     * @return true if the measure is deleted successfully
     */
    public boolean delete(Measure measure) throws BanyanDBException {
        MeasureMetadataRegistry measureRegistry = new MeasureMetadataRegistry(checkNotNull(this.channel));
        if (measureRegistry.delete(measure.group(), measure.name())) {
            this.metadataCache.unregister(measure);
            return true;
        }
        return false;
    }

    /**
     * Define a new TopNAggregation
     *
     * @param topNAggregation the topN rule to be created
     */
    public void define(TopNAggregation topNAggregation) throws BanyanDBException {
        TopNAggregationMetadataRegistry registry = new TopNAggregationMetadataRegistry(checkNotNull(this.channel));
        registry.create(topNAggregation);
    }

    /**
     * Apply(Create or update) the property with {@link PropertyStore.Strategy#MERGE}
     *
     * @param property the property to be stored in the BanyanBD
     */
    public PropertyStore.ApplyResult apply(Property property) throws BanyanDBException {
        PropertyStore store = new PropertyStore(checkNotNull(this.channel));
        return store.apply(property);
    }

    /**
     * Apply(Create or update) the property
     *
     * @param property the property to be stored in the BanyanBD
     * @param strategy dedicates how to apply the property
     */
    public PropertyStore.ApplyResult apply(Property property, PropertyStore.Strategy strategy) throws
            BanyanDBException {
        PropertyStore store = new PropertyStore(checkNotNull(this.channel));
        return store.apply(property, strategy);
    }

    /**
     * Find property
     *
     * @param group group of the metadata
     * @param name  name of the metadata
     * @param id    identity of the property
     * @param tags  tags to be returned
     * @return property if it can be found
     */
    public Property findProperty(String group, String name, String id, String... tags) throws BanyanDBException {
        PropertyStore store = new PropertyStore(checkNotNull(this.channel));
        return store.get(group, name, id, tags);
    }

    /**
     * List Properties
     *
     * @param group group of the metadata
     * @param name  name of the metadata
     * @return all properties belonging to the group and the name
     */
    public List<Property> findProperties(String group, String name) throws BanyanDBException {
        return findProperties(group, name, null, null);

    }

    /**
     * List Properties
     *
     * @param group group of the metadata
     * @param name  name of the metadata
     * @param ids   identities of the properties
     * @param tags  tags to be returned
     * @return all properties belonging to the group and the name
     */
    public List<Property> findProperties(String group, String name, List<String> ids, List<String> tags) throws BanyanDBException {
        PropertyStore store = new PropertyStore(checkNotNull(this.channel));
        return store.list(group, name, ids, tags);
    }

    /**
     * Delete property
     *
     * @param group group of the metadata
     * @param name  name of the metadata
     * @param id    identity of the property
     * @param tags  tags to be deleted. If null, the property is deleted
     * @return if this property has been deleted
     */
    public PropertyStore.DeleteResult deleteProperty(String group, String name, String id, String... tags) throws
            BanyanDBException {
        PropertyStore store = new PropertyStore(checkNotNull(this.channel));
        return store.delete(group, name, id, tags);
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
            try {
                irRegistry.create(ir);
            } catch (BanyanDBException ex) {
                if (ex.getStatus().equals(Status.Code.ALREADY_EXISTS)) {
                    continue;
                }
                throw ex;
            }
        }
        if (indexRules.isEmpty()) {
            return;
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
            try {
                irRegistry.create(ir);
            } catch (BanyanDBException ex) {
                // multiple entity can share a single index rule
                if (ex.getStatus().equals(Status.Code.ALREADY_EXISTS)) {
                    continue;
                }
                throw ex;
            }
        }
        if (indexRules.isEmpty()) {
            return;
        }

        List<String> indexRuleNames = indexRules.stream().map(IndexRule::name).collect(Collectors.toList());

        IndexRuleBindingMetadataRegistry irbRegistry = new IndexRuleBindingMetadataRegistry(checkNotNull(this.channel));
        IndexRuleBinding binding = IndexRuleBinding.create(measure.group(),
                IndexRuleBinding.defaultBindingRule(measure.name()),
                IndexRuleBinding.Subject.referToMeasure(measure.name()),
                indexRuleNames);
        irbRegistry.create(binding);
    }

    /**
     * Try to find the group defined
     *
     * @param name name of the group
     * @return the group found in BanyanDB
     */
    public Group findGroup(String name) throws BanyanDBException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

        try {
            return new GroupMetadataRegistry(checkNotNull(this.channel)).get(name, name);
        } catch (BanyanDBException ex) {
            if (ex.getStatus().equals(Status.Code.NOT_FOUND)) {
                return null;
            }

            throw ex;
        }
    }

    /**
     * Try to find the TopNAggregation from the BanyanDB with given group and name.
     *
     * @param group group of the TopNAggregation
     * @param name  name of the TopNAggregation
     * @return TopNAggregation if found. Otherwise, null is returned.
     */
    public TopNAggregation findTopNAggregation(String group, String name) throws BanyanDBException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

        return new TopNAggregationMetadataRegistry(checkNotNull(this.channel)).get(group, name);
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
        s = s.withIndexRules(findIndexRulesByGroupAndBindingName(group, IndexRuleBinding.defaultBindingRule(name)));
        this.metadataCache.register(s);
        return s;
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
        m = m.withIndexRules(findIndexRulesByGroupAndBindingName(group, IndexRuleBinding.defaultBindingRule(name)));
        this.metadataCache.register(m);
        return m;
    }

    private List<IndexRule> findIndexRulesByGroupAndBindingName(String group, String bindingName) throws
            BanyanDBException {
        IndexRuleBindingMetadataRegistry irbRegistry = new IndexRuleBindingMetadataRegistry(checkNotNull(this.channel));

        IndexRuleBinding irb;
        try {
            irb = irbRegistry.get(group, bindingName);
        } catch (BanyanDBException ex) {
            if (ex.getStatus().equals(Status.Code.NOT_FOUND)) {
                return Collections.emptyList();
            }
            throw ex;
        }

        if (irb == null) {
            return Collections.emptyList();
        }

        IndexRuleMetadataRegistry irRegistry = new IndexRuleMetadataRegistry(checkNotNull(this.channel));
        List<IndexRule> indexRules = new ArrayList<>(irb.rules().size());
        for (final String rule : irb.rules()) {
            try {
                indexRules.add(irRegistry.get(group, rule));
            } catch (BanyanDBException ex) {
                if (ex.getStatus().equals(Status.Code.NOT_FOUND)) {
                    continue;
                }
                throw ex;
            }
        }
        return indexRules;
    }

    /**
     * Check if the given stream exists.
     *
     * @param group group of the stream
     * @param name  name of the stream
     * @return ResourceExist which indicates whether group and stream exist
     */
    public ResourceExist existStream(String group, String name) throws BanyanDBException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

        return new StreamMetadataRegistry(checkNotNull(this.channel)).exist(group, name);
    }

    /**
     * Check if the given measure exists.
     *
     * @param group group of the measure
     * @param name  name of the measure
     * @return ResourceExist which indicates whether group and measure exist
     */
    public ResourceExist existMeasure(String group, String name) throws BanyanDBException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

        return new MeasureMetadataRegistry(checkNotNull(this.channel)).exist(group, name);
    }

    /**
     * Check if the given TopNAggregation exists.
     *
     * @param group group of the TopNAggregation
     * @param name  name of the TopNAggregation
     * @return ResourceExist which indicates whether group and TopNAggregation exist
     */
    public ResourceExist existTopNAggregation(String group, String name) throws BanyanDBException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

        return new TopNAggregationMetadataRegistry(checkNotNull(this.channel)).exist(group, name);
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
