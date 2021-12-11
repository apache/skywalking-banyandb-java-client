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

import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.banyandb.stream.v1.StreamServiceGrpc;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * StreamBulkWriteProcessor works for stream flush.
 */
@Slf4j
@ThreadSafe
public class StreamBulkWriteProcessor extends BulkWriteProcessor {
    /**
     * The BanyanDB instance name.
     */
    private final String group;
    private StreamServiceGrpc.StreamServiceStub streamServiceStub;

    /**
     * Create the processor.
     *
     * @param streamServiceStub stub for gRPC call.
     * @param maxBulkSize       the max bulk size for the flush operation
     * @param flushInterval     if given maxBulkSize is not reached in this period, the flush would be trigger
     *                          automatically. Unit is second.
     * @param concurrency       the number of concurrency would run for the flush max.
     */
    protected StreamBulkWriteProcessor(final String group,
                                       final StreamServiceGrpc.StreamServiceStub streamServiceStub,
                                       final int maxBulkSize,
                                       final int flushInterval,
                                       final int concurrency) {
        super("StreamBulkWriteProcessor", maxBulkSize, flushInterval, concurrency);
        this.group = group;
        this.streamServiceStub = streamServiceStub;
    }

    /**
     * Add the stream to the bulk processor.
     *
     * @param streamWrite to add.
     */
    public void add(StreamWrite streamWrite) {
        this.buffer.produce(streamWrite);
    }

    @Override
    protected void flush(final List data) {
        final StreamObserver<BanyandbStream.WriteRequest> writeRequestStreamObserver
                = streamServiceStub.withDeadlineAfter(
                        flushInterval, TimeUnit.SECONDS)
                .write(
                        new StreamObserver<BanyandbStream.WriteResponse>() {
                            @Override
                            public void onNext(
                                    BanyandbStream.WriteResponse writeResponse) {
                            }

                            @Override
                            public void onError(
                                    Throwable throwable) {
                                log.error(
                                        "Error occurs in flushing streams.",
                                        throwable
                                );
                            }

                            @Override
                            public void onCompleted() {
                            }
                        });
        try {
            data.forEach(write -> {
                final StreamWrite streamWrite = (StreamWrite) write;
                BanyandbStream.WriteRequest request = streamWrite.build(group);
                writeRequestStreamObserver.onNext(request);
            });
        } finally {
            writeRequestStreamObserver.onCompleted();
        }
    }
}
