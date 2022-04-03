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

import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.banyandb.stream.v1.StreamServiceGrpc;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;
import org.apache.skywalking.banyandb.v1.client.grpc.GRPCStreamServiceStatus;

import javax.annotation.concurrent.ThreadSafe;

/**
 * StreamBulkWriteProcessor works for stream flush.
 */
@Slf4j
@ThreadSafe
public class StreamBulkWriteProcessor extends AbstractBulkWriteProcessor<BanyandbStream.WriteRequest,
        StreamServiceGrpc.StreamServiceStub> {
    /**
     * Create the processor.
     *
     * @param serviceStub   stub for gRPC call.
     * @param maxBulkSize   the max bulk size for the flush operation
     * @param flushInterval if given maxBulkSize is not reached in this period, the flush would be trigger
     *                      automatically. Unit is second.
     * @param concurrency   the number of concurrency would run for the flush max.
     */
    protected StreamBulkWriteProcessor(
            final StreamServiceGrpc.StreamServiceStub serviceStub,
            final int maxBulkSize,
            final int flushInterval,
            final int concurrency) {
        super(serviceStub, "StreamBulkWriteProcessor", maxBulkSize, flushInterval, concurrency);
    }

    @Override
    protected StreamObserver<BanyandbStream.WriteRequest> buildStreamObserver(StreamServiceGrpc.StreamServiceStub stub, GRPCStreamServiceStatus status) {
        return stub.write(
                new StreamObserver<BanyandbStream.WriteResponse>() {
                    @Override
                    public void onNext(BanyandbStream.WriteResponse writeResponse) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        status.finished();
                        log.error("Error occurs in flushing streams", t);
                    }

                    @Override
                    public void onCompleted() {
                        status.finished();
                    }
                });
    }
}
