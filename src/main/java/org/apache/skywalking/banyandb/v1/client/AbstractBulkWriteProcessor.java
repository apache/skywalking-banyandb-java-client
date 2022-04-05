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

import io.grpc.stub.AbstractAsyncStub;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.banyandb.v1.client.grpc.GRPCStreamServiceStatus;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractBulkWriteProcessor<REQ extends com.google.protobuf.GeneratedMessageV3,
        STUB extends AbstractAsyncStub<STUB>> extends BulkWriteProcessor {
    private final STUB stub;

    /**
     * Create the processor.
     *
     * @param stub          an implementation of {@link AbstractAsyncStub}
     * @param processorName name of the processor for logging
     * @param maxBulkSize   the max bulk size for the flush operation
     * @param flushInterval if given maxBulkSize is not reached in this period, the flush would be trigger
     *                      automatically. Unit is second.
     * @param concurrency   the number of concurrency would run for the flush max.
     */
    protected AbstractBulkWriteProcessor(STUB stub, String processorName, int maxBulkSize, int flushInterval, int concurrency) {
        super(processorName, maxBulkSize, flushInterval, concurrency);
        this.stub = stub;
    }

    /**
     * Add the measure to the bulk processor.
     *
     * @param writeEntity to add.
     */
    public void add(AbstractWrite<REQ> writeEntity) {
        this.buffer.produce(writeEntity);
    }

    @Override
    protected void flush(List data) {
        final GRPCStreamServiceStatus status = new GRPCStreamServiceStatus(false);
        final StreamObserver<REQ> writeRequestStreamObserver
                = this.buildStreamObserver(stub.withDeadlineAfter(flushInterval, TimeUnit.SECONDS), status);

        try {
            data.forEach(write -> {
                REQ request = ((AbstractWrite<REQ>) write).build();
                writeRequestStreamObserver.onNext(request);
            });
        } catch (Throwable t) {
            log.error("Transform and send request to BanyanDB fail.", t);
        } finally {
            writeRequestStreamObserver.onCompleted();
        }

        status.wait4Finish();
    }

    protected abstract StreamObserver<REQ> buildStreamObserver(STUB stub, GRPCStreamServiceStatus status);
}
