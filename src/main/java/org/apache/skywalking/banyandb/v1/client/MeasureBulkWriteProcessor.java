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
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.measure.v1.MeasureServiceGrpc;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;

/**
 * MeasureBulkWriteProcessor works for measure flush.
 */
@Slf4j
@ThreadSafe
public class MeasureBulkWriteProcessor extends AbstractBulkWriteProcessor<BanyandbMeasure.WriteRequest,
        MeasureServiceGrpc.MeasureServiceStub> {
    /**
     * Create the processor.
     *
     * @param measureServiceStub stub for gRPC call.
     * @param maxBulkSize        the max bulk size for the flush operation
     * @param flushInterval      if given maxBulkSize is not reached in this period, the flush would be trigger
     *                           automatically. Unit is second.
     * @param concurrency        the number of concurrency would run for the flush max.
     */
    protected MeasureBulkWriteProcessor(
            final MeasureServiceGrpc.MeasureServiceStub measureServiceStub,
            final int maxBulkSize,
            final int flushInterval,
            final int concurrency) {
        super(measureServiceStub, "MeasureBulkWriteProcessor", maxBulkSize, flushInterval, concurrency);
    }

    @Override
    protected StreamObserver<BanyandbMeasure.WriteRequest> buildStreamObserver(MeasureServiceGrpc.MeasureServiceStub stub,
                                                                               CompletableFuture<Void> batch) {
        return stub.write(new StreamObserver<BanyandbMeasure.WriteResponse>() {
            @Override
            public void onNext(BanyandbMeasure.WriteResponse writeResponse) {

            }

            @Override
            public void onError(Throwable t) {
                batch.completeExceptionally(t);
                log.error("Error occurs in flushing measures", t);
            }

            @Override
            public void onCompleted() {
                batch.complete(null);
            }
        });
    }
}
