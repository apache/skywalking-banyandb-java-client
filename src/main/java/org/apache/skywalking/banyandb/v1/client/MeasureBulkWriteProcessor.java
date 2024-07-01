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

import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.measure.v1.MeasureServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;

import javax.annotation.concurrent.ThreadSafe;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * MeasureBulkWriteProcessor works for measure flush.
 */
@Slf4j
@ThreadSafe
public class MeasureBulkWriteProcessor extends AbstractBulkWriteProcessor<BanyandbMeasure.WriteRequest,
        MeasureServiceGrpc.MeasureServiceStub> {
    private final BanyanDBClient client;

    /**
     * Create the processor.
     *
     * @param client        the client
     * @param maxBulkSize   the max bulk size for the flush operation
     * @param flushInterval if given maxBulkSize is not reached in this period, the flush would be trigger
     *                      automatically. Unit is second.
     * @param timeout       network timeout threshold in seconds.
     * @param concurrency   the number of concurrency would run for the flush max.
     */
    protected MeasureBulkWriteProcessor(
            final BanyanDBClient client,
            final int maxBulkSize,
            final int flushInterval,
            final int concurrency,
            final int timeout) {
        super(client.getMeasureServiceStub(), "MeasureBulkWriteProcessor", maxBulkSize, flushInterval, concurrency, timeout);
        this.client = client;
    }

    @Override
    protected StreamObserver<BanyandbMeasure.WriteRequest> buildStreamObserver(MeasureServiceGrpc.MeasureServiceStub stub,
                                                                               CompletableFuture<Void> batch) {
        return stub.write(new StreamObserver<BanyandbMeasure.WriteResponse>() {
            private final Set<String> schemaExpired = new HashSet<>();

            @Override
            public void onNext(BanyandbMeasure.WriteResponse writeResponse) {
                switch (writeResponse.getStatus()) {
                    case STATUS_EXPIRED_SCHEMA:
                        BanyandbCommon.Metadata metadata = writeResponse.getMetadata();
                        String schemaKey = metadata.getGroup() + "." + metadata.getName();
                        if (!schemaExpired.contains(schemaKey)) {
                            log.warn("The schema {} is expired, trying update the schema...", schemaKey);
                            try {
                                client.findMeasure(metadata.getGroup(), metadata.getName());
                                schemaExpired.add(schemaKey);
                            } catch (BanyanDBException e) {
                                log.error(e.getMessage(), e);
                            }
                        }
                        break;
                    default:
                }
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
