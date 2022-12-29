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

import com.google.auto.value.AutoValue;
import io.grpc.stub.AbstractAsyncStub;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

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
    public CompletableFuture<Void> add(AbstractWrite<REQ> writeEntity) {
        final CompletableFuture<Void> f = new CompletableFuture<>();
        this.buffer.produce(Holder.create(writeEntity, f));
        return f;
    }

    @Override
    protected void flush(List data) {
        final CompletableFuture<Void> batch = new CompletableFuture<>();
        final StreamObserver<REQ> writeRequestStreamObserver
                = this.buildStreamObserver(stub.withDeadlineAfter(flushInterval, TimeUnit.SECONDS), batch);

        List sentData = new ArrayList(data.size());
        try {
            data.forEach(holder -> {
                Holder h = (Holder) holder;
                AbstractWrite<REQ> entity = (AbstractWrite<REQ>) h.writeEntity();
                REQ request;
                try {
                    request = entity.build();
                } catch (Throwable bt) {
                    log.error("building the entity fails: {}", entity.toString(), bt);
                    h.future().completeExceptionally(bt);
                    return;
                }
                writeRequestStreamObserver.onNext(request);
                sentData.add(h);
            });
        } catch (Throwable t) {
            log.error("Transform and send request to BanyanDB fail.", t);
            batch.completeExceptionally(t);
        } finally {
            writeRequestStreamObserver.onCompleted();
        }
        batch.whenComplete((ignored, exp) -> {
            if (exp != null) {
                sentData.stream().map((Function<Object, CompletableFuture<Void>>) o -> ((Holder) o).future())
                        .forEach((Consumer<CompletableFuture<Void>>) it -> it.completeExceptionally(exp));
                log.error("Failed to execute requests in bulk", exp);
            } else {
                log.debug("Succeeded to execute {} requests in bulk", data.size());
                sentData.stream().map((Function<Object, CompletableFuture<Void>>) o -> ((Holder) o).future())
                        .forEach((Consumer<CompletableFuture<Void>>) it -> it.complete(null));
            }
        });
        try {
            batch.get(30, TimeUnit.SECONDS);
        } catch (Throwable t) {
            log.error("Waiting responses from BanyanDB fail.", t);
        }
    }

    protected abstract StreamObserver<REQ> buildStreamObserver(STUB stub, CompletableFuture<Void> batch);

    @AutoValue
    static abstract class Holder {
        abstract AbstractWrite writeEntity();

        abstract CompletableFuture<Void> future();

        public static <REQ extends com.google.protobuf.GeneratedMessageV3> Holder create(AbstractWrite<REQ> writeEntity, CompletableFuture<Void> future) {
            return new AutoValue_AbstractBulkWriteProcessor_Holder(writeEntity, future);
        }

    }
}
