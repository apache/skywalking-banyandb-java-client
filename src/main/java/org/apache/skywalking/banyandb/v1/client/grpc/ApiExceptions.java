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

package org.apache.skywalking.banyandb.v1.client.grpc;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.grpc.Status;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBApiException;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBGrpcApiExceptionFactory;

import java.util.concurrent.Future;

public class ApiExceptions {
    private ApiExceptions() {
    }

    private static final BanyanDBGrpcApiExceptionFactory EXCEPTION_FACTORY = new BanyanDBGrpcApiExceptionFactory(
            // Exceptions caused by network issues are retryable
            Sets.newHashSet(Status.Code.UNAVAILABLE, Status.Code.DEADLINE_EXCEEDED)
    );

    /**
     * call the underlying operation and get response from the future.
     *
     * @param future a gRPC future
     * @param <RESP> a generic type of user-defined gRPC response
     * @return response in the type of  defined in the gRPC protocol
     * @throws BanyanDBApiException                             if the execution of the future itself thrown an exception
     * @throws java.util.concurrent.CancellationException       if the future is got cancelled
     * @throws com.google.common.util.concurrent.ExecutionError if java.lang.Error is thrown during the execution
     */
    public static <RESP> RESP callAndTranslateApiException(Future<RESP> future) {
        try {
            return Futures.getUnchecked(future);
        } catch (UncheckedExecutionException exception) {
            throw EXCEPTION_FACTORY.createException(exception.getCause());
        }
    }
}
