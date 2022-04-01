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

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.database.v1.IndexRuleRegistryServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.AbstractBanyanDBClientTest;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBApiException;
import org.apache.skywalking.banyandb.v1.client.metadata.IndexRuleMetadataRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.powermock.api.mockito.PowerMockito.mock;

public class ExceptionTest extends AbstractBanyanDBClientTest {
    @Before
    public void setUp() throws IOException {
        super.setUp();
    }

    @Test
    public void testStatusInvalidArgument() {
        final IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceImplBase serviceImpl =
                mock(IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceImplBase.class, delegatesTo(
                        new IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceImplBase() {
                            @Override
                            public void get(BanyandbDatabase.IndexRuleRegistryServiceGetRequest request, StreamObserver<BanyandbDatabase.IndexRuleRegistryServiceGetResponse> responseObserver) {
                                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("invalid arg").asRuntimeException());
                            }
                        }));

        serviceRegistry.addService(serviceImpl);

        try {
            new IndexRuleMetadataRegistry(this.channel).get("group", "trace_id");
            Assert.fail();
        } catch (BanyanDBApiException ex) {
            Assert.assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus());
            Assert.assertTrue(ex.getMessage().contains("invalid arg"));
        }
    }
}
