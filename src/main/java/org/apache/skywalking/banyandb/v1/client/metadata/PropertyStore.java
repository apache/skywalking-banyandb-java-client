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

package org.apache.skywalking.banyandb.v1.client.metadata;

import io.grpc.Channel;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.property.v1.BanyandbProperty;
import org.apache.skywalking.banyandb.property.v1.BanyandbProperty.Property;
import org.apache.skywalking.banyandb.property.v1.BanyandbProperty.ApplyRequest;
import org.apache.skywalking.banyandb.property.v1.BanyandbProperty.ApplyRequest.Strategy;
import org.apache.skywalking.banyandb.property.v1.BanyandbProperty.ApplyResponse;
import org.apache.skywalking.banyandb.property.v1.BanyandbProperty.DeleteRequest;
import org.apache.skywalking.banyandb.property.v1.BanyandbProperty.DeleteResponse;
import org.apache.skywalking.banyandb.property.v1.PropertyServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.grpc.HandleExceptionsWith;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;

import java.util.Arrays;
import java.util.List;

public class PropertyStore {
    private final PropertyServiceGrpc.PropertyServiceBlockingStub stub;

    public PropertyStore(Channel channel) {
        this.stub = PropertyServiceGrpc.newBlockingStub(channel);
    }

    public ApplyResponse apply(Property payload) throws BanyanDBException {
        return apply(payload, Strategy.STRATEGY_MERGE);
    }

    public ApplyResponse apply(Property payload, Strategy strategy) throws BanyanDBException {
        BanyandbProperty.ApplyRequest.Strategy s = BanyandbProperty.ApplyRequest.Strategy.STRATEGY_MERGE;
        ApplyRequest r = BanyandbProperty.ApplyRequest.newBuilder()
                .setProperty(payload)
                .setStrategy(strategy)
                .build();
        return HandleExceptionsWith.callAndTranslateApiException(() ->
                this.stub.apply(r));
    }

    public DeleteResponse delete(String group, String name, String id, String... tags) throws BanyanDBException {
        DeleteRequest.Builder b = DeleteRequest.newBuilder();
        if (tags != null && tags.length > 0) {
            b.addAllTags(Arrays.asList(tags));
        }
        return HandleExceptionsWith.callAndTranslateApiException(() ->
                this.stub.delete(b.setMetadata(BanyandbProperty.Metadata
                                .newBuilder()
                                .setContainer(BanyandbCommon.Metadata.newBuilder()
                                        .setGroup(group)
                                        .setName(name)
                                        .build())
                                .setId(id)
                                .build())
                        .build()));
    }

    public Property get(String group, String name, String id, String... tags) throws BanyanDBException {
        BanyandbProperty.GetRequest.Builder b = BanyandbProperty.GetRequest.newBuilder();
        if (tags != null && tags.length > 0) {
            b.addAllTags(Arrays.asList(tags));
        }
        BanyandbProperty.GetResponse resp = HandleExceptionsWith.callAndTranslateApiException(() ->
                this.stub.get(b.setMetadata(BanyandbProperty.Metadata
                                .newBuilder()
                                .setContainer(BanyandbCommon.Metadata.newBuilder()
                                        .setGroup(group)
                                        .setName(name)
                                        .build())
                                .setId(id)
                                .build())
                        .build()));

        return resp.getProperty();
    }

    public List<Property> list(String group, String name, List<String> ids, List<String> tags) throws BanyanDBException {
        BanyandbProperty.ListRequest.Builder builder = BanyandbProperty.ListRequest.newBuilder()
                .setContainer(BanyandbCommon.Metadata.newBuilder()
                .setGroup(group)
                .setName(name)
                .build());
        if (ids != null && ids.size() > 0) {
            builder.addAllIds(ids);
        }
        if (tags != null && tags.size() > 0) {
            builder.addAllTags(tags);
        }
        BanyandbProperty.ListResponse resp = HandleExceptionsWith.callAndTranslateApiException(() ->
                this.stub.list(builder.build()));

        return resp.getPropertyList();
    }

    public void keepAlive(long leaseId) throws BanyanDBException {
        BanyandbProperty.KeepAliveRequest req = BanyandbProperty.KeepAliveRequest.newBuilder()
                .setLeaseId(leaseId)
                .build();
        HandleExceptionsWith.callAndTranslateApiException(() ->
                this.stub.keepAlive(req));
    }

//    public enum Strategy {
//        MERGE, REPLACE
//    }
//
//    @AutoValue
//    public abstract static class ApplyResult {
//        public abstract boolean created();
//
//        public abstract int tagsNum();
//
//        public abstract long leaseId();
//    }
//
//    @AutoValue
//    public abstract static class DeleteResult {
//        public abstract boolean deleted();
//
//        public abstract int tagsNum();
//    }
}
