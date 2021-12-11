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

import com.google.common.base.Preconditions;
import io.grpc.Channel;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.database.v1.StreamRegistryServiceGrpc;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class StreamMetadataRegistry extends MetadataClient<BanyandbDatabase.Stream, Stream> {
    private final StreamRegistryServiceGrpc.StreamRegistryServiceBlockingStub blockingStub;

    public StreamMetadataRegistry(String group, Channel channel) {
        super(group);
        Preconditions.checkArgument(channel != null, "channel must not be null");
        this.blockingStub = StreamRegistryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void create(Stream payload) {
        blockingStub.create(BanyandbDatabase.StreamRegistryServiceCreateRequest.newBuilder()
                .setStream(payload.serialize(this.group))
                .build());
    }

    @Override
    public void update(Stream payload) {
        blockingStub.update(BanyandbDatabase.StreamRegistryServiceUpdateRequest.newBuilder()
                .setStream(payload.serialize(this.group))
                .build());
    }

    @Override
    public boolean delete(String name) {
        BanyandbDatabase.StreamRegistryServiceDeleteResponse resp = blockingStub.delete(BanyandbDatabase.StreamRegistryServiceDeleteRequest.newBuilder()
                .setMetadata(BanyandbCommon.Metadata.newBuilder().setGroup(this.group).setName(name).build())
                .build());
        return resp != null && resp.getDeleted();
    }

    @Override
    public Stream get(String name) {
        BanyandbDatabase.StreamRegistryServiceGetResponse resp = blockingStub.get(BanyandbDatabase.StreamRegistryServiceGetRequest.newBuilder()
                .setMetadata(BanyandbCommon.Metadata.newBuilder().setGroup(this.group).setName(name).build())
                .build());
        if (resp == null) {
            return null;
        }

        return Stream.fromProtobuf(resp.getStream());
    }

    @Override
    public List<Stream> list() {
        BanyandbDatabase.StreamRegistryServiceListResponse resp = blockingStub.list(BanyandbDatabase.StreamRegistryServiceListRequest.newBuilder()
                .setGroup(this.group)
                .build());
        if (resp == null) {
            return Collections.emptyList();
        }

        return resp.getStreamList().stream().map(Stream::fromProtobuf).collect(Collectors.toList());
    }
}
