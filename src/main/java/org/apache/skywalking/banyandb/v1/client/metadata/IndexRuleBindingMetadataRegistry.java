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
import org.apache.skywalking.banyandb.database.v1.metadata.BanyandbMetadata;
import org.apache.skywalking.banyandb.database.v1.metadata.IndexRuleBindingRegistryServiceGrpc;
import org.apache.skywalking.banyandb.v1.Banyandb;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IndexRuleBindingMetadataRegistry extends MetadataClient<BanyandbMetadata.IndexRuleBinding, IndexRuleBinding> {
    private final IndexRuleBindingRegistryServiceGrpc.IndexRuleBindingRegistryServiceBlockingStub blockingStub;

    public IndexRuleBindingMetadataRegistry(String group, Channel channel) {
        super(group);
        Preconditions.checkArgument(channel != null, "channel must not be null");
        this.blockingStub = IndexRuleBindingRegistryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void create(IndexRuleBinding payload) {
        blockingStub.create(BanyandbMetadata.IndexRuleBindingRegistryServiceCreateRequest.newBuilder()
                .setIndexRuleBinding(payload.serialize(this.group))
                .build());
    }

    @Override
    public void update(IndexRuleBinding payload) {
        blockingStub.update(BanyandbMetadata.IndexRuleBindingRegistryServiceUpdateRequest.newBuilder()
                .setIndexRuleBinding(payload.serialize(this.group))
                .build());
    }

    @Override
    public boolean delete(String name) {
        BanyandbMetadata.IndexRuleBindingRegistryServiceDeleteResponse resp = blockingStub.delete(BanyandbMetadata.IndexRuleBindingRegistryServiceDeleteRequest.newBuilder()
                .setMetadata(Banyandb.Metadata.newBuilder().setGroup(this.group).setName(name).build())
                .build());
        return resp != null && resp.getDeleted();
    }

    @Override
    public IndexRuleBinding get(String name) {
        BanyandbMetadata.IndexRuleBindingRegistryServiceGetResponse resp = blockingStub.get(BanyandbMetadata.IndexRuleBindingRegistryServiceGetRequest.newBuilder()
                .setMetadata(Banyandb.Metadata.newBuilder().setGroup(this.group).setName(name).build())
                .build());
        if (resp == null) {
            return null;
        }

        return IndexRuleBinding.fromProtobuf(resp.getIndexRuleBinding());
    }

    @Override
    public List<IndexRuleBinding> list() {
        BanyandbMetadata.IndexRuleBindingRegistryServiceListResponse resp = blockingStub.list(BanyandbMetadata.IndexRuleBindingRegistryServiceListRequest.newBuilder()
                .setGroup(this.group)
                .build());
        if (resp == null) {
            return Collections.emptyList();
        }

        return resp.getIndexRuleBindingList().stream().map(IndexRuleBinding::fromProtobuf).collect(Collectors.toList());
    }
}