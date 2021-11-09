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
import org.apache.skywalking.banyandb.database.v1.metadata.IndexRuleRegistryServiceGrpc;
import org.apache.skywalking.banyandb.v1.Banyandb;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IndexRuleMetadataRegistry extends MetadataClient<BanyandbMetadata.IndexRule, IndexRule> {
    private final IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceBlockingStub blockingStub;

    public IndexRuleMetadataRegistry(String group, Channel channel) {
        super(group);
        Preconditions.checkArgument(channel != null, "channel must not be null");
        this.blockingStub = IndexRuleRegistryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void create(IndexRule payload) {
        blockingStub.create(BanyandbMetadata.IndexRuleRegistryServiceCreateRequest.newBuilder()
                .setIndexRule(payload.serialize(this.group))
                .build());
    }

    @Override
    public void update(IndexRule payload) {
        blockingStub.update(BanyandbMetadata.IndexRuleRegistryServiceUpdateRequest.newBuilder()
                .setIndexRule(payload.serialize(this.group))
                .build());
    }

    @Override
    public boolean delete(String name) {
        BanyandbMetadata.IndexRuleRegistryServiceDeleteResponse resp = blockingStub.delete(BanyandbMetadata.IndexRuleRegistryServiceDeleteRequest.newBuilder()
                .setMetadata(Banyandb.Metadata.newBuilder().setGroup(this.group).setName(name).build())
                .build());
        return resp != null && resp.getDeleted();
    }

    @Override
    public IndexRule get(String name) {
        BanyandbMetadata.IndexRuleRegistryServiceGetResponse resp = blockingStub.get(BanyandbMetadata.IndexRuleRegistryServiceGetRequest.newBuilder()
                .setMetadata(Banyandb.Metadata.newBuilder().setGroup(this.group).setName(name).build())
                .build());
        if (resp == null) {
            return null;
        }

        return IndexRule.fromProtobuf(resp.getIndexRule());
    }

    @Override
    public List<IndexRule> list() {
        BanyandbMetadata.IndexRuleRegistryServiceListResponse resp = blockingStub.list(BanyandbMetadata.IndexRuleRegistryServiceListRequest.newBuilder()
                .setGroup(this.group)
                .build());
        if (resp == null) {
            return Collections.emptyList();
        }

        return resp.getIndexRuleList().stream().map(IndexRule::fromProtobuf).collect(Collectors.toList());
    }
}
