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
import org.apache.skywalking.banyandb.database.v1.IndexRuleRegistryServiceGrpc;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IndexRuleMetadataRegistry extends MetadataClient<BanyandbDatabase.IndexRule, IndexRule> {
    private final IndexRuleRegistryServiceGrpc.IndexRuleRegistryServiceBlockingStub blockingStub;

    public IndexRuleMetadataRegistry(Channel channel) {
        Preconditions.checkArgument(channel != null, "channel must not be null");
        this.blockingStub = IndexRuleRegistryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void create(IndexRule payload) {
        blockingStub.create(BanyandbDatabase.IndexRuleRegistryServiceCreateRequest.newBuilder()
                .setIndexRule(payload.serialize())
                .build());
    }

    @Override
    public void update(IndexRule payload) {
        blockingStub.update(BanyandbDatabase.IndexRuleRegistryServiceUpdateRequest.newBuilder()
                .setIndexRule(payload.serialize())
                .build());
    }

    @Override
    public boolean delete(String group, String name) {
        BanyandbDatabase.IndexRuleRegistryServiceDeleteResponse resp = blockingStub.delete(BanyandbDatabase.IndexRuleRegistryServiceDeleteRequest.newBuilder()
                .setMetadata(BanyandbCommon.Metadata.newBuilder().setGroup(group).setName(name).build())
                .build());
        return resp != null && resp.getDeleted();
    }

    @Override
    public IndexRule get(String group, String name) {
        BanyandbDatabase.IndexRuleRegistryServiceGetResponse resp = blockingStub.get(BanyandbDatabase.IndexRuleRegistryServiceGetRequest.newBuilder()
                .setMetadata(BanyandbCommon.Metadata.newBuilder().setGroup(group).setName(name).build())
                .build());
        if (resp == null) {
            return null;
        }

        return IndexRule.fromProtobuf(resp.getIndexRule());
    }

    @Override
    public List<IndexRule> list(String group) {
        BanyandbDatabase.IndexRuleRegistryServiceListResponse resp = blockingStub.list(BanyandbDatabase.IndexRuleRegistryServiceListRequest.newBuilder()
                .setGroup(group)
                .build());
        if (resp == null) {
            return Collections.emptyList();
        }

        return resp.getIndexRuleList().stream().map(IndexRule::fromProtobuf).collect(Collectors.toList());
    }
}
