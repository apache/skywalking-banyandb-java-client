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
import org.apache.skywalking.banyandb.database.v1.GroupRegistryServiceGrpc;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class GroupMetadataRegistry extends MetadataClient<BanyandbCommon.Group, Group> {
    private final GroupRegistryServiceGrpc.GroupRegistryServiceBlockingStub blockingStub;

    public GroupMetadataRegistry(Channel channel) {
        Preconditions.checkArgument(channel != null, "channel must not be null");
        this.blockingStub = GroupRegistryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void create(Group payload) {
        blockingStub.create(BanyandbDatabase.GroupRegistryServiceCreateRequest.newBuilder()
                .setGroup(payload.serialize())
                .build());
    }

    @Override
    public void update(Group payload) {
        blockingStub.update(BanyandbDatabase.GroupRegistryServiceUpdateRequest.newBuilder()
                .setGroup(payload.serialize())
                .build());
    }

    @Override
    public boolean delete(String group, String name) {
        BanyandbDatabase.GroupRegistryServiceDeleteResponse resp = blockingStub.delete(BanyandbDatabase.GroupRegistryServiceDeleteRequest.newBuilder()
                .setGroup(name)
                .build());
        return resp != null && resp.getDeleted();
    }

    @Override
    public Group get(String group, String name) {
        BanyandbDatabase.GroupRegistryServiceGetResponse resp = blockingStub.get(BanyandbDatabase.GroupRegistryServiceGetRequest.newBuilder()
                .setGroup(name)
                .build());
        if (resp == null) {
            return null;
        }

        return Group.fromProtobuf(resp.getGroup());
    }

    @Override
    public List<Group> list(String group) {
        BanyandbDatabase.GroupRegistryServiceListResponse resp = blockingStub.list(BanyandbDatabase.GroupRegistryServiceListRequest.newBuilder()
                .build());
        if (resp == null) {
            return Collections.emptyList();
        }

        return resp.getGroupList().stream().map(Group::fromProtobuf).collect(Collectors.toList());
    }
}