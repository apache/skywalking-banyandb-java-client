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
import org.apache.skywalking.banyandb.database.v1.metadata.GroupRegistryServiceGrpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroupRegistry {
    private final GroupRegistryServiceGrpc.GroupRegistryServiceBlockingStub blockingStub;

    public GroupRegistry(Channel channel) {
        Preconditions.checkArgument(channel != null, "channel must not be null");
        this.blockingStub = GroupRegistryServiceGrpc.newBlockingStub(channel);
    }

    /**
     * create a group
     *
     * @param group the group to be created
     */
    public void create(final String group) {
        this.blockingStub.create(BanyandbMetadata.GroupRegistryServiceCreateRequest.newBuilder()
                .setGroup(group)
                .build());
    }

    /**
     * delete a group
     *
     * @param group the group to be deleted
     * @return bool that indicates whether the group has been deleted
     */
    public boolean delete(final String group) {
        BanyandbMetadata.GroupRegistryServiceDeleteResponse resp = this.blockingStub.delete(BanyandbMetadata.GroupRegistryServiceDeleteRequest.newBuilder()
                .setGroup(group)
                .build());

        if (resp == null) {
            return false;
        }

        return resp.getDeleted();
    }

    /**
     * check the existence of the group
     *
     * @param group the group name to be checked
     * @return bool that indicates whether the group exists
     */
    public boolean exist(final String group) {
        try {
            BanyandbMetadata.GroupRegistryServiceExistResponse resp = this.blockingStub.exist(BanyandbMetadata.GroupRegistryServiceExistRequest.newBuilder()
                    .setGroup(group)
                    .build());

            return resp != null;
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * List all groups in the database
     *
     * @return a list of groups
     */
    public List<String> list() {
        BanyandbMetadata.GroupRegistryServiceListResponse resp = this.blockingStub.list(BanyandbMetadata.GroupRegistryServiceListRequest.newBuilder()
                .build());
        if (resp == null) {
            return Collections.emptyList();
        }

        return new ArrayList<>(resp.getGroupList());
    }
}
