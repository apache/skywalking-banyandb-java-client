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
import org.apache.skywalking.banyandb.database.v1.MeasureRegistryServiceGrpc;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MeasureMetadataRegistry extends MetadataClient<BanyandbDatabase.Measure, Measure> {
    private final MeasureRegistryServiceGrpc.MeasureRegistryServiceBlockingStub blockingStub;

    public MeasureMetadataRegistry(String group, Channel channel) {
        super(group);
        Preconditions.checkArgument(channel != null, "channel must not be null");
        this.blockingStub = MeasureRegistryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void create(Measure payload) {
        blockingStub.create(BanyandbDatabase.MeasureRegistryServiceCreateRequest.newBuilder()
                .setMeasure(payload.serialize(this.group))
                .build());
    }

    @Override
    public void update(Measure payload) {
        blockingStub.update(BanyandbDatabase.MeasureRegistryServiceUpdateRequest.newBuilder()
                .setMeasure(payload.serialize(this.group))
                .build());
    }

    @Override
    public boolean delete(String name) {
        BanyandbDatabase.MeasureRegistryServiceDeleteResponse resp = blockingStub.delete(BanyandbDatabase.MeasureRegistryServiceDeleteRequest.newBuilder()
                .setMetadata(BanyandbCommon.Metadata.newBuilder().setGroup(this.group).setName(name).build())
                .build());
        return resp != null && resp.getDeleted();
    }

    @Override
    public Measure get(String name) {
        BanyandbDatabase.MeasureRegistryServiceGetResponse resp = blockingStub.get(BanyandbDatabase.MeasureRegistryServiceGetRequest.newBuilder()
                .setMetadata(BanyandbCommon.Metadata.newBuilder().setGroup(this.group).setName(name).build())
                .build());
        if (resp == null) {
            return null;
        }

        return Measure.fromProtobuf(resp.getMeasure());
    }

    @Override
    public List<Measure> list() {
        BanyandbDatabase.MeasureRegistryServiceListResponse resp = blockingStub.list(BanyandbDatabase.MeasureRegistryServiceListRequest.newBuilder()
                .setGroup(this.group)
                .build());
        if (resp == null) {
            return Collections.emptyList();
        }

        return resp.getMeasureList().stream().map(Measure::fromProtobuf).collect(Collectors.toList());
    }
}
