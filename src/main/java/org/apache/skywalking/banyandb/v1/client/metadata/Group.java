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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import java.time.ZonedDateTime;

@Setter
@Getter
@EqualsAndHashCode(callSuper = true)
public class Group extends NamedSchema<BanyandbCommon.Group> {
    /**
     * catalog denotes which type of data the group contains
     */
    private final Catalog catalog;

    /**
     * shard_num is the number of shards in this group
     */
    private final int shardNum;

    private final int blockNum;

    private final Duration ttl;

    public Group(String name, Catalog catalog, int shardNum, int blockNum, Duration ttl) {
        this(name, catalog, shardNum, blockNum, ttl, null);
    }

    public Group(String name, Catalog catalog, int shardNum, int blockNum, Duration ttl, ZonedDateTime updatedAt) {
        super(null, name, updatedAt);
        this.catalog = catalog;
        this.shardNum = shardNum;
        this.blockNum = blockNum;
        this.ttl = ttl;
    }

    @Override
    public BanyandbCommon.Group serialize() {
        return BanyandbCommon.Group.newBuilder()
                // use name as the group
                .setMetadata(this.buildMetadata())
                .setCatalog(this.catalog.getCatalog())
                .setResourceOpts(BanyandbCommon.ResourceOpts.newBuilder()
                        .setShardNum(this.shardNum)
                        .setBlockNum(this.blockNum)
                        .setTtl(this.ttl.format())
                        .build())
                .build();
    }

    public static Group fromProtobuf(BanyandbCommon.Group group) {
        Catalog catalog = Catalog.UNSPECIFIED;
        switch (group.getCatalog()) {
            case CATALOG_STREAM:
                catalog = Catalog.STREAM;
                break;
            case CATALOG_MEASURE:
                catalog = Catalog.MEASURE;
                break;
        }

        return new Group(group.getMetadata().getName(),
                catalog,
                group.getResourceOpts().getShardNum(),
                group.getResourceOpts().getBlockNum(),
                Duration.parse(group.getResourceOpts().getTtl()),
                TimeUtils.parseTimestamp(group.getUpdatedAt()));
    }
}
