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

import com.google.auto.value.AutoValue;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import java.time.ZonedDateTime;

@AutoValue
public abstract class Group extends NamedSchema<BanyandbCommon.Group> {
    /**
     * catalog denotes which type of data the group contains
     */
    abstract Catalog catalog();

    /**
     * shard_num is the number of shards in this group
     */
    abstract int shardNum();

    abstract IntervalRule blockInterval();

    abstract IntervalRule segmentInterval();

    abstract IntervalRule ttl();

    public static Group create(String name, Catalog catalog, int shardNum, IntervalRule blockInterval, IntervalRule segmentInterval, IntervalRule ttl) {
        return new AutoValue_Group(null, name, null, catalog, shardNum, blockInterval, segmentInterval, ttl);
    }

    public static Group create(String name, Catalog catalog, int shardNum, IntervalRule blockInterval, IntervalRule segmentInterval, IntervalRule ttl, ZonedDateTime updatedAt) {
        return new AutoValue_Group(null, name, updatedAt, catalog, shardNum, blockInterval, segmentInterval, ttl);
    }

    @Override
    public BanyandbCommon.Group serialize() {
        return BanyandbCommon.Group.newBuilder()
                // use name as the group
                .setMetadata(this.buildMetadata().toBuilder())
                .setCatalog(catalog().getCatalog())
                .setResourceOpts(BanyandbCommon.ResourceOpts.newBuilder()
                        .setShardNum(shardNum())
                        .setBlockInterval(blockInterval().serialize())
                        .setSegmentInterval(segmentInterval().serialize())
                        .setTtl(ttl().serialize())
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

        return new AutoValue_Group(null,
                group.getMetadata().getName(),
                TimeUtils.parseTimestamp(group.getUpdatedAt()),
                catalog,
                group.getResourceOpts().getShardNum(),
                IntervalRule.fromProtobuf(group.getResourceOpts().getBlockInterval()),
                IntervalRule.fromProtobuf(group.getResourceOpts().getSegmentInterval()),
                IntervalRule.fromProtobuf(group.getResourceOpts().getTtl()));
    }
}
