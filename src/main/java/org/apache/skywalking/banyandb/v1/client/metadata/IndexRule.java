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
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.skywalking.banyandb.database.v1.metadata.BanyandbMetadata;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@EqualsAndHashCode(callSuper = true)
public class IndexRule extends NamedSchema<BanyandbMetadata.IndexRule> {
    /**
     * tags are the combination that refers to an indexed object
     * If the elements in tags are more than 1, the object will generate a multi-tag index
     * Caveat: All tags in a multi-tag MUST have an identical IndexType
     */
    private List<String> tags;

    /**
     * indexType determine the index structure under the hood
     */
    private IndexType indexType;

    /**
     * indexLocation indicates where to store index.
     */
    private IndexLocation indexLocation;

    public IndexRule(String name, IndexType indexType, IndexLocation indexLocation) {
        this(name, indexType, indexLocation, null);
    }

    private IndexRule(String name, IndexType indexType, IndexLocation indexLocation, ZonedDateTime updatedAt) {
        super(name, updatedAt);
        this.tags = new ArrayList<>();
        this.indexType = indexType;
        this.indexLocation = indexLocation;
    }

    /**
     * Add tag to the index rule
     *
     * @param tag the name of the tag to be appended
     */
    public IndexRule addTag(String tag) {
        this.tags.add(tag);
        return this;
    }

    @Override
    public BanyandbMetadata.IndexRule serialize(String group) {
        BanyandbMetadata.IndexRule.Builder b = BanyandbMetadata.IndexRule.newBuilder()
                .setMetadata(buildMetadata(group))
                .addAllTags(this.tags)
                .setLocation(this.indexLocation.location)
                .setType(this.indexType.type);

        if (this.updatedAt != null) {
            b.setUpdatedAt(TimeUtils.buildTimestamp(this.updatedAt));
        }
        return b.build();
    }

    static IndexRule fromProtobuf(BanyandbMetadata.IndexRule pb) {
        IndexType indexType = IndexType.fromProtobuf(pb.getType());
        IndexLocation indexLocation = IndexLocation.fromProtobuf(pb.getLocation());
        IndexRule indexRule = new IndexRule(pb.getMetadata().getName(), indexType, indexLocation,
                TimeUtils.parseTimestamp(pb.getUpdatedAt()));
        indexRule.setTags(new ArrayList<>(pb.getTagsList()));
        return indexRule;
    }

    @RequiredArgsConstructor
    public enum IndexType {
        TREE(BanyandbMetadata.IndexRule.Type.TYPE_TREE), INVERTED(BanyandbMetadata.IndexRule.Type.TYPE_INVERTED);

        private final BanyandbMetadata.IndexRule.Type type;

        private static IndexType fromProtobuf(BanyandbMetadata.IndexRule.Type type) {
            switch (type) {
                case TYPE_TREE:
                    return TREE;
                case TYPE_INVERTED:
                    return INVERTED;
                default:
                    throw new IllegalArgumentException("unrecognized index type");
            }
        }
    }

    @RequiredArgsConstructor
    public enum IndexLocation {
        SERIES(BanyandbMetadata.IndexRule.Location.LOCATION_SERIES), GLOBAL(BanyandbMetadata.IndexRule.Location.LOCATION_GLOBAL);

        private final BanyandbMetadata.IndexRule.Location location;

        private static IndexLocation fromProtobuf(BanyandbMetadata.IndexRule.Location loc) {
            switch (loc) {
                case LOCATION_GLOBAL:
                    return GLOBAL;
                case LOCATION_SERIES:
                    return SERIES;
                default:
                    throw new IllegalArgumentException("unrecognized index location");
            }
        }
    }
}
