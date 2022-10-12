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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import javax.annotation.Nullable;
import java.time.ZonedDateTime;

@AutoValue
public abstract class IndexRule extends NamedSchema<BanyandbDatabase.IndexRule> {
    /**
     * tags are the combination that refers to an indexed object
     * If the elements in tags are more than 1, the object will generate a multi-tag index
     * Caveat: All tags in a multi-tag MUST have an identical IndexType
     */
    abstract ImmutableList<String> tags();

    /**
     * indexType determine the index structure under the hood
     */
    abstract IndexType indexType();

    /**
     * indexLocation indicates where to store index.
     */
    abstract IndexLocation indexLocation();

    /**
     * analyzer indicates how to analyze the value.
     */
    @Nullable
    abstract Analyzer analyzer();

    abstract Builder toBuilder();

    public final IndexRule withGroup(String group) {
        return toBuilder().setGroup(group).build();
    }

    public static IndexRule create(String name, IndexType indexType, IndexLocation indexLocation) {
        return new AutoValue_IndexRule.Builder().setName(name)
                .setTags(ImmutableList.of(name))
                .setIndexType(indexType)
                .setIndexLocation(indexLocation)
                .build();
    }

    @VisibleForTesting
    static IndexRule create(String group, String name, IndexType indexType, IndexLocation indexLocation) {
        return new AutoValue_IndexRule.Builder().setGroup(group).setName(name)
                .setTags(ImmutableList.of(name))
                .setIndexType(indexType)
                .setIndexLocation(indexLocation)
                .build();
    }

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder setGroup(String group);

        abstract Builder setName(String name);

        abstract Builder setTags(ImmutableList<String> tags);

        abstract Builder setIndexType(IndexType indexType);

        abstract Builder setIndexLocation(IndexLocation indexLocation);

        abstract Builder setAnalyzer(Analyzer analyzer);

        abstract Builder setUpdatedAt(ZonedDateTime updatedAt);

        abstract IndexRule build();
    }

    @Override
    public BanyandbDatabase.IndexRule serialize() {
        final BanyandbDatabase.IndexRule.Builder b = BanyandbDatabase.IndexRule.newBuilder()
                .setMetadata(buildMetadata())
                .addAllTags(tags())
                .setLocation(indexLocation().location)
                .setType(indexType().type);

        if (updatedAt() != null) {
            b.setUpdatedAt(TimeUtils.buildTimestamp(updatedAt()));
        }
        return b.build();
    }

    public static IndexRule fromProtobuf(BanyandbDatabase.IndexRule pb) {
        IndexType indexType = IndexType.fromProtobuf(pb.getType());
        IndexLocation indexLocation = IndexLocation.fromProtobuf(pb.getLocation());
        return new AutoValue_IndexRule.Builder()
                .setGroup(pb.getMetadata().getGroup())
                .setName(pb.getMetadata().getName())
                .setUpdatedAt(TimeUtils.parseTimestamp(pb.getUpdatedAt()))
                .setIndexLocation(indexLocation)
                .setIndexType(indexType)
                .setTags(ImmutableList.copyOf(pb.getTagsList())).build();
    }

    @RequiredArgsConstructor
    public enum IndexType {
        TREE(BanyandbDatabase.IndexRule.Type.TYPE_TREE), INVERTED(BanyandbDatabase.IndexRule.Type.TYPE_INVERTED);

        private final BanyandbDatabase.IndexRule.Type type;

        private static IndexType fromProtobuf(BanyandbDatabase.IndexRule.Type type) {
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
        SERIES(BanyandbDatabase.IndexRule.Location.LOCATION_SERIES), GLOBAL(BanyandbDatabase.IndexRule.Location.LOCATION_GLOBAL);

        private final BanyandbDatabase.IndexRule.Location location;

        private static IndexLocation fromProtobuf(BanyandbDatabase.IndexRule.Location loc) {
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

    @RequiredArgsConstructor
    public enum Analyzer {
        KEYWORD(BanyandbDatabase.IndexRule.Analyzer.ANALYZER_KEYWORD), STANDARD(BanyandbDatabase.IndexRule.Analyzer.ANALYZER_STANDARD),
        SIMPLE(BanyandbDatabase.IndexRule.Analyzer.ANALYZER_SIMPLE);

        private final BanyandbDatabase.IndexRule.Analyzer analyzer;

        private static Analyzer fromProtobuf(BanyandbDatabase.IndexRule.Analyzer analyzer) {
            switch (analyzer) {
                case ANALYZER_KEYWORD:
                    return KEYWORD;
                case ANALYZER_SIMPLE:
                    return SIMPLE;
                case ANALYZER_STANDARD:
                    return STANDARD;
                default:
                    throw new IllegalArgumentException("unrecognized analyzer");
            }
        }
    }
}
