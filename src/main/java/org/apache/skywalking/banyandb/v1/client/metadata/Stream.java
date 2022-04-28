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
import com.google.common.collect.ImmutableList;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@AutoValue
public abstract class Stream extends NamedSchema<BanyandbDatabase.Stream> {
    /**
     * specs of tag families
     */
    abstract ImmutableList<TagFamilySpec> tagFamilies();

    /**
     * tag names used to generate an entity
     */
    abstract ImmutableList<String> entityRelativeTags();

    /**
     * index rules bound to the stream
     */
    public abstract ImmutableList<IndexRule> indexRules();

    abstract Builder toBuilder();

    public final Stream withIndexRules(List<IndexRule> indexRules) {
        return toBuilder().addIndexes(indexRules).build();
    }

    public static Stream.Builder create(String group, String name) {
        return new AutoValue_Stream.Builder().setGroup(group).setName(name);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        abstract String group();

        abstract Builder setGroup(String group);

        abstract Builder setName(String name);

        abstract Builder setUpdatedAt(ZonedDateTime updatedAt);

        abstract ImmutableList.Builder<TagFamilySpec> tagFamiliesBuilder();

        public final Builder addTagFamily(TagFamilySpec tagFamilySpec) {
            tagFamiliesBuilder().add(tagFamilySpec);
            return this;
        }

        public final Builder addTagFamilies(Iterable<TagFamilySpec> tagFamilySpecs) {
            tagFamiliesBuilder().addAll(tagFamilySpecs);
            return this;
        }

        abstract ImmutableList.Builder<IndexRule> indexRulesBuilder();

        public final Builder addIndexes(Iterable<IndexRule> indexRules) {
            for (final IndexRule ir : indexRules) {
                this.addIndex(ir);
            }
            return this;
        }

        public final Builder addIndex(IndexRule indexRule) {
            indexRulesBuilder().add(indexRule.withGroup(group()));
            return this;
        }

        public abstract Builder setEntityRelativeTags(String... entityRelativeTags);

        public abstract Builder setEntityRelativeTags(List<String> entityRelativeTags);

        public abstract Stream build();
    }

    @Override
    public BanyandbDatabase.Stream serialize() {
        List<BanyandbDatabase.TagFamilySpec> metadataTagFamilySpecs = new ArrayList<>(this.tagFamilies().size());
        for (final TagFamilySpec spec : this.tagFamilies()) {
            metadataTagFamilySpecs.add(spec.serialize());
        }

        BanyandbDatabase.Stream.Builder b = BanyandbDatabase.Stream.newBuilder()
                .setMetadata(buildMetadata())
                .addAllTagFamilies(metadataTagFamilySpecs)
                .setEntity(BanyandbDatabase.Entity.newBuilder().addAllTagNames(entityRelativeTags()).build());

        if (this.updatedAt() != null) {
            b.setUpdatedAt(TimeUtils.buildTimestamp(updatedAt()));
        }
        return b.build();
    }

    public static Stream fromProtobuf(final BanyandbDatabase.Stream pb) {
        Stream.Builder s = Stream.create(pb.getMetadata().getGroup(), pb.getMetadata().getName())
                .setUpdatedAt(TimeUtils.parseTimestamp(pb.getUpdatedAt()))
                .setEntityRelativeTags(pb.getEntity().getTagNamesList());
        // build tag family spec
        for (int i = 0; i < pb.getTagFamiliesCount(); i++) {
            s.addTagFamily(TagFamilySpec.fromProtobuf(pb.getTagFamilies(i)));
        }
        return s.build();
    }
}
