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
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.property.v1.BanyandbProperty;
import org.apache.skywalking.banyandb.v1.client.TagAndValue;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@AutoValue
public abstract class Property extends NamedSchema<BanyandbProperty.Property> {
    abstract String id();

    abstract ImmutableList<TagAndValue<?>> tags();

    @Override
    public BanyandbProperty.Property serialize() {
        List<BanyandbModel.Tag> tags = new ArrayList<>(this.tags().size());
        for (final TagAndValue<?> tagAndValue : this.tags()) {
            tags.add(tagAndValue.build());
        }
        return BanyandbProperty.Property.newBuilder()
                .setMetadata(BanyandbProperty.Metadata.newBuilder()
                        .setId(id())
                        .setContainer(buildMetadata())
                        .build())
                .addAllTags(tags)
                .build();
    }

    public static Builder create(String group, String name, String id) {
        return new AutoValue_Property.Builder().setGroup(group).setName(name).setId(id);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        abstract Builder setGroup(String group);

        abstract Builder setName(String name);

        abstract Builder setUpdatedAt(ZonedDateTime updatedAt);

        public abstract Builder setId(String id);

        abstract ImmutableList.Builder<TagAndValue<?>> tagsBuilder();

        public final Builder addTag(TagAndValue<?> tagAndValue) {
            tagsBuilder().add(tagAndValue);
            return this;
        }

        public abstract Property build();
    }
}
