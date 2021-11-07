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
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.banyandb.database.v1.metadata.BanyandbMetadata;

import java.util.ArrayList;
import java.util.List;

@Getter
@EqualsAndHashCode
public class TagFamilySpec implements Serializable<BanyandbMetadata.TagFamilySpec> {
    private final String tagFamilyName;

    private final List<TagSpec> tagSpecs;

    public TagFamilySpec(String tagFamilyName) {
        this(tagFamilyName, new ArrayList<>(5));
    }

    public TagFamilySpec(String tagFamilyName, List<TagSpec> specs) {
        Preconditions.checkArgument(specs != null, "spec must not be null");
        this.tagFamilyName = tagFamilyName;
        this.tagSpecs = specs;
    }

    public TagFamilySpec addTagSpec(TagSpec tagSpec) {
        this.tagSpecs.add(tagSpec);
        return this;
    }

    public BanyandbMetadata.TagFamilySpec serialize() {
        List<BanyandbMetadata.TagSpec> metadataTagSpecs = new ArrayList<>(this.tagSpecs.size());
        for (final TagSpec spec : this.tagSpecs) {
            metadataTagSpecs.add(spec.serialize());
        }
        return BanyandbMetadata.TagFamilySpec.newBuilder()
                .setName(this.tagFamilyName)
                .addAllTags(metadataTagSpecs)
                .build();
    }

    @Getter
    @EqualsAndHashCode
    public static class TagSpec implements Serializable<BanyandbMetadata.TagSpec> {
        private final String tagName;
        private final TagType tagType;

        private TagSpec(String tagName, TagType tagType) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(tagName), "tagName must not be null or empty");
            this.tagName = tagName;
            this.tagType = tagType;
        }

        public static TagSpec newIntTag(final String name) {
            return new TagSpec(name, TagType.INT);
        }

        public static TagSpec newStringTag(final String name) {
            return new TagSpec(name, TagType.STRING);
        }

        public static TagSpec newIntArrayTag(final String name) {
            return new TagSpec(name, TagType.INT_ARRAY);
        }

        public static TagSpec newStringArrayTag(final String name) {
            return new TagSpec(name, TagType.STRING_ARRAY);
        }

        public static TagSpec newBinaryTag(final String name) {
            return new TagSpec(name, TagType.BINARY);
        }

        @Override
        public BanyandbMetadata.TagSpec serialize() {
            return BanyandbMetadata.TagSpec.newBuilder()
                    .setName(this.tagName)
                    .setType(this.tagType.getTagType())
                    .build();
        }

        @RequiredArgsConstructor
        public enum TagType {
            INT(BanyandbMetadata.TagType.TAG_TYPE_INT),
            STRING(BanyandbMetadata.TagType.TAG_TYPE_STRING),
            INT_ARRAY(BanyandbMetadata.TagType.TAG_TYPE_INT_ARRAY),
            STRING_ARRAY(BanyandbMetadata.TagType.TAG_TYPE_STRING_ARRAY),
            BINARY(BanyandbMetadata.TagType.TAG_TYPE_DATA_BINARY);

            @Getter(AccessLevel.PRIVATE)
            private final BanyandbMetadata.TagType tagType;
        }
    }
}
