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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;

import java.util.ArrayList;
import java.util.List;

@AutoValue
public abstract class TagFamilySpec implements Serializable<BanyandbDatabase.TagFamilySpec> {
    /**
     * name of the tag family
     */
    public abstract String tagFamilyName();

    /**
     * TagSpec(s) contained in this family
     */
    public abstract ImmutableList<TagSpec> tagSpecs();

    public static TagFamilySpec.Builder create(String tagFamilyName) {
        return new AutoValue_TagFamilySpec.Builder().setTagFamilyName(tagFamilyName);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        abstract Builder setTagFamilyName(String tagFamilyName);

        abstract ImmutableList.Builder<TagSpec> tagSpecsBuilder();

        public final Builder addTagSpec(TagSpec tagSpec) {
            tagSpecsBuilder().add(tagSpec);
            return this;
        }

        public final Builder addTagSpecs(Iterable<TagSpec> tagSpecs) {
            tagSpecsBuilder().addAll(tagSpecs);
            return this;
        }

        public abstract TagFamilySpec build();
    }

    public BanyandbDatabase.TagFamilySpec serialize() {
        List<BanyandbDatabase.TagSpec> metadataTagSpecs = new ArrayList<>(this.tagSpecs().size());
        for (final TagSpec spec : this.tagSpecs()) {
            metadataTagSpecs.add(spec.serialize());
        }
        return BanyandbDatabase.TagFamilySpec.newBuilder()
                .setName(tagFamilyName())
                .addAllTags(metadataTagSpecs)
                .build();
    }

    public static TagFamilySpec fromProtobuf(BanyandbDatabase.TagFamilySpec pb) {
        final TagFamilySpec.Builder builder = TagFamilySpec.create(pb.getName());
        for (int j = 0; j < pb.getTagsCount(); j++) {
            final BanyandbDatabase.TagSpec ts = pb.getTags(j);
            final String tagName = ts.getName();
            switch (ts.getType()) {
                case TAG_TYPE_INT:
                    builder.addTagSpec(TagFamilySpec.TagSpec.newIntTag(tagName));
                    break;
                case TAG_TYPE_STRING:
                    builder.addTagSpec(TagFamilySpec.TagSpec.newStringTag(tagName));
                    break;
                case TAG_TYPE_INT_ARRAY:
                    builder.addTagSpec(TagFamilySpec.TagSpec.newIntArrayTag(tagName));
                    break;
                case TAG_TYPE_STRING_ARRAY:
                    builder.addTagSpec(TagFamilySpec.TagSpec.newStringArrayTag(tagName));
                    break;
                case TAG_TYPE_DATA_BINARY:
                    builder.addTagSpec(TagFamilySpec.TagSpec.newBinaryTag(tagName));
                    break;
                case TAG_TYPE_ID:
                    builder.addTagSpec(TagFamilySpec.TagSpec.newIDTag(tagName));
                    break;
                default:
                    throw new IllegalStateException("unrecognized tag type");
            }
        }

        return builder.build();
    }

    @Getter
    @EqualsAndHashCode
    public static class TagSpec implements Serializable<BanyandbDatabase.TagSpec> {
        /**
         * name of the tag
         */
        private final String tagName;
        /**
         * type of the tag
         */
        private final TagType tagType;

        /**
         * indexedOnly of the tag
         */
        private boolean indexedOnly;

        private TagSpec(String tagName, TagType tagType) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(tagName), "tagName must not be null or empty");
            this.tagName = tagName;
            this.tagType = tagType;
        }

        /**
         * create an int tag spec
         *
         * @param name the name of the tag
         * @return an int tag spec
         */
        public static TagSpec newIntTag(final String name) {
            return new TagSpec(name, TagType.INT);
        }

        /**
         * create a string tag spec
         *
         * @param name the name of the tag
         * @return a string tag spec
         */
        public static TagSpec newStringTag(final String name) {
            return new TagSpec(name, TagType.STRING);
        }

        /**
         * create an int array tag spec
         *
         * @param name the name of the tag
         * @return an int array tag spec
         */
        public static TagSpec newIntArrayTag(final String name) {
            return new TagSpec(name, TagType.INT_ARRAY);
        }

        /**
         * create a string array tag spec
         *
         * @param name the name of the tag
         * @return a string array tag spec
         */
        public static TagSpec newStringArrayTag(final String name) {
            return new TagSpec(name, TagType.STRING_ARRAY);
        }

        /**
         * create a binary tag spec
         *
         * @param name the name of the tag
         * @return a binary array tag spec
         */
        public static TagSpec newBinaryTag(final String name) {
            return new TagSpec(name, TagType.BINARY);
        }

        /**
         * create a ID tag spec
         *
         * @param name the name of the tag
         * @return a binary array tag spec
         */
        private static TagSpec newIDTag(final String name) {
            return new TagSpec(name, TagType.ID);
        }

        /**
         * Set the tag to indexed_only
         */
        public TagSpec indexedOnly() {
            indexedOnly = true;
            return this;
        }

        @Override
        public BanyandbDatabase.TagSpec serialize() {
            return BanyandbDatabase.TagSpec.newBuilder()
                    .setName(this.tagName)
                    .setType(this.tagType.getTagType())
                    .setIndexedOnly(this.indexedOnly)
                    .build();
        }

        @RequiredArgsConstructor
        public enum TagType {
            INT(BanyandbDatabase.TagType.TAG_TYPE_INT),
            STRING(BanyandbDatabase.TagType.TAG_TYPE_STRING),
            INT_ARRAY(BanyandbDatabase.TagType.TAG_TYPE_INT_ARRAY),
            STRING_ARRAY(BanyandbDatabase.TagType.TAG_TYPE_STRING_ARRAY),
            BINARY(BanyandbDatabase.TagType.TAG_TYPE_DATA_BINARY),
            ID(BanyandbDatabase.TagType.TAG_TYPE_ID);

            @Getter(AccessLevel.PRIVATE)
            private final BanyandbDatabase.TagType tagType;
        }
    }
}
