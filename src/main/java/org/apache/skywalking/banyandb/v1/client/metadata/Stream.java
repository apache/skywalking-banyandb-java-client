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
import org.apache.skywalking.banyandb.database.v1.metadata.BanyandbMetadata;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@EqualsAndHashCode(callSuper = true)
public class Stream extends Schema<BanyandbMetadata.Stream> {
    /**
     * specs of tag families
     */
    private List<TagFamilySpec> tagFamilySpecs;

    /**
     * tag names used to generate an entity
     */
    private List<String> entityTagNames;

    /**
     * number of shards
     */
    private int shardNum;

    /**
     * duration determines how long a Stream keeps its data
     */
    private Duration duration;

    public Stream(String name, int shardNum, Duration duration) {
        this(name, shardNum, duration, null);
    }

    private Stream(String name, int shardNum, Duration duration, ZonedDateTime updatedAt) {
        super(name, updatedAt);
        this.tagFamilySpecs = new ArrayList<>(2);
        this.entityTagNames = new ArrayList<>();
        this.shardNum = shardNum;
        this.duration = duration;
    }

    /**
     * Add a tag name as a part of the entity
     *
     * @param name the name of the tag
     */
    public Stream addTagNameAsEntity(String name) {
        this.entityTagNames.add(name);
        return this;
    }

    /**
     * Add a tag family spec to the schema
     *
     * @param tagFamilySpec a tag family containing tag specs
     */
    public Stream addTagFamilySpec(TagFamilySpec tagFamilySpec) {
        this.tagFamilySpecs.add(tagFamilySpec);
        return this;
    }

    @Override
    public BanyandbMetadata.Stream serialize(String group) {
        List<BanyandbMetadata.TagFamilySpec> metadataTagFamilySpecs = new ArrayList<>(this.tagFamilySpecs.size());
        for (final TagFamilySpec spec : this.tagFamilySpecs) {
            metadataTagFamilySpecs.add(spec.serialize());
        }

        BanyandbMetadata.Stream.Builder b = BanyandbMetadata.Stream.newBuilder()
                .setMetadata(buildMetadata(group))
                .addAllTagFamilies(metadataTagFamilySpecs)
                .setEntity(BanyandbMetadata.Entity.newBuilder().addAllTagNames(entityTagNames).build())
                .setShardNum(this.shardNum)
                .setDuration(this.duration.serialize());

        if (this.updatedAt != null) {
            b.setUpdatedAt(TimeUtils.buildTimestamp(this.updatedAt));
        }
        return b.build();
    }

    public static Stream fromProtobuf(final BanyandbMetadata.Stream stream) {
        Stream s = new Stream(stream.getMetadata().getName(), stream.getShardNum(),
                Duration.fromProtobuf(stream.getDuration()), TimeUtils.parseTimestamp(stream.getUpdatedAt()));
        // prepare entity
        for (int i = 0; i < stream.getEntity().getTagNamesCount(); i++) {
            s.addTagNameAsEntity(stream.getEntity().getTagNames(i));
        }
        // build tag family spec
        for (int i = 0; i < stream.getTagFamiliesCount(); i++) {
            final BanyandbMetadata.TagFamilySpec tfs = stream.getTagFamilies(i);
            final TagFamilySpec tagFamilySpec = new TagFamilySpec(tfs.getName());
            for (int j = 0; j < tfs.getTagsCount(); j++) {
                final BanyandbMetadata.TagSpec ts = tfs.getTags(j);
                final String tagName = ts.getName();
                switch (ts.getType()) {
                    case TAG_TYPE_INT:
                        tagFamilySpec.addTagSpec(TagFamilySpec.TagSpec.newIntTag(tagName));
                        break;
                    case TAG_TYPE_STRING:
                        tagFamilySpec.addTagSpec(TagFamilySpec.TagSpec.newStringTag(tagName));
                        break;
                    case TAG_TYPE_INT_ARRAY:
                        tagFamilySpec.addTagSpec(TagFamilySpec.TagSpec.newIntArrayTag(tagName));
                        break;
                    case TAG_TYPE_STRING_ARRAY:
                        tagFamilySpec.addTagSpec(TagFamilySpec.TagSpec.newStringArrayTag(tagName));
                        break;
                    case TAG_TYPE_DATA_BINARY:
                        tagFamilySpec.addTagSpec(TagFamilySpec.TagSpec.newBinaryTag(tagName));
                        break;
                    default:
                        throw new IllegalStateException("unrecognized tag type");
                }
            }
            s.addTagFamilySpec(tagFamilySpec);
        }
        return s;
    }
}
