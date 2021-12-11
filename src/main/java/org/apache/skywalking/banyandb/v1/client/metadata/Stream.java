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
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@EqualsAndHashCode(callSuper = true)
public class Stream extends NamedSchema<BanyandbDatabase.Stream> {
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
    private Duration ttl;

    public Stream(String name, int shardNum, Duration ttl) {
        this(name, shardNum, ttl, null);
    }

    private Stream(String name, int shardNum, Duration ttl, ZonedDateTime updatedAt) {
        super(name, updatedAt);
        this.tagFamilySpecs = new ArrayList<>(2);
        this.entityTagNames = new ArrayList<>();
        this.shardNum = shardNum;
        this.ttl = ttl;
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
    public BanyandbDatabase.Stream serialize(String group) {
        List<BanyandbDatabase.TagFamilySpec> metadataTagFamilySpecs = new ArrayList<>(this.tagFamilySpecs.size());
        for (final TagFamilySpec spec : this.tagFamilySpecs) {
            metadataTagFamilySpecs.add(spec.serialize());
        }

        BanyandbDatabase.Stream.Builder b = BanyandbDatabase.Stream.newBuilder()
                .setMetadata(buildMetadata(group))
                .addAllTagFamilies(metadataTagFamilySpecs)
                .setEntity(BanyandbDatabase.Entity.newBuilder().addAllTagNames(entityTagNames).build())
                .setOpts(BanyandbDatabase.ResourceOpts.newBuilder().setShardNum(this.shardNum).setTtl(this.ttl.serialize()));

        if (this.updatedAt != null) {
            b.setUpdatedAtNanoseconds(TimeUtils.buildTimestamp(this.updatedAt));
        }
        return b.build();
    }

    public static Stream fromProtobuf(final BanyandbDatabase.Stream pb) {
        Stream s = new Stream(pb.getMetadata().getName(), pb.getOpts().getShardNum(),
                Duration.fromProtobuf(pb.getOpts().getTtl()), TimeUtils.parseTimestamp(pb.getUpdatedAtNanoseconds()));
        // prepare entity
        for (int i = 0; i < pb.getEntity().getTagNamesCount(); i++) {
            s.addTagNameAsEntity(pb.getEntity().getTagNames(i));
        }
        // build tag family spec
        for (int i = 0; i < pb.getTagFamiliesCount(); i++) {
            s.addTagFamilySpec(TagFamilySpec.fromProtobuf(pb.getTagFamilies(i)));
        }
        return s;
    }
}
