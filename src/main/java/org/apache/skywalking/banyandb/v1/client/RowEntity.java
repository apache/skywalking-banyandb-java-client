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

package org.apache.skywalking.banyandb.v1.client;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Getter;
import org.apache.skywalking.banyandb.v1.Banyandb;
import org.apache.skywalking.banyandb.v1.stream.BanyandbStream;

/**
 * RowEntity represents an entity of BanyanDB entity.
 */
@Getter
public class RowEntity {
    /**
     * identity of the entity.
     * For a trace entity, it is the spanID of a Span or the segmentId of a segment in Skywalking.
     */
    private final String id;

    /**
     * timestamp of the entity in the timeunit of milliseconds.
     */
    private final long timestamp;

    /**
     * fields are indexed-fields that are searchable in BanyanBD
     */
    private final List<List<TagAndValue<?>>> tags;

    RowEntity(BanyandbStream.Element element) {
        id = element.getElementId();
        timestamp = element.getTimestamp().getSeconds() * 1000 + element.getTimestamp().getNanos() / 1_000_000;
        final int tagFamilyCount = element.getTagFamiliesCount();
        this.tags = new ArrayList<>(tagFamilyCount);
        for (int i = 0; i < tagFamilyCount; i++) {
            Banyandb.TagFamily tagFamily = element.getTagFamilies(i);
            List<TagAndValue<?>> tagAndValuesInTagFamily = tagFamily.getTagsList().stream()
                    .map((Function<Banyandb.Tag, TagAndValue<?>>) tag -> TagAndValue.build(tagFamily.getName(), tag))
                    .collect(Collectors.toList());
            this.tags.set(i, tagAndValuesInTagFamily);
        }
    }
}
