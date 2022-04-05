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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.banyandb.v1.client.util.CopyOnWriteMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum MetadataCache {
    INSTANCE;

    private final Map<String, EntityMetadata> cache;

    MetadataCache() {
        this.cache = new CopyOnWriteMap<>();
    }

    public Stream register(Stream stream) {
        this.cache.put(formatKey(stream.group(), stream.name()), parse(stream));
        return stream;
    }

    public Measure register(Measure measure) {
        this.cache.put(formatKey(measure.group(), measure.name()), parse(measure));
        return measure;
    }

    public EntityMetadata findMetadata(String group, String name) {
        return this.cache.get(formatKey(group, name));
    }

    static String formatKey(String group, String name) {
        return group + ":" + name;
    }

    static EntityMetadata parse(Stream s) {
        int totalTags = 0;
        final int[] tagFamilyCapacity = new int[s.tagFamilies().size()];
        Map<String, TagInfo> tagInfo = new HashMap<>();
        int k = 0;
        for (int i = 0; i < s.tagFamilies().size(); i++) {
            final String tagFamilyName = s.tagFamilies().get(i).tagFamilyName();
            tagFamilyCapacity[i] = s.tagFamilies().get(i).tagSpecs().size();
            totalTags += tagFamilyCapacity[i];
            for (int j = 0; j < tagFamilyCapacity[i]; j++) {
                tagInfo.put(s.tagFamilies().get(i).tagSpecs().get(j).getTagName(), new TagInfo(tagFamilyName, k++));
            }
        }
        return new EntityMetadata(totalTags, 0, tagFamilyCapacity,
                Collections.unmodifiableMap(tagInfo),
                Collections.emptyMap());
    }

    static EntityMetadata parse(Measure m) {
        int totalTags = 0;
        final int[] tagFamilyCapacity = new int[m.tagFamilies().size()];
        final Map<String, TagInfo> tagOffset = new HashMap<>();
        int k = 0;
        for (int i = 0; i < m.tagFamilies().size(); i++) {
            final String tagFamilyName = m.tagFamilies().get(i).tagFamilyName();
            tagFamilyCapacity[i] = m.tagFamilies().get(i).tagSpecs().size();
            totalTags += tagFamilyCapacity[i];
            for (int j = 0; j < tagFamilyCapacity[i]; j++) {
                tagOffset.put(m.tagFamilies().get(i).tagSpecs().get(j).getTagName(), new TagInfo(tagFamilyName, k++));
            }
        }
        final Map<String, Integer> fieldOffset = new HashMap<>();
        for (int i = 0; i < m.fields().size(); i++) {
            fieldOffset.put(m.fields().get(i).getName(), i);
        }
        return new EntityMetadata(totalTags, m.fields().size(), tagFamilyCapacity,
                Collections.unmodifiableMap(tagOffset), Collections.unmodifiableMap(fieldOffset));
    }

    @Getter
    @RequiredArgsConstructor
    public static class EntityMetadata {
        private final int totalTags;

        private final int totalFields;

        private final int[] tagFamilyCapacity;

        private final Map<String, TagInfo> tagOffset;

        private final Map<String, Integer> fieldOffset;

        public Optional<TagInfo> findTagInfo(String name) {
            return Optional.ofNullable(this.tagOffset.get(name));
        }

        public int findFieldInfo(String name) {
            return this.fieldOffset.get(name);
        }
    }

    @RequiredArgsConstructor
    @Getter
    public static class TagInfo {
        private final String tagFamilyName;
        private final int offset;
    }
}
