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

import com.google.protobuf.Timestamp;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;

import java.util.List;

/**
 * StreamWrite represents a write operation, including necessary fields, for {@link
 * BanyanDBClient#buildStreamWriteProcessor}.
 */
@Builder
@Getter(AccessLevel.PROTECTED)
public class StreamWrite {
    /**
     * Owner name current entity
     */
    private final String name;
    /**
     * ID of current entity
     */
    private final String elementId;
    /**
     * Timestamp represents the time of current stream
     * in the timeunit of milliseconds.
     */
    private final long timestamp;
    /**
     * The fields represent objects of current stream, and they are not indexed.
     * It could be organized by different serialization formats.
     * For instance, regarding the binary format, SkyWalking may use protobuf, but it is not required.
     * The BanyanDB server wouldn't deserialize binary data. Thus, no specific format requirement.
     */
    @Singular
    private final List<SerializableTag<BanyandbModel.TagValue>> dataTags;
    /**
     * The values of "searchable" fields, which are defined by the schema.
     * In the bulk write process, BanyanDB client doesn't require field names anymore.
     */
    @Singular
    private final List<SerializableTag<BanyandbModel.TagValue>> searchableTags;

    /**
     * @param group of the BanyanDB client connected.
     * @return {@link BanyandbStream.WriteRequest} for the bulk process.
     */
    BanyandbStream.WriteRequest build(String group) {
        final BanyandbStream.WriteRequest.Builder builder = BanyandbStream.WriteRequest.newBuilder();
        builder.setMetadata(BanyandbCommon.Metadata.newBuilder().setGroup(group).setName(name).build());
        final BanyandbStream.ElementValue.Builder elemValBuilder = BanyandbStream.ElementValue.newBuilder();
        elemValBuilder.setElementId(elementId);
        elemValBuilder.setTimestamp(Timestamp.newBuilder()
                .setSeconds(timestamp / 1000)
                .setNanos((int) (timestamp % 1000 * 1_000_000)));
        // 1 - add "data" tags
        BanyandbModel.TagFamilyForWrite.Builder dataBuilder = BanyandbModel.TagFamilyForWrite.newBuilder();
        for (final SerializableTag<BanyandbModel.TagValue> dataTag : this.dataTags) {
            dataBuilder.addTags(dataTag.toTag());
        }
        elemValBuilder.addTagFamilies(dataBuilder.build());
        // 2 - add "searchable" tags
        BanyandbModel.TagFamilyForWrite.Builder searchableBuilder = BanyandbModel.TagFamilyForWrite.newBuilder();
        for (final SerializableTag<BanyandbModel.TagValue> searchableTag : this.searchableTags) {
            searchableBuilder.addTags(searchableTag.toTag());
        }
        elemValBuilder.addTagFamilies(searchableBuilder);
        builder.setElement(elemValBuilder);
        return builder.build();
    }
}
