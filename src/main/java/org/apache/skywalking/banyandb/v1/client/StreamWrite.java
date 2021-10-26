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

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

import java.util.List;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import org.apache.skywalking.banyandb.v1.Banyandb;
import org.apache.skywalking.banyandb.v1.stream.BanyandbStream;

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
     * The binary raw data represents the whole object of current stream. It could be organized by
     * different serialization formats. Natively, SkyWalking uses protobuf, but it is not required. The BanyanDB server
     * wouldn't deserialize this. So, no format requirement.
     */
    private final byte[] binary;
    /**
     * The values of fields, which are defined by the schema. In the bulk write process, BanyanDB client doesn't require
     * field names anymore.
     */
    @Singular
    private final List<SerializableTag<Banyandb.TagValue>> tags;

    /**
     * @param group of the BanyanDB client connected.
     * @return {@link BanyandbStream.WriteRequest} for the bulk process.
     */
    BanyandbStream.WriteRequest build(String group) {
        final BanyandbStream.WriteRequest.Builder builder = BanyandbStream.WriteRequest.newBuilder();
        builder.setMetadata(Banyandb.Metadata.newBuilder().setGroup(group).setName(name).build());
        final BanyandbStream.ElementValue.Builder elemValBuilder = BanyandbStream.ElementValue.newBuilder();
        elemValBuilder.setElementId(elementId);
        elemValBuilder.setTimestamp(Timestamp.newBuilder()
                .setSeconds(timestamp / 1000)
                .setNanos((int) (timestamp % 1000 * 1_000_000)));
        // 1 - add "data" tags
        elemValBuilder.addTagFamilies(Banyandb.TagFamilyForWrite.newBuilder().addTags(
                Banyandb.TagValue.newBuilder()
                        .setBinaryData(ByteString.copyFrom(this.binary))
                        .build()
        ).build());
        // 2 - add "searchable" tags
        Banyandb.TagFamilyForWrite.Builder b = Banyandb.TagFamilyForWrite.newBuilder();
        for (final SerializableTag<Banyandb.TagValue> tag : tags) {
            b.addTags(tag.toTag());
        }
        elemValBuilder.addTagFamilies(b);
        builder.setElement(elemValBuilder);
        return builder.build();
    }
}
