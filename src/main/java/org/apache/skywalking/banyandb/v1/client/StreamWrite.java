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

import lombok.Getter;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;
import org.apache.skywalking.banyandb.v1.client.metadata.Serializable;

/**
 * StreamWrite represents a write operation, including necessary fields, for {@link
 * BanyanDBClient#buildStreamWriteProcessor}.
 */
public class StreamWrite extends AbstractWrite<BanyandbStream.WriteRequest> {
    /**
     * ID of current entity
     */
    @Getter
    private final String elementId;
    /**
     * The fields represent objects of current stream, and they are not indexed.
     * It could be organized by different serialization formats.
     * For instance, regarding the binary format, SkyWalking may use protobuf, but it is not required.
     * The BanyanDB server wouldn't deserialize binary data. Thus, no specific format requirement.
     */
    private Object[] dataTags;
    /**
     * The values of "searchable" fields, which are defined by the schema.
     * In the bulk write process, BanyanDB client doesn't require field names anymore.
     */
    private Object[] searchableTags;

    public StreamWrite(final String group, final String name, final String elementId, long timestamp, int dataTagCap, int searchableTagLen) {
        super(group, name, timestamp);
        this.elementId = elementId;
        this.dataTags = new Object[dataTagCap];
        this.searchableTags = new Object[searchableTagLen];
    }

    public StreamWrite dataTag(int index, Serializable<BanyandbModel.TagValue> tag) {
        this.dataTags[index] = tag;
        return this;
    }

    public StreamWrite searchableTag(int index, Serializable<BanyandbModel.TagValue> tag) {
        this.searchableTags[index] = tag;
        return this;
    }

    /**
     * Build a write request
     *
     * @return {@link BanyandbStream.WriteRequest} for the bulk process.
     */
    @Override
    protected BanyandbStream.WriteRequest build(BanyandbCommon.Metadata metadata, Timestamp ts) {
        final BanyandbStream.WriteRequest.Builder builder = BanyandbStream.WriteRequest.newBuilder();
        builder.setMetadata(metadata);
        final BanyandbStream.ElementValue.Builder elemValBuilder = BanyandbStream.ElementValue.newBuilder();
        elemValBuilder.setElementId(elementId);
        elemValBuilder.setTimestamp(ts);
        // 1 - add "data" tags
        BanyandbModel.TagFamilyForWrite.Builder dataBuilder = BanyandbModel.TagFamilyForWrite.newBuilder();
        for (final Object dataTag : this.dataTags) {
            dataBuilder.addTags(((Serializable<BanyandbModel.TagValue>) dataTag).serialize());
        }
        elemValBuilder.addTagFamilies(dataBuilder.build());
        // 2 - add "searchable" tags
        BanyandbModel.TagFamilyForWrite.Builder searchableBuilder = BanyandbModel.TagFamilyForWrite.newBuilder();
        for (final Object searchableTag : this.searchableTags) {
            searchableBuilder.addTags(((Serializable<BanyandbModel.TagValue>) searchableTag).serialize());
        }
        elemValBuilder.addTagFamilies(searchableBuilder);
        builder.setElement(elemValBuilder);
        return builder.build();
    }
}
