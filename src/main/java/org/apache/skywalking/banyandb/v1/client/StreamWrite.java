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
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.metadata.Serializable;

import java.util.Deque;
import java.util.LinkedList;

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

    public StreamWrite(final String group, final String name, final String elementId, long timestamp) {
        super(group, name, timestamp);
        this.elementId = elementId;
    }

    @Override
    public StreamWrite tag(String tagName, Serializable<BanyandbModel.TagValue> tagValue) throws BanyanDBException {
        return (StreamWrite) super.tag(tagName, tagValue);
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
        // memorize the last offset for the last tag family
        int lastFamilyOffset = 0;
        for (final int tagsPerFamily : this.entityMetadata.getTagFamilyCapacity()) {
            boolean firstNonNullTagFound = false;
            Deque<BanyandbModel.TagValue> tags = new LinkedList<>();
            for (int j = tagsPerFamily - 1; j >= 0; j--) {
                Object obj = this.tags[lastFamilyOffset + j];
                if (obj == null) {
                    if (firstNonNullTagFound) {
                        tags.addFirst(TagAndValue.nullTagValue().serialize());
                    }
                    continue;
                }
                firstNonNullTagFound = true;
                tags.addFirst(((Serializable<BanyandbModel.TagValue>) obj).serialize());
            }
            lastFamilyOffset += tagsPerFamily;
            elemValBuilder.addTagFamilies(BanyandbModel.TagFamilyForWrite.newBuilder().addAllTags(tags).build());
        }
        builder.setElement(elemValBuilder);
        return builder.build();
    }
}
