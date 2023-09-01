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

import java.util.Map;
import java.util.Optional;

import lombok.Getter;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.InvalidReferenceException;
import org.apache.skywalking.banyandb.v1.client.metadata.MetadataCache;
import org.apache.skywalking.banyandb.v1.client.metadata.Serializable;

public abstract class AbstractWrite<P extends com.google.protobuf.GeneratedMessageV3> {
    /**
     * Timestamp represents the time of current stream
     * in the timeunit of milliseconds.
     */
    @Getter
    protected long timestamp;

    protected final Object[] tags;

    protected final MetadataCache.EntityMetadata entityMetadata;

    public AbstractWrite(MetadataCache.EntityMetadata entityMetadata, long timestamp) {
        if (entityMetadata == null) {
            throw new IllegalArgumentException("metadata not found");
        }
        this.entityMetadata = entityMetadata;
        this.timestamp = timestamp;
        this.tags = new Object[this.entityMetadata.getTotalTags()];
    }

    /**
     * Build a write request without initial timestamp.
     */
    AbstractWrite(MetadataCache.EntityMetadata entityMetadata) {
        this(entityMetadata, 0);
    }

    public AbstractWrite<P> tag(String tagName, Serializable<BanyandbModel.TagValue> tagValue) throws BanyanDBException {
        final Optional<MetadataCache.TagInfo> tagInfo = this.entityMetadata.findTagInfo(tagName);
        if (!tagInfo.isPresent()) {
            throw InvalidReferenceException.fromInvalidTag(tagName);
        }
        this.tags[tagInfo.get().getOffset()] = tagValue;
        return this;
    }

    P build() {
        if (timestamp <= 0) {
            throw new IllegalArgumentException("timestamp is invalid.");
        }

        BanyandbCommon.Metadata metadata = BanyandbCommon.Metadata.newBuilder()
                .setGroup(entityMetadata.getGroup()).setName(entityMetadata.getName()).setModRevision(entityMetadata.getModRevision()).build();
        Timestamp ts = Timestamp.newBuilder()
                .setSeconds(timestamp / 1000)
                .setNanos((int) (timestamp % 1000 * 1_000_000)).build();
        return build(metadata, ts);
    }

    protected abstract P build(BanyandbCommon.Metadata metadata, Timestamp ts);

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("group=").append(entityMetadata.getGroup()).append(", ").append("name=")
                .append(entityMetadata.getName()).append(", ").append("timestamp=").append(timestamp).append(", ");
        for (int i = 0; i < this.tags.length; i++) {
            final int index = i;
            Map<String, MetadataCache.TagInfo> tagMap = this.entityMetadata.getTagOffset();
            Optional<String> tagName = tagMap.keySet().stream().filter(name -> tagMap.get(name).getOffset() == index).findAny();
            if (tagName.isPresent()) {
                stringBuilder.append(tagName.get()).append("=").append(((Serializable<BanyandbModel.TagValue>) this.tags[i]).serialize()).append(", ");
            }
        }
        return stringBuilder.toString();
    }
}
