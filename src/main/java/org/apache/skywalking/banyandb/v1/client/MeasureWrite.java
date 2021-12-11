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
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.v1.client.metadata.Serializable;

public class MeasureWrite extends AbstractWrite<BanyandbMeasure.WriteRequest> {
    /**
     * The fields represent objects of current stream, and they are not indexed.
     * It could be organized by different serialization formats.
     * For instance, regarding the binary format, SkyWalking may use protobuf, but it is not required.
     * The BanyanDB server wouldn't deserialize binary data. Thus, no specific format requirement.
     */
    private Object[] defaultTags;
    /**
     * The values of "searchable" fields, which are defined by the schema.
     * In the bulk write process, BanyanDB client doesn't require field names anymore.
     */
    private Object[] fields;

    public MeasureWrite(final String group, final String name, long timestamp, int defaultTagsCap, int fieldsCap) {
        super(group, name, timestamp);
        this.defaultTags = new Object[defaultTagsCap];
        this.fields = new Object[fieldsCap];
    }

    public MeasureWrite field(int index, Serializable<BanyandbModel.FieldValue> field) {
        this.fields[index] = field;
        return this;
    }

    public MeasureWrite defaultTag(int index, Serializable<BanyandbModel.TagValue> tag) {
        this.defaultTags[index] = tag;
        return this;
    }

    /**
     * Build a write request
     *
     * @return {@link BanyandbMeasure.WriteRequest} for the bulk process.
     */
    @Override
    protected BanyandbMeasure.WriteRequest build(BanyandbCommon.Metadata metadata, Timestamp ts) {
        final BanyandbMeasure.WriteRequest.Builder builder = BanyandbMeasure.WriteRequest.newBuilder();
        builder.setMetadata(metadata);
        final BanyandbMeasure.DataPointValue.Builder datapointValueBuilder = BanyandbMeasure.DataPointValue.newBuilder();
        datapointValueBuilder.setTimestamp(ts);
        // 1 - add "default" tags
        BanyandbModel.TagFamilyForWrite.Builder defaultBuilder = BanyandbModel.TagFamilyForWrite.newBuilder();
        for (final Object dataTag : this.defaultTags) {
            defaultBuilder.addTags(((Serializable<BanyandbModel.TagValue>) dataTag).serialize());
        }
        datapointValueBuilder.addTagFamilies(defaultBuilder.build());
        // 2 - add fields
        for (final Object field : this.fields) {
            datapointValueBuilder.addFields(((Serializable<BanyandbModel.FieldValue>) field).serialize());
        }

        builder.setDataPoint(datapointValueBuilder);
        return builder.build();
    }
}
