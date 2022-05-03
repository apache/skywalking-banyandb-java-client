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

import lombok.Getter;
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.v1.client.metadata.Measure;

import java.util.HashMap;
import java.util.Map;

/**
 * RowEntity represents an entity of BanyanDB entity.
 */
@Getter
public class DataPoint extends RowEntity {
    private final Map<String, Object> fields;

    public static DataPoint create(BanyandbMeasure.DataPoint dataPoint) {
        final DataPoint dp = new DataPoint(dataPoint);
        dp.id = dp.getTagValue(Measure.ID);
        return dp;
    }

    private DataPoint(BanyandbMeasure.DataPoint dataPoint) {
        super(dataPoint.getTimestamp(), dataPoint.getTagFamiliesList());
        this.fields = new HashMap<>(dataPoint.getFieldsCount());
        for (BanyandbMeasure.DataPoint.Field f : dataPoint.getFieldsList()) {
            this.fields.put(f.getName(), convertToJavaType(f.getValue()));
        }
    }

    public <T> T getFieldValue(String fieldName) {
        return (T) this.fields.get(fieldName);
    }

    private Object convertToJavaType(BanyandbModel.FieldValue fieldValue) {
        switch (fieldValue.getValueCase()) {
            case INT:
                return fieldValue.getInt().getValue();
            case STR:
                return fieldValue.getStr().getValue();
            case NULL:
                return null;
            case BINARY_DATA:
                return fieldValue.getBinaryData().toByteArray();
            default:
                throw new IllegalStateException("illegal type of FieldValue");
        }
    }
}
