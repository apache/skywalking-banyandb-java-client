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

import java.util.List;

import lombok.EqualsAndHashCode;
import org.apache.skywalking.banyandb.v1.Banyandb;

/**
 * FieldAndValue represents a value of column in the response
 */
@EqualsAndHashCode(callSuper = true)
public abstract class FieldAndValue<T> extends Tag<T> {
    protected final String fieldName;

    protected FieldAndValue(String fieldName, T value) {
        super(value);
        this.fieldName = fieldName;
    }

    /**
     * @return field name
     */
    public String getFieldName() {
        return this.fieldName;
    }

    /**
     * @return true if value is null;
     */
    public boolean isNull() {
        return this.value == null;
    }

    static FieldAndValue<?> build(Banyandb.Tag tag) {
        switch (tag.getValue().getValueCase()) {
            case INT:
                return new LongFieldPair(tag.getKey(), tag.getValue().getInt().getValue());
            case STR:
                return new StringFieldPair(tag.getKey(), tag.getValue().getStr().getValue());
            case INT_ARRAY:
                return new LongArrayFieldPair(tag.getKey(), tag.getValue().getIntArray().getValueList());
            case STR_ARRAY:
                return new StringArrayFieldPair(tag.getKey(), tag.getValue().getStrArray().getValueList());
            default:
                throw new IllegalArgumentException("Unrecognized NullType");
        }
    }

    public static class StringFieldPair extends FieldAndValue<String> {
        StringFieldPair(final String fieldName, final String value) {
            super(fieldName, value);
        }
    }

    public static class StringArrayFieldPair extends FieldAndValue<List<String>> {
        StringArrayFieldPair(final String fieldName, final List<String> value) {
            super(fieldName, value);
        }
    }

    public static class LongFieldPair extends FieldAndValue<Long> {
        LongFieldPair(final String fieldName, final Long value) {
            super(fieldName, value);
        }
    }

    public static class LongArrayFieldPair extends FieldAndValue<List<Long>> {
        LongArrayFieldPair(final String fieldName, final List<Long> value) {
            super(fieldName, value);
        }
    }
}
