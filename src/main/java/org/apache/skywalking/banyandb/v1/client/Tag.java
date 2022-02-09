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

import com.google.protobuf.ByteString;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;

import static com.google.protobuf.NullValue.NULL_VALUE;

/**
 * Field represents a value of column/field in the write-op or response.
 */
@EqualsAndHashCode
public abstract class Tag<T> {
    @Getter
    protected final T value;

    protected Tag(T value) {
        this.value = value;
    }

    /**
     * NullField is a value which can be converted to {@link com.google.protobuf.NullValue}.
     * Users should use the singleton instead of create a new instance everytime.
     */
    public static class NullField extends Tag<Object> implements SerializableTag<BanyandbModel.TagValue> {
        private static final NullField INSTANCE = new NullField();

        private NullField() {
            super(null);
        }

        @Override
        public BanyandbModel.TagValue toTag() {
            return BanyandbModel.TagValue.newBuilder().setNull(NULL_VALUE).build();
        }
    }

    /**
     * The value of a String type field.
     */
    public static class StringField extends Tag<String> implements SerializableTag<BanyandbModel.TagValue> {
        private StringField(String value) {
            super(value);
        }

        @Override
        public BanyandbModel.TagValue toTag() {
            return BanyandbModel.TagValue.newBuilder().setStr(BanyandbModel.Str.newBuilder().setValue(value)).build();
        }
    }

    /**
     * The value of a String array type field.
     */
    public static class StringArrayField extends Tag<List<String>> implements SerializableTag<BanyandbModel.TagValue> {
        private StringArrayField(List<String> value) {
            super(value);
        }

        @Override
        public BanyandbModel.TagValue toTag() {
            return BanyandbModel.TagValue.newBuilder().setStrArray(BanyandbModel.StrArray.newBuilder().addAllValue(value)).build();
        }
    }

    /**
     * The value of an int64(Long) type field.
     */
    public static class LongField extends Tag<Long> implements SerializableTag<BanyandbModel.TagValue> {
        private LongField(Long value) {
            super(value);
        }

        @Override
        public BanyandbModel.TagValue toTag() {
            return BanyandbModel.TagValue.newBuilder().setInt(BanyandbModel.Int.newBuilder().setValue(value)).build();
        }
    }

    /**
     * The value of an int64(Long) array type field.
     */
    public static class LongArrayField extends Tag<List<Long>> implements SerializableTag<BanyandbModel.TagValue> {
        private LongArrayField(List<Long> value) {
            super(value);
        }

        @Override
        public BanyandbModel.TagValue toTag() {
            return BanyandbModel.TagValue.newBuilder().setIntArray(BanyandbModel.IntArray.newBuilder().addAllValue(value)).build();
        }
    }

    /**
     * The value of a byte array(ByteString) type field.
     */
    public static class BinaryField extends Tag<ByteString> implements SerializableTag<BanyandbModel.TagValue> {
        public BinaryField(ByteString byteString) {
            super(byteString);
        }

        @Override
        public BanyandbModel.TagValue toTag() {
            return BanyandbModel.TagValue.newBuilder().setBinaryData(value).build();
        }
    }

    /**
     * Construct a string field
     *
     * @param val payload
     * @return Anonymous field with String payload
     */
    public static SerializableTag<BanyandbModel.TagValue> stringField(String val) {
        if (val == null) {
            return nullField();
        }
        return new StringField(val);
    }

    /**
     * Construct a numeric field
     *
     * @param val payload
     * @return Anonymous field with numeric payload
     */
    public static SerializableTag<BanyandbModel.TagValue> longField(long val) {
        return new LongField(val);
    }

    /**
     * Construct a string array field
     *
     * @param val payload
     * @return Anonymous field with string array payload
     */
    public static SerializableTag<BanyandbModel.TagValue> stringArrayField(List<String> val) {
        return new StringArrayField(val);
    }

    /**
     * Construct a byte array field.
     *
     * @param bytes binary data
     * @return Anonymous field with binary payload
     */
    public static SerializableTag<BanyandbModel.TagValue> binaryField(byte[] bytes) {
        return new BinaryField(ByteString.copyFrom(bytes));
    }

    /**
     * Construct a long array field
     *
     * @param val payload
     * @return Anonymous field with numeric array payload
     */
    public static SerializableTag<BanyandbModel.TagValue> longArrayField(List<Long> val) {
        return new LongArrayField(val);
    }

    public static SerializableTag<BanyandbModel.TagValue> nullField() {
        return NullField.INSTANCE;
    }
}
