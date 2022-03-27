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

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.v1.client.metadata.Serializable;

import static com.google.protobuf.NullValue.NULL_VALUE;

/**
 * Field represents a value in the write-op or response.
 */
@EqualsAndHashCode
public abstract class Value<T> {
    @Getter
    protected final T value;

    protected Value(T value) {
        this.value = value;
    }

    /**
     * NullTagValue is a value which can be converted to {@link com.google.protobuf.NullValue}.
     * Users should use the singleton instead of create a new instance everytime.
     */
    public static class NullTagValue extends Value<Object> implements Serializable<BanyandbModel.TagValue> {
        private static final NullTagValue INSTANCE = new NullTagValue();

        private NullTagValue() {
            super(null);
        }

        @Override
        public BanyandbModel.TagValue serialize() {
            return BanyandbModel.TagValue.newBuilder().setNull(NULL_VALUE).build();
        }
    }

    /**
     * The value of a String type tag.
     */
    public static class StringTagValue extends Value<String> implements Serializable<BanyandbModel.TagValue> {
        private StringTagValue(String value) {
            super(value);
        }

        @Override
        public BanyandbModel.TagValue serialize() {
            return BanyandbModel.TagValue.newBuilder().setStr(BanyandbModel.Str.newBuilder().setValue(value)).build();
        }
    }

    /**
     * The value of a String array type tag.
     */
    public static class StringArrayTagValue extends Value<List<String>> implements Serializable<BanyandbModel.TagValue> {
        private StringArrayTagValue(List<String> value) {
            super(value);
        }

        @Override
        public BanyandbModel.TagValue serialize() {
            return BanyandbModel.TagValue.newBuilder().setStrArray(BanyandbModel.StrArray.newBuilder().addAllValue(value)).build();
        }
    }

    public static class IDTagValue extends Value<String> implements Serializable<BanyandbModel.TagValue> {
        private IDTagValue(String value) {
            super(value);
        }

        @Override
        public BanyandbModel.TagValue serialize() {
            return BanyandbModel.TagValue.newBuilder().setId(BanyandbModel.ID.newBuilder().setValue(value)).build();
        }
    }

    /**
     * The value of an int64(Long) type tag.
     */
    public static class LongTagValue extends Value<Long> implements Serializable<BanyandbModel.TagValue> {
        private LongTagValue(Long value) {
            super(value);
        }

        @Override
        public BanyandbModel.TagValue serialize() {
            return BanyandbModel.TagValue.newBuilder().setInt(BanyandbModel.Int.newBuilder().setValue(value)).build();
        }
    }

    /**
     * The value of an int64(Long) array type tag.
     */
    public static class LongArrayTagValue extends Value<List<Long>> implements Serializable<BanyandbModel.TagValue> {
        private LongArrayTagValue(List<Long> value) {
            super(value);
        }

        @Override
        public BanyandbModel.TagValue serialize() {
            return BanyandbModel.TagValue.newBuilder().setIntArray(BanyandbModel.IntArray.newBuilder().addAllValue(value)).build();
        }
    }

    /**
     * The value of a byte array(ByteString) type tag.
     */
    public static class BinaryTagValue extends Value<ByteString> implements Serializable<BanyandbModel.TagValue> {
        public BinaryTagValue(ByteString byteString) {
            super(byteString);
        }

        @Override
        public BanyandbModel.TagValue serialize() {
            return BanyandbModel.TagValue.newBuilder().setBinaryData(value).build();
        }
    }

    /**
     * Construct a string tag
     *
     * @param val payload
     * @return Anonymous tag with String payload
     */
    public static Serializable<BanyandbModel.TagValue> stringTagValue(String val) {
        if (val == null) {
            return nullTagValue();
        }
        return new StringTagValue(val);
    }

    public static Serializable<BanyandbModel.TagValue> idTagValue(String val) {
        if (Strings.isNullOrEmpty(val)) {
            throw new NullPointerException();
        }
        return new IDTagValue(val);
    }

    /**
     * Construct a numeric tag
     *
     * @param val payload
     * @return Anonymous tag with numeric payload
     */
    public static Serializable<BanyandbModel.TagValue> longTagValue(long val) {
        return new LongTagValue(val);
    }

    /**
     * Construct a string array tag
     *
     * @param val payload
     * @return Anonymous tag with string array payload
     */
    public static Serializable<BanyandbModel.TagValue> stringArrayTagValue(List<String> val) {
        return new StringArrayTagValue(val);
    }

    /**
     * Construct a byte array tag.
     *
     * @param bytes binary data
     * @return Anonymous tag with binary payload
     */
    public static Serializable<BanyandbModel.TagValue> binaryTagValue(byte[] bytes) {
        return new BinaryTagValue(ByteString.copyFrom(bytes));
    }

    /**
     * Construct a long array tag
     *
     * @param val payload
     * @return Anonymous tag with numeric array payload
     */
    public static Serializable<BanyandbModel.TagValue> longArrayTag(List<Long> val) {
        return new LongArrayTagValue(val);
    }

    public static Serializable<BanyandbModel.TagValue> nullTagValue() {
        return NullTagValue.INSTANCE;
    }

    /**
     * The value of a String type field.
     */
    public static class StringFieldValue extends Value<String> implements Serializable<BanyandbModel.FieldValue> {
        private StringFieldValue(String value) {
            super(value);
        }

        @Override
        public BanyandbModel.FieldValue serialize() {
            return BanyandbModel.FieldValue.newBuilder().setStr(BanyandbModel.Str.newBuilder().setValue(value)).build();
        }
    }

    public static Serializable<BanyandbModel.FieldValue> stringFieldValue(String val) {
        if (Strings.isNullOrEmpty(val)) {
            return nullFieldValue();
        }
        return new StringFieldValue(val);
    }

    /**
     * NullFieldValue is a value which can be converted to {@link com.google.protobuf.NullValue}.
     * Users should use the singleton instead of create a new instance everytime.
     */
    public static class NullFieldValue extends Value<Object> implements Serializable<BanyandbModel.FieldValue> {
        private static final NullFieldValue INSTANCE = new NullFieldValue();

        private NullFieldValue() {
            super(null);
        }

        @Override
        public BanyandbModel.FieldValue serialize() {
            return BanyandbModel.FieldValue.newBuilder().setNull(NULL_VALUE).build();
        }
    }

    public static Serializable<BanyandbModel.FieldValue> nullFieldValue() {
        return NullFieldValue.INSTANCE;
    }

    /**
     * The value of an int64(Long) type field.
     */
    public static class LongFieldValue extends Value<Long> implements Serializable<BanyandbModel.FieldValue> {
        private LongFieldValue(Long value) {
            super(value);
        }

        @Override
        public BanyandbModel.FieldValue serialize() {
            return BanyandbModel.FieldValue.newBuilder().setInt(BanyandbModel.Int.newBuilder().setValue(value)).build();
        }
    }

    /**
     * Construct a numeric tag
     *
     * @param val payload
     * @return Anonymous tag with numeric payload
     */
    public static Serializable<BanyandbModel.FieldValue> longFieldValue(long val) {
        return new LongFieldValue(val);
    }

    /**
     * The value of a byte array(ByteString) type field.
     */
    public static class BinaryFieldValue extends Value<ByteString> implements Serializable<BanyandbModel.FieldValue> {
        public BinaryFieldValue(ByteString byteString) {
            super(byteString);
        }

        @Override
        public BanyandbModel.FieldValue serialize() {
            return BanyandbModel.FieldValue.newBuilder().setBinaryData(value).build();
        }
    }

    /**
     * Construct a byte array tag.
     *
     * @param bytes binary data
     * @return Anonymous tag with binary payload
     */
    public static Serializable<BanyandbModel.FieldValue> binaryFieldValue(byte[] bytes) {
        return new BinaryFieldValue(ByteString.copyFrom(bytes));
    }
}
