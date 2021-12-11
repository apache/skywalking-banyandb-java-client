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

package org.apache.skywalking.banyandb.v1.client.metadata;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@EqualsAndHashCode(callSuper = true)
public class Measure extends NamedSchema<BanyandbDatabase.Measure> {
    /**
     * specs of tag families
     */
    private List<TagFamilySpec> tagFamilySpecs;

    /**
     * fieldSpecs denote measure values
     */
    private List<FieldSpec> fieldSpecs;

    /**
     * tag names used to generate an entity
     */
    private List<String> entityTagNames;

    /**
     * interval indicates how frequently to send a data point
     */
    private final Duration interval;

    public Measure(String group, String name, Duration interval) {
        this(group, name, interval, null);
    }

    private Measure(String group, String name, Duration interval, ZonedDateTime updatedAt) {
        super(group, name, updatedAt);
        this.tagFamilySpecs = new ArrayList<>();
        this.entityTagNames = new ArrayList<>();
        this.fieldSpecs = new ArrayList<>();
        this.interval = interval;
    }

    /**
     * Add a tag name as a part of the entity
     *
     * @param name the name of the tag
     */
    public Measure addTagNameAsEntity(String name) {
        this.entityTagNames.add(name);
        return this;
    }

    /**
     * Add a tag family spec to the schema
     *
     * @param tagFamilySpec a tag family containing tag specs
     */
    public Measure addTagFamilySpec(TagFamilySpec tagFamilySpec) {
        this.tagFamilySpecs.add(tagFamilySpec);
        return this;
    }

    /**
     * Add a tag family spec to the schema
     *
     * @param fieldSpec a tag family containing tag specs
     */
    public Measure addFieldSpec(FieldSpec fieldSpec) {
        this.fieldSpecs.add(fieldSpec);
        return this;
    }

    static Measure fromProtobuf(BanyandbDatabase.Measure pb) {
        Measure m = new Measure(pb.getMetadata().getGroup(), pb.getMetadata().getName(),
                Duration.parse(pb.getInterval()),
                TimeUtils.parseTimestamp(pb.getUpdatedAt()));

        // prepare entity
        for (int i = 0; i < pb.getEntity().getTagNamesCount(); i++) {
            m.addTagNameAsEntity(pb.getEntity().getTagNames(i));
        }

        // build tag family spec
        for (int i = 0; i < pb.getTagFamiliesCount(); i++) {
            m.addTagFamilySpec(TagFamilySpec.fromProtobuf(pb.getTagFamilies(i)));
        }

        // build field spec
        for (int i = 0; i < pb.getFieldsCount(); i++) {
            m.addFieldSpec(FieldSpec.fromProtobuf(pb.getFields(i)));
        }

        return m;
    }

    @Override
    public BanyandbDatabase.Measure serialize() {
        List<BanyandbDatabase.TagFamilySpec> tfs = new ArrayList<>(this.tagFamilySpecs.size());
        for (final TagFamilySpec spec : this.tagFamilySpecs) {
            tfs.add(spec.serialize());
        }

        List<BanyandbDatabase.FieldSpec> fs = new ArrayList<>(this.fieldSpecs.size());
        for (final FieldSpec spec : this.fieldSpecs) {
            fs.add(spec.serialize());
        }

        BanyandbDatabase.Measure.Builder b = BanyandbDatabase.Measure.newBuilder()
                .setInterval(this.interval.format())
                .setMetadata(buildMetadata())
                .addAllTagFamilies(tfs)
                .addAllFields(fs)
                .setEntity(BanyandbDatabase.Entity.newBuilder().addAllTagNames(entityTagNames).build());

        if (this.updatedAt != null) {
            b.setUpdatedAt(TimeUtils.buildTimestamp(this.updatedAt));
        }

        return b.build();
    }

    @EqualsAndHashCode
    public static class FieldSpec implements Serializable<BanyandbDatabase.FieldSpec> {
        /**
         * name is the identity of a field
         */
        private final String name;
        /**
         * fieldType denotes the type of field value
         */
        private final FieldType fieldType;
        /**
         * encodingMethod indicates how to encode data during writing
         */
        private final EncodingMethod encodingMethod;
        /**
         * compressionMethod indicates how to compress data during writing
         */
        private final CompressionMethod compressionMethod;

        private FieldSpec(Builder builder) {
            this.name = builder.name;
            this.fieldType = builder.fieldType;
            this.encodingMethod = builder.encodingMethod;
            this.compressionMethod = builder.compressionMethod;
        }

        @RequiredArgsConstructor
        public enum FieldType {
            UNSPECIFIED(BanyandbDatabase.FieldType.FIELD_TYPE_UNSPECIFIED),
            STRING(BanyandbDatabase.FieldType.FIELD_TYPE_STRING),
            INT(BanyandbDatabase.FieldType.FIELD_TYPE_INT),
            BINARY(BanyandbDatabase.FieldType.FIELD_TYPE_DATA_BINARY);

            private final BanyandbDatabase.FieldType fieldType;
        }

        @RequiredArgsConstructor
        public enum EncodingMethod {
            UNSPECIFIED(BanyandbDatabase.EncodingMethod.ENCODING_METHOD_UNSPECIFIED),
            GORILLA(BanyandbDatabase.EncodingMethod.ENCODING_METHOD_GORILLA);

            private final BanyandbDatabase.EncodingMethod encodingMethod;
        }

        @RequiredArgsConstructor
        public enum CompressionMethod {
            UNSPECIFIED(BanyandbDatabase.CompressionMethod.COMPRESSION_METHOD_UNSPECIFIED),
            ZSTD(BanyandbDatabase.CompressionMethod.COMPRESSION_METHOD_ZSTD);

            private final BanyandbDatabase.CompressionMethod compressionMethod;
        }

        @Override
        public BanyandbDatabase.FieldSpec serialize() {
            return BanyandbDatabase.FieldSpec.newBuilder()
                    .setName(this.name)
                    .setFieldType(this.fieldType.fieldType)
                    .setEncodingMethod(this.encodingMethod.encodingMethod)
                    .setCompressionMethod(this.compressionMethod.compressionMethod)
                    .build();
        }

        private static FieldSpec fromProtobuf(BanyandbDatabase.FieldSpec pb) {
            Builder b = null;
            switch (pb.getFieldType()) {
                case FIELD_TYPE_STRING:
                    b = newStringField(pb.getName());
                    break;
                case FIELD_TYPE_INT:
                    b = newIntField(pb.getName());
                    break;
                case FIELD_TYPE_DATA_BINARY:
                    b = newBinaryField(pb.getName());
                    break;
                default:
                    throw new IllegalArgumentException("unrecognized field type");
            }

            switch (pb.getEncodingMethod()) {
                case ENCODING_METHOD_GORILLA:
                    b.encodeWithGorilla();
                    break;
            }

            switch (pb.getCompressionMethod()) {
                case COMPRESSION_METHOD_ZSTD:
                    b.compressWithZSTD();
                    break;
            }

            return b.build();
        }

        /**
         * Create a builder with string type
         *
         * @param name name of the field
         */
        public static Builder newStringField(final String name) {
            return new Builder(name, FieldType.STRING);
        }

        /**
         * Create a builder with int type
         *
         * @param name name of the field
         */
        public static Builder newIntField(final String name) {
            return new Builder(name, FieldType.INT);
        }

        /**
         * Create a builder with binary type
         *
         * @param name name of the field
         */
        public static Builder newBinaryField(final String name) {
            return new Builder(name, FieldType.BINARY);
        }

        public static final class Builder {
            private final String name;
            private final FieldType fieldType;
            private EncodingMethod encodingMethod;
            private CompressionMethod compressionMethod;

            private Builder(final String name, final FieldType fieldType) {
                this.name = name;
                this.fieldType = fieldType;
                this.encodingMethod = EncodingMethod.UNSPECIFIED;
                this.compressionMethod = CompressionMethod.UNSPECIFIED;
            }

            /**
             * Use Gorilla as encoding algorithm
             */
            public Builder encodeWithGorilla() {
                this.encodingMethod = EncodingMethod.GORILLA;
                return this;
            }

            /**
             * Use ZSTD as compression algorithm
             */
            public Builder compressWithZSTD() {
                this.compressionMethod = CompressionMethod.ZSTD;
                return this;
            }

            public FieldSpec build() {
                // TODO: check validity of type, encoding and compression methods?
                return new FieldSpec(this);
            }
        }
    }
}
