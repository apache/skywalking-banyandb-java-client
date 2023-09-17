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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.v1.client.util.IgnoreHashEquals;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@AutoValue
public abstract class Measure extends NamedSchema<BanyandbDatabase.Measure> {

    /**
     * specs of tag families
     */
    abstract ImmutableList<TagFamilySpec> tagFamilies();

    /**
     * fieldSpecs denote measure values
     */
    abstract ImmutableList<FieldSpec> fields();

    /**
     * tag names used to generate an entity
     */
    abstract ImmutableList<String> entityRelativeTags();

    /**
     * interval indicates how frequently to send a data point
     */
    abstract Duration interval();

    /**
     * last updated revision
     * This field can only be set by the server.
     */
    @Nullable
    @IgnoreHashEquals
    abstract Long modRevision();

    /**
     * index rules bound to the stream
     */
    public abstract ImmutableList<IndexRule> indexRules();

    public abstract Measure.Builder toBuilder();

    public final Measure withIndexRules(List<IndexRule> indexRules) {
        return toBuilder().addIndexes(indexRules)
                .build();
    }

    public final Measure withModRevision(Long modRevision) {
        return toBuilder().setModRevision(modRevision).build();
    }

    public static Measure.Builder create(String group, String name, Duration interval) {
        return new AutoValue_Measure.Builder().setGroup(group).setName(name).setInterval(interval);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        abstract String group();

        abstract ImmutableList<TagFamilySpec> tagFamilies();

        abstract Measure.Builder setGroup(String group);

        abstract Measure.Builder setName(String name);

        abstract Measure.Builder setUpdatedAt(ZonedDateTime updatedAt);

        abstract Measure.Builder setModRevision(Long modRevision);

        abstract ImmutableList.Builder<TagFamilySpec> tagFamiliesBuilder();

        public final Measure.Builder addTagFamily(TagFamilySpec tagFamilySpec) {
            tagFamiliesBuilder().add(tagFamilySpec);
            return this;
        }

        public final Measure.Builder addTagFamilies(Iterable<TagFamilySpec> tagFamilySpecs) {
            tagFamiliesBuilder().addAll(tagFamilySpecs);
            return this;
        }

        abstract ImmutableList.Builder<FieldSpec> fieldsBuilder();

        public final Measure.Builder addField(FieldSpec fieldSpec) {
            fieldsBuilder().add(fieldSpec);
            return this;
        }

        abstract ImmutableList.Builder<IndexRule> indexRulesBuilder();

        public final Measure.Builder addIndexes(Iterable<IndexRule> indexRules) {
            for (final IndexRule ir : indexRules) {
                this.addIndex(ir);
            }
            return this;
        }

        public final Measure.Builder addIndex(IndexRule indexRule) {
            indexRulesBuilder().add(indexRule.withGroup(group()));
            return this;
        }

        abstract Measure.Builder setInterval(Duration interval);

        public abstract Measure.Builder setEntityRelativeTags(List<String> entityRelativeTags);

        public abstract Measure.Builder setEntityRelativeTags(String... entityRelativeTags);

        public abstract Measure build();
    }

    static Measure fromProtobuf(BanyandbDatabase.Measure pb) {
        final Measure.Builder m = Measure.create(pb.getMetadata().getGroup(), pb.getMetadata().getName(),
                        Duration.parse(pb.getInterval()))
                .setUpdatedAt(TimeUtils.parseTimestamp(pb.getUpdatedAt()))
                .setModRevision(pb.getMetadata().getModRevision())
                .setEntityRelativeTags(pb.getEntity().getTagNamesList());

        // build tag family spec
        for (int i = 0; i < pb.getTagFamiliesCount(); i++) {
            m.addTagFamily(TagFamilySpec.fromProtobuf(pb.getTagFamilies(i)));
        }

        // build field spec
        for (int i = 0; i < pb.getFieldsCount(); i++) {
            m.addField(FieldSpec.fromProtobuf(pb.getFields(i)));
        }

        return m.build();
    }

    @Override
    public BanyandbDatabase.Measure serialize() {
        List<BanyandbDatabase.TagFamilySpec> tfs = new ArrayList<>(this.tagFamilies().size());
        for (final TagFamilySpec spec : this.tagFamilies()) {
            tfs.add(spec.serialize());
        }

        List<BanyandbDatabase.FieldSpec> fs = new ArrayList<>(this.fields().size());
        for (final FieldSpec spec : this.fields()) {
            fs.add(spec.serialize());
        }

        BanyandbCommon.Metadata metadata = buildMetadata();
        if (this.modRevision() != null) {
            metadata = buildMetadata(this.modRevision());
        }
        BanyandbDatabase.Measure.Builder b = BanyandbDatabase.Measure.newBuilder()
                .setInterval(interval().format())
                .setMetadata(metadata)
                .addAllTagFamilies(tfs)
                .addAllFields(fs)
                .setEntity(BanyandbDatabase.Entity.newBuilder().addAllTagNames(entityRelativeTags()).build());

        if (updatedAt() != null) {
            b.setUpdatedAt(TimeUtils.buildTimestamp(updatedAt()));
        }

        return b.build();
    }

    @EqualsAndHashCode
    public static class FieldSpec implements Serializable<BanyandbDatabase.FieldSpec> {
        /**
         * name is the identity of a field
         */
        @Getter
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
            BINARY(BanyandbDatabase.FieldType.FIELD_TYPE_DATA_BINARY),
            FLOAT(BanyandbDatabase.FieldType.FIELD_TYPE_FLOAT);

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
                case FIELD_TYPE_FLOAT:
                    b = newFloatField(pb.getName());
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
         * Create a builder with float type
         *
         * @param name name of the field
         */
        public static Builder newFloatField(String name) {
            return new Builder(name, FieldType.FLOAT);
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
