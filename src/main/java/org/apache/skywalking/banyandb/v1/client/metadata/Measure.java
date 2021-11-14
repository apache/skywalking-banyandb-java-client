package org.apache.skywalking.banyandb.v1.client.metadata;

import lombok.*;
import org.apache.skywalking.banyandb.database.v1.metadata.BanyandbMetadata;
import org.apache.skywalking.banyandb.v1.Banyandb;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@EqualsAndHashCode(callSuper = true)
public class Measure extends NamedSchema<BanyandbMetadata.Measure> {
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
     * intervalRules define data points writing interval
     */
    private List<IntervalRule<?>> intervalRules;

    /**
     * number of shards
     */
    private int shardNum;

    /**
     * duration determines how long a Stream keeps its data
     */
    private Duration ttl;

    private Measure(String name, int shardNum, Duration ttl, ZonedDateTime updatedAt) {
        super(name, updatedAt);
        this.tagFamilySpecs = new ArrayList<>();
        this.entityTagNames = new ArrayList<>();
        this.fieldSpecs = new ArrayList<>();
        this.intervalRules = new ArrayList<>();
        this.shardNum = shardNum;
        this.ttl = ttl;
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
     * Add an interval rule to the schema
     *
     * @param tagFamilySpec an interval rule to match tag name and value
     */
    public Measure addIntervalRule(TagFamilySpec tagFamilySpec) {
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

    @Override
    public BanyandbMetadata.Measure serialize(String group) {
        List<BanyandbMetadata.TagFamilySpec> tfs = new ArrayList<>(this.tagFamilySpecs.size());
        for (final TagFamilySpec spec : this.tagFamilySpecs) {
            tfs.add(spec.serialize());
        }

        List<BanyandbMetadata.FieldSpec> fs = new ArrayList<>(this.fieldSpecs.size());
        for (final FieldSpec spec : this.fieldSpecs) {
            fs.add(spec.serialize());
        }

        List<BanyandbMetadata.IntervalRule> irs = new ArrayList<>(this.intervalRules.size());
        for (final IntervalRule<?> spec : this.intervalRules) {
            irs.add(spec.serialize());
        }

        BanyandbMetadata.Measure.Builder b = BanyandbMetadata.Measure.newBuilder()
                .setMetadata(buildMetadata(group))
                .addAllTagFamilies(tfs)
                .addAllFields(fs)
                .addAllIntervalRules(irs)
                .setEntity(BanyandbMetadata.Entity.newBuilder().addAllTagNames(entityTagNames).build());

        if (this.updatedAt != null) {
            b.setUpdatedAtNanoseconds(TimeUtils.buildTimestamp(this.updatedAt));
        }

        return b.build();
    }

    @Builder
    public static class FieldSpec implements Serializable<BanyandbMetadata.FieldSpec> {
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

        @RequiredArgsConstructor
        public enum FieldType {
            UNSPECIFIED(BanyandbMetadata.FieldType.FIELD_TYPE_UNSPECIFIED),
            STRING(BanyandbMetadata.FieldType.FIELD_TYPE_STRING),
            INT(BanyandbMetadata.FieldType.FIELD_TYPE_INT),
            BINARY(BanyandbMetadata.FieldType.FIELD_TYPE_DATA_BINARY);

            private final BanyandbMetadata.FieldType fieldType;
        }

        @RequiredArgsConstructor
        public enum EncodingMethod {
            UNSPECIFIED(BanyandbMetadata.EncodingMethod.ENCODING_METHOD_UNSPECIFIED),
            GORILLA(BanyandbMetadata.EncodingMethod.ENCODING_METHOD_GORILLA);

            private final BanyandbMetadata.EncodingMethod encodingMethod;
        }

        @RequiredArgsConstructor
        public enum CompressionMethod {
            UNSPECIFIED(BanyandbMetadata.CompressionMethod.COMPRESSION_METHOD_UNSPECIFIED),
            ZSTD(BanyandbMetadata.CompressionMethod.COMPRESSION_METHOD_ZSTD);

            private final BanyandbMetadata.CompressionMethod compressionMethod;
        }

        @Override
        public BanyandbMetadata.FieldSpec serialize() {
            return BanyandbMetadata.FieldSpec.newBuilder()
                    .setName(this.name)
                    .setFieldType(this.fieldType.fieldType)
                    .setEncodingMethod(this.encodingMethod.encodingMethod)
                    .setCompressionMethod(this.compressionMethod.compressionMethod)
                    .build();
        }
    }

    @Getter
    @EqualsAndHashCode
    public static abstract class IntervalRule<T> implements Serializable<BanyandbMetadata.IntervalRule> {
        /**
         * name of the tag to be matched
         */
        protected final String tagName;
        /**
         * value of the tag matched
         */
        protected final T tagValue;
        /**
         * interval of the measure
         */
        protected final String interval;

        public IntervalRule(String tagName, T tagValue, String interval) {
            this.tagName = tagName;
            this.tagValue = tagValue;
            this.interval = interval;
        }

        protected abstract BanyandbMetadata.IntervalRule.Builder applyTagValue(BanyandbMetadata.IntervalRule.Builder b);

        /**
         * Create an interval rule to match a tag with string value
         *
         * @param tagName
         * @param tagValue value of the tag, which must be string
         * @param interval
         * @return an interval rule to match a tag with given string
         */
        public static IntervalRule<String> matchStringLabel(final String tagName, final String tagValue, final String interval) {
            return new IntervalRule<String>(tagName, tagValue, interval) {
                @Override
                protected BanyandbMetadata.IntervalRule.Builder applyTagValue(BanyandbMetadata.IntervalRule.Builder b) {
                    return b.setStr(this.tagValue);
                }
            };
        }

        /**
         * Create an interval rule to match a tag with string value
         *
         * @param tagName
         * @param tagValue value of the tag, which must be string
         * @param interval
         * @return an interval rule to match a tag with given string
         */
        public static IntervalRule<Long> matchNumericLabel(final String tagName, final Long tagValue, final String interval) {
            return new IntervalRule<Long>(tagName, tagValue, interval) {
                @Override
                protected BanyandbMetadata.IntervalRule.Builder applyTagValue(BanyandbMetadata.IntervalRule.Builder b) {
                    return b.setInt(this.tagValue);
                }
            };
        }

        @Override
        public BanyandbMetadata.IntervalRule serialize() {
            return applyTagValue(BanyandbMetadata.IntervalRule.newBuilder()
                    .setTagName(this.tagName)
                    .setInterval(this.interval))
                    .build();
        }
    }
}
