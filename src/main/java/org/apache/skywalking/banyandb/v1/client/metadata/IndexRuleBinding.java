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
import org.apache.skywalking.banyandb.database.v1.metadata.BanyandbMetadata;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@EqualsAndHashCode(callSuper = true)
public class IndexRuleBinding extends NamedSchema<BanyandbMetadata.IndexRuleBinding> {
    /**
     * rule names refer to the IndexRule
     */
    private List<String> rules;

    /**
     * subject indicates the subject of binding action
     */
    private final Subject subject;

    /**
     * begin_at is the timestamp, after which the binding will be active
     */
    private ZonedDateTime beginAt;

    /**
     * expire_at is the timestamp, after which the binding will be inactive
     * expire_at must be larger than begin_at
     */
    private ZonedDateTime expireAt;

    public IndexRuleBinding(String name, Subject subject) {
        this(name, subject, null);
    }

    private IndexRuleBinding(String name, Subject subject, ZonedDateTime updatedAt) {
        super(name, updatedAt);
        this.rules = new ArrayList<>();
        this.subject = subject;
    }

    /**
     * Add a rule name
     *
     * @param ruleName the name of the IndexRule in the same group
     */
    public IndexRuleBinding addRule(String ruleName) {
        this.rules.add(ruleName);
        return this;
    }

    @Override
    public BanyandbMetadata.IndexRuleBinding serialize(String group) {
        BanyandbMetadata.IndexRuleBinding.Builder b = BanyandbMetadata.IndexRuleBinding.newBuilder()
                .setMetadata(buildMetadata(group))
                .addAllRules(this.rules)
                .setSubject(this.subject.serialize())
                .setBeginAt(TimeUtils.buildTimestamp(this.beginAt))
                .setExpireAt(TimeUtils.buildTimestamp(this.expireAt));
        if (this.updatedAt != null) {
            b.setUpdatedAt(TimeUtils.buildTimestamp(this.updatedAt));
        }
        return b.build();
    }

    static IndexRuleBinding fromProtobuf(BanyandbMetadata.IndexRuleBinding pb) {
        IndexRuleBinding indexRuleBinding = new IndexRuleBinding(pb.getMetadata().getName(), Subject.fromProtobuf(pb.getSubject()), TimeUtils.parseTimestamp(pb.getUpdatedAt()));
        indexRuleBinding.setRules(new ArrayList<>(pb.getRulesList()));
        indexRuleBinding.setBeginAt(TimeUtils.parseTimestamp(pb.getBeginAt()));
        indexRuleBinding.setExpireAt(TimeUtils.parseTimestamp(pb.getExpireAt()));
        return indexRuleBinding;
    }

    @RequiredArgsConstructor
    @Getter
    @EqualsAndHashCode
    public static class Subject implements Serializable<BanyandbMetadata.Subject> {
        /**
         * name refers to a stream or measure in a particular catalog
         */
        private final String name;

        /**
         * catalog is where the subject belongs to
         */
        private final Catalog catalog;

        @Override
        public BanyandbMetadata.Subject serialize() {
            return BanyandbMetadata.Subject.newBuilder()
                    .setName(this.name)
                    .setCatalog(this.catalog.getCatalog())
                    .build();
        }

        /**
         * Create a subject that refer to a stream
         *
         * @param name the name of the stream
         * @return a stream subject with a given name
         */
        public static Subject referToStream(final String name) {
            return new Subject(name, Catalog.STREAM);
        }

        /**
         * Create a subject that refer to a measure
         *
         * @param name the name of the measure
         * @return a measure subject with a given name
         */
        public static Subject referToMeasure(final String name) {
            return new Subject(name, Catalog.MEASURE);
        }

        private static Subject fromProtobuf(BanyandbMetadata.Subject pb) {
            switch (pb.getCatalog()) {
                case CATALOG_STREAM:
                    return referToStream(pb.getName());
                case CATALOG_MEASURE:
                    return referToMeasure(pb.getName());
                default:
                    throw new IllegalArgumentException("unrecognized catalog");
            }
        }
    }
}
