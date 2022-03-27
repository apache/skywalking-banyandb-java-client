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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

@AutoValue
public abstract class IndexRuleBinding extends NamedSchema<BanyandbDatabase.IndexRuleBinding> {
    private static final ZonedDateTime DEFAULT_EXPIRE_AT = ZonedDateTime.of(2099, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    /**
     * rule names refer to the IndexRule
     */
    public abstract ImmutableList<String> rules();

    /**
     * subject indicates the subject of binding action
     */
    abstract Subject subject();

    /**
     * begin_at is the timestamp, after which the binding will be active
     */
    abstract ZonedDateTime beginAt();

    /**
     * expire_at is the timestamp, after which the binding will be inactive
     * expire_at must be larger than begin_at
     */
    abstract ZonedDateTime expireAt();

    public static IndexRuleBinding create(String group, String name, Subject subject, List<String> rules) {
        return new AutoValue_IndexRuleBinding(group, name, null,
                ImmutableList.copyOf(rules), subject, ZonedDateTime.now(), DEFAULT_EXPIRE_AT);
    }

    @VisibleForTesting
    static IndexRuleBinding create(String group, String name, Subject subject, List<String> rules, ZonedDateTime beginAt, ZonedDateTime expireAt) {
        return new AutoValue_IndexRuleBinding(group, name, null,
                ImmutableList.copyOf(rules), subject, beginAt, expireAt);
    }

    @Override
    public BanyandbDatabase.IndexRuleBinding serialize() {
        BanyandbDatabase.IndexRuleBinding.Builder b = BanyandbDatabase.IndexRuleBinding.newBuilder()
                .setMetadata(buildMetadata())
                .addAllRules(rules())
                .setSubject(subject().serialize())
                .setBeginAt(TimeUtils.buildTimestamp(beginAt()))
                .setExpireAt(TimeUtils.buildTimestamp(expireAt()));
        if (updatedAt() != null) {
            b.setUpdatedAt(TimeUtils.buildTimestamp(updatedAt()));
        }
        return b.build();
    }

    static IndexRuleBinding fromProtobuf(BanyandbDatabase.IndexRuleBinding pb) {
        return new AutoValue_IndexRuleBinding(
                pb.getMetadata().getGroup(),
                pb.getMetadata().getName(),
                TimeUtils.parseTimestamp(pb.getUpdatedAt()),
                ImmutableList.copyOf(pb.getRulesList()),
                Subject.fromProtobuf(pb.getSubject()),
                TimeUtils.parseTimestamp(pb.getBeginAt()),
                TimeUtils.parseTimestamp(pb.getExpireAt())
        );
    }

    @RequiredArgsConstructor
    @Getter
    @EqualsAndHashCode
    public static class Subject implements Serializable<BanyandbDatabase.Subject> {
        /**
         * name refers to a stream or measure in a particular catalog
         */
        private final String name;

        /**
         * catalog is where the subject belongs to
         */
        private final Catalog catalog;

        @Override
        public BanyandbDatabase.Subject serialize() {
            return BanyandbDatabase.Subject.newBuilder()
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

        private static Subject fromProtobuf(BanyandbDatabase.Subject pb) {
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
