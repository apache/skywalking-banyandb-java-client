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
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.v1.client.AbstractCriteria;
import org.apache.skywalking.banyandb.v1.client.AbstractQuery;
import org.apache.skywalking.banyandb.v1.client.util.TimeUtils;

import javax.annotation.Nullable;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

@AutoValue
public abstract class TopNAggregation extends NamedSchema<BanyandbDatabase.TopNAggregation> {
    public abstract String sourceMeasureName();

    public abstract String fieldName();

    @Nullable
    public abstract AbstractQuery.Sort fieldValueSort();

    @Nullable
    public abstract ImmutableList<String> groupByTagNames();

    @Nullable
    abstract AbstractCriteria criteria();

    abstract int countersNumber();

    abstract int lruSize();

    abstract TopNAggregation.Builder toBuilder();

    public static TopNAggregation.Builder create(String group, String name) {
        return new AutoValue_TopNAggregation.Builder().setGroup(group).setName(name);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        abstract TopNAggregation.Builder setGroup(String group);

        abstract TopNAggregation.Builder setName(String name);

        abstract TopNAggregation.Builder setUpdatedAt(ZonedDateTime updatedAt);

        public abstract TopNAggregation.Builder setLruSize(int lruSize);

        public abstract TopNAggregation.Builder setCriteria(AbstractCriteria criteria);

        public abstract TopNAggregation.Builder setFieldValueSort(AbstractQuery.Sort sort);

        public abstract TopNAggregation.Builder setFieldName(String fieldName);

        public abstract TopNAggregation.Builder setCountersNumber(int countersNumber);

        public abstract TopNAggregation.Builder setGroupByTagNames(String... groupByTagNames);

        public abstract TopNAggregation.Builder setGroupByTagNames(List<String> groupByTagNames);

        public abstract TopNAggregation.Builder setSourceMeasureName(String sourceMeasureName);

        public abstract TopNAggregation build();
    }

    @Override
    public BanyandbDatabase.TopNAggregation serialize() {
        BanyandbDatabase.TopNAggregation.Builder bld = BanyandbDatabase.TopNAggregation.newBuilder()
                .setMetadata(buildMetadata());

        bld.setFieldName(this.fieldName())
                .setSourceMeasure(BanyandbCommon.Metadata.newBuilder().setGroup(group()).setName(this.sourceMeasureName()).build())
                .setCountersNumber(this.countersNumber())
                .setLruSize(this.lruSize());

        if (this.criteria() != null) {
            bld.setCriteria(this.criteria().build());
        }

        if (this.groupByTagNames() != null) {
            bld.addAllGroupByTagNames(this.groupByTagNames());
        } else {
            bld.addAllGroupByTagNames(Collections.emptyList());
        }

        if (this.fieldValueSort() == null || this.fieldValueSort() == AbstractQuery.Sort.UNSPECIFIED) {
            bld.setFieldValueSort(BanyandbModel.Sort.SORT_UNSPECIFIED);
        } else {
            bld.setFieldValueSort(AbstractQuery.Sort.DESC == this.fieldValueSort() ? BanyandbModel.Sort.SORT_DESC : BanyandbModel.Sort.SORT_ASC);
        }

        if (this.updatedAt() != null) {
            bld.setUpdatedAt(TimeUtils.buildTimestamp(updatedAt()));
        }
        return bld.build();
    }

    public static TopNAggregation fromProtobuf(final BanyandbDatabase.TopNAggregation pb) {
        TopNAggregation.Builder bld = TopNAggregation.create(pb.getMetadata().getGroup(), pb.getMetadata().getName())
                .setUpdatedAt(TimeUtils.parseTimestamp(pb.getUpdatedAt()))
                .setCountersNumber(pb.getCountersNumber())
                .setLruSize(pb.getLruSize())
                .setFieldName(pb.getFieldName())
                .setSourceMeasureName(pb.getSourceMeasure().getName())
                .setGroupByTagNames(pb.getGroupByTagNamesList());

        switch (pb.getFieldValueSort()) {
            case SORT_ASC:
                bld.setFieldValueSort(AbstractQuery.Sort.ASC);
                break;
            case SORT_DESC:
                bld.setFieldValueSort(AbstractQuery.Sort.DESC);
                break;
            default:
                bld.setFieldValueSort(AbstractQuery.Sort.UNSPECIFIED);
        }

        // TODO: deserialize Criteria

        return bld.build();
    }
}
