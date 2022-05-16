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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.apache.skywalking.banyandb.v1.client.metadata.Measure;

import java.util.Set;

/**
 * MeasureQuery is the high-level query API for the measure model.
 */
@Setter
public class MeasureQuery extends AbstractQuery<BanyandbMeasure.QueryRequest> {
    /**
     * field_projection can be used to select fields of the data points in the response
     */
    private final Set<String> fieldProjections;

    private Aggregation aggregation;

    private TopN topN;

    public MeasureQuery(final String group, final String name, final Set<String> tagProjections, final Set<String> fieldProjections) {
        this(group, name, null, tagProjections, fieldProjections);
    }

    public MeasureQuery(final String group, final String name, final TimestampRange timestampRange, final Set<String> tagProjections, final Set<String> fieldProjections) {
        super(group, name, timestampRange, addIDProjection(tagProjections));
        this.fieldProjections = fieldProjections;
    }

    public MeasureQuery groupBy(Set<String> groupByKeys) {
        Preconditions.checkArgument(this.tagProjections.containsAll(groupByKeys), "groupBy tags should be selected first");
        this.aggregation = new Aggregation(null, Aggregation.Type.UNSPECIFIED, groupByKeys);
        return this;
    }

    public MeasureQuery maxBy(String field, Set<String> groupByKeys) {
        Preconditions.checkArgument(fieldProjections.contains(field), "field should be selected first");
        Preconditions.checkArgument(this.tagProjections.containsAll(groupByKeys), "groupBy tags should be selected first");
        this.aggregation = new Aggregation(field, Aggregation.Type.MAX, groupByKeys);
        return this;
    }

    public MeasureQuery minBy(String field, Set<String> groupByKeys) {
        Preconditions.checkArgument(fieldProjections.contains(field), "field should be selected first");
        Preconditions.checkArgument(this.tagProjections.containsAll(groupByKeys), "groupBy tags should be selected first");
        Preconditions.checkState(this.aggregation == null, "aggregation should only be set once");
        this.aggregation = new Aggregation(field, Aggregation.Type.MIN, groupByKeys);
        return this;
    }

    public MeasureQuery meanBy(String field, Set<String> groupByKeys) {
        Preconditions.checkArgument(fieldProjections.contains(field), "field should be selected first");
        Preconditions.checkArgument(this.tagProjections.containsAll(groupByKeys), "groupBy tags should be selected first");
        Preconditions.checkState(this.aggregation == null, "aggregation should only be set once");
        this.aggregation = new Aggregation(field, Aggregation.Type.MEAN, groupByKeys);
        return this;
    }

    public MeasureQuery countBy(String field, Set<String> groupByKeys) {
        Preconditions.checkArgument(fieldProjections.contains(field), "field should be selected first");
        Preconditions.checkArgument(this.tagProjections.containsAll(groupByKeys), "groupBy tags should be selected first");
        Preconditions.checkState(this.aggregation == null, "aggregation should only be set once");
        this.aggregation = new Aggregation(field, Aggregation.Type.COUNT, groupByKeys);
        return this;
    }

    public MeasureQuery sumBy(String field, Set<String> groupByKeys) {
        Preconditions.checkArgument(fieldProjections.contains(field), "field should be selected first");
        Preconditions.checkArgument(this.tagProjections.containsAll(groupByKeys), "groupBy tags should be selected first");
        Preconditions.checkState(this.aggregation == null, "aggregation should only be set once");
        this.aggregation = new Aggregation(field, Aggregation.Type.COUNT, groupByKeys);
        return this;
    }

    public MeasureQuery limit(int number, String field, Sort sort) {
        Preconditions.checkArgument(fieldProjections.contains(field), "field should be selected first");
        this.topN = new TopN(field, number, sort);
        return this;
    }

    /**
     * Query ID column with given value.
     *
     * @param value candidate value of ID
     */
    public MeasureQuery andWithID(String value) {
        this.and(PairQueryCondition.IDQueryCondition.eq(Measure.ID, value));
        return this;
    }

    /**
     * @return QueryRequest for gRPC level query.
     */
    BanyandbMeasure.QueryRequest build() throws BanyanDBException {
        final BanyandbMeasure.QueryRequest.Builder builder = BanyandbMeasure.QueryRequest.newBuilder();
        builder.setMetadata(buildMetadata());
        if (timestampRange != null) {
            builder.setTimeRange(timestampRange.build());
        } else {
            builder.setTimeRange(TimestampRange.MAX_RANGE);
        }
        builder.setTagProjection(buildTagProjections());
        builder.setFieldProjection(BanyandbMeasure.QueryRequest.FieldProjection.newBuilder()
                .addAllNames(fieldProjections)
                .build());
        if (this.aggregation != null) {
            BanyandbMeasure.QueryRequest.GroupBy.Builder groupByBuilder = BanyandbMeasure.QueryRequest.GroupBy.newBuilder()
                    .setTagProjection(buildTagProjections(this.aggregation.groupByTagsProjection));
            if (Strings.isNullOrEmpty(this.aggregation.fieldName)) {
                if (this.aggregation.aggregationType != Aggregation.Type.UNSPECIFIED) {
                    throw new IllegalArgumentException("field name cannot be null or empty if aggregation is specified");
                }
            } else {
                builder.setGroupBy(groupByBuilder.build());
                groupByBuilder.setFieldName(this.aggregation.fieldName);
                BanyandbMeasure.QueryRequest.Aggregation aggr = BanyandbMeasure.QueryRequest.Aggregation.newBuilder()
                        .setFunction(this.aggregation.aggregationType.function)
                        .setFieldName(this.aggregation.fieldName)
                        .build();
                builder.setGroupBy(groupByBuilder.build()).setAgg(aggr);
            }
        }
        if (this.topN != null) {
            BanyandbMeasure.QueryRequest.Top top = BanyandbMeasure.QueryRequest.Top.newBuilder()
                    .setFieldName(this.topN.fieldName)
                    .setNumber(this.topN.number)
                    .setFieldValueSort(Sort.DESC.equals(this.topN.sort) ? BanyandbModel.Sort.SORT_DESC : BanyandbModel.Sort.SORT_ASC)
                    .build();
            builder.setTop(top);
        }
        // add all criteria
        builder.addAllCriteria(buildCriteria());
        return builder.build();
    }

    @RequiredArgsConstructor
    public static class TopN {
        private final String fieldName;
        private final int number;
        private final AbstractQuery.Sort sort;
    }

    @RequiredArgsConstructor
    public static class Aggregation {
        private final String fieldName;
        private final Type aggregationType;
        private final Set<String> groupByTagsProjection;

        @RequiredArgsConstructor
        public enum Type {
            UNSPECIFIED(BanyandbModel.AggregationFunction.AGGREGATION_FUNCTION_UNSPECIFIED),
            MEAN(BanyandbModel.AggregationFunction.AGGREGATION_FUNCTION_MEAN),
            MAX(BanyandbModel.AggregationFunction.AGGREGATION_FUNCTION_MAX),
            MIN(BanyandbModel.AggregationFunction.AGGREGATION_FUNCTION_MIN),
            COUNT(BanyandbModel.AggregationFunction.AGGREGATION_FUNCTION_COUNT),
            SUM(BanyandbModel.AggregationFunction.AGGREGATION_FUNCTION_SUM);
            private final BanyandbModel.AggregationFunction function;
        }
    }

    static ImmutableSet<String> addIDProjection(Set<String> tagProjections) {
        // make a defensive copy in case the original one is immutable
        return ImmutableSet.<String>builder().addAll(tagProjections).add(Measure.ID).build();
    }
}
