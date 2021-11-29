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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.skywalking.banyandb.v1.Banyandb;
import org.apache.skywalking.banyandb.v1.stream.BanyandbStream;

/**
 * StreamQuery is the high-level query API for the stream model.
 */
@Setter
public class StreamQuery {
    /**
     * Owner name current entity
     */
    private final String name;
    /**
     * The time range for query.
     */
    private final TimestampRange timestampRange;
    /**
     * The projections of query result.
     * These should have defined in the schema and must be `searchable`.
     */
    private final List<String> projections;
    /**
     * Query conditions.
     */
    private final List<PairQueryCondition<?>> conditions;
    /**
     * The starting row id of the query. Default value is 0.
     */
    private int offset;
    /**
     * The limit size of the query. Default value is 20.
     */
    private int limit;
    /**
     * One order condition is supported and optional.
     */
    private OrderBy orderBy;
    /**
     * Whether to fetch unindexed fields from the "data" tag family for the query
     */
    private List<String> dataProjections;

    public StreamQuery(final String name, final TimestampRange timestampRange, final List<String> projections) {
        this.name = name;
        this.timestampRange = timestampRange;
        this.projections = projections;
        this.conditions = new ArrayList<>(10);
        this.offset = 0;
        this.limit = 20;
        // by default, we don't need projection for data tag family
        this.dataProjections = Collections.emptyList();
    }

    public StreamQuery(final String name, final List<String> projections) {
        this(name, null, projections);
    }

    /**
     * Fluent API for appending query condition
     *
     * @param condition the query condition to be appended
     */
    public StreamQuery appendCondition(PairQueryCondition<?> condition) {
        this.conditions.add(condition);
        return this;
    }

    /**
     * @param group The instance name.
     * @return QueryRequest for gRPC level query.
     */
    BanyandbStream.QueryRequest build(String group) {
        final BanyandbStream.QueryRequest.Builder builder = BanyandbStream.QueryRequest.newBuilder();
        builder.setMetadata(Banyandb.Metadata.newBuilder()
                .setGroup(group)
                .setName(name)
                .build());
        if (timestampRange != null) {
            builder.setTimeRange(timestampRange.build());
        }
        // set projection
        Banyandb.Projection.Builder projectionBuilder = Banyandb.Projection.newBuilder()
                .addTagFamilies(Banyandb.Projection.TagFamily.newBuilder()
                        .setName("searchable")
                        .addAllTags(this.projections)
                        .build());
        if (!this.dataProjections.isEmpty()) {
            projectionBuilder.addTagFamilies(Banyandb.Projection.TagFamily.newBuilder()
                    .setName("data")
                    .addAllTags(this.dataProjections)
                    .build());
        }
        builder.setProjection(projectionBuilder);
        // set conditions grouped by tagFamilyName
        Map<String, List<PairQueryCondition<?>>> groupedConditions = conditions.stream()
                .collect(Collectors.groupingBy(TagAndValue::getTagFamilyName));
        for (final Map.Entry<String, List<PairQueryCondition<?>>> tagFamily : groupedConditions.entrySet()) {
            final List<Banyandb.Condition> conditionList = tagFamily.getValue().stream().map(PairQueryCondition::build)
                    .collect(Collectors.toList());
            BanyandbStream.QueryRequest.Criteria criteria = BanyandbStream.QueryRequest.Criteria
                    .newBuilder()
                    .setTagFamilyName(tagFamily.getKey())
                    .addAllConditions(conditionList).build();
            builder.addCriteria(criteria);
        }
        builder.setOffset(offset);
        builder.setLimit(limit);
        if (orderBy != null) {
            builder.setOrderBy(orderBy.build());
        }
        return builder.build();
    }

    @RequiredArgsConstructor
    public static class OrderBy {
        /**
         * The field name for ordering.
         */
        private final String indexRuleName;
        /**
         * The type of ordering.
         */
        private final Type type;

        private Banyandb.QueryOrder build() {
            final Banyandb.QueryOrder.Builder builder = Banyandb.QueryOrder.newBuilder();
            builder.setIndexRuleName(indexRuleName);
            builder.setSort(
                    Type.DESC.equals(type) ? Banyandb.QueryOrder.Sort.SORT_DESC : Banyandb.QueryOrder.Sort.SORT_ASC);
            return builder.build();
        }

        public enum Type {
            ASC, DESC
        }
    }
}
