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

import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractQuery<T> {
    /**
     * Group of the current entity
     */
    protected final String group;
    /**
     * Owner name of the current entity
     */
    protected final String name;
    /**
     * The time range for query.
     */
    protected final TimestampRange timestampRange;
    /**
     * Query conditions.
     */
    protected final List<PairQueryCondition<?>> conditions;
    /**
     * The projections of query result.
     * These should have defined in the schema.
     */
    protected final Set<String> projections;

    public AbstractQuery(String group, String name, TimestampRange timestampRange, Set<String> projections) {
        this.group = group;
        this.name = name;
        this.timestampRange = timestampRange;
        this.conditions = new ArrayList<>(10);
        this.projections = projections;
    }

    /**
     * Fluent API for appending query condition
     *
     * @param condition the query condition to be appended
     */
    public AbstractQuery<T> appendCondition(PairQueryCondition<?> condition) {
        this.conditions.add(condition);
        return this;
    }

    /**
     * @return QueryRequest for gRPC level query.
     */
    abstract T build();

    protected BanyandbCommon.Metadata buildMetadata() {
        return BanyandbCommon.Metadata.newBuilder()
                .setGroup(group)
                .setName(name)
                .build();
    }

    protected List<BanyandbModel.Criteria> buildCriteria() {
        List<BanyandbModel.Criteria> criteriaList = new ArrayList<>();
        // set conditions grouped by tagFamilyName
        Map<String, List<PairQueryCondition<?>>> groupedConditions = conditions.stream()
                .collect(Collectors.groupingBy(TagAndValue::getTagFamilyName));
        for (final Map.Entry<String, List<PairQueryCondition<?>>> tagFamily : groupedConditions.entrySet()) {
            final List<BanyandbModel.Condition> conditionList = tagFamily.getValue().stream().map(PairQueryCondition::build)
                    .collect(Collectors.toList());
            BanyandbModel.Criteria criteria = BanyandbModel.Criteria
                    .newBuilder()
                    .setTagFamilyName(tagFamily.getKey())
                    .addAllConditions(conditionList).build();
            criteriaList.add(criteria);
        }
        return criteriaList;
    }

    protected BanyandbModel.TagProjection.Builder buildTagProjection(String tagFamilyName) {
        return this.buildTagProjection(tagFamilyName, this.projections);
    }

    protected BanyandbModel.TagProjection.Builder buildTagProjection(String tagFamilyName, Iterable<String> tagProjections) {
        return BanyandbModel.TagProjection.newBuilder()
                .addTagFamilies(BanyandbModel.TagProjection.TagFamily.newBuilder()
                        .setName(tagFamilyName)
                        .addAllTags(tagProjections)
                        .build());
    }
}
