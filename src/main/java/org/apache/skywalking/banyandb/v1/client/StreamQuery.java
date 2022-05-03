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

import java.util.Set;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.stream.v1.BanyandbStream;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;

/**
 * StreamQuery is the high-level query API for the stream model.
 */
@Setter
public class StreamQuery extends AbstractQuery<BanyandbStream.QueryRequest> {
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

    public StreamQuery(final String group, final String name, final TimestampRange timestampRange, final Set<String> projections) {
        super(group, name, timestampRange, projections);
        this.offset = 0;
        this.limit = 20;
    }

    public StreamQuery(final String group, final String name, final Set<String> projections) {
        this(group, name, null, projections);
    }

    @Override
    public StreamQuery and(PairQueryCondition<?> condition) {
        return (StreamQuery) super.and(condition);
    }

    @Override
    BanyandbStream.QueryRequest build() throws BanyanDBException {
        final BanyandbStream.QueryRequest.Builder builder = BanyandbStream.QueryRequest.newBuilder()
                .setMetadata(buildMetadata());
        if (timestampRange != null) {
            builder.setTimeRange(timestampRange.build());
        }
        builder.setProjection(buildTagProjections());
        // set conditions grouped by tagFamilyName
        builder.addAllCriteria(buildCriteria());
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

        private BanyandbModel.QueryOrder build() {
            final BanyandbModel.QueryOrder.Builder builder = BanyandbModel.QueryOrder.newBuilder();
            builder.setIndexRuleName(indexRuleName);
            builder.setSort(
                    Type.DESC.equals(type) ? BanyandbModel.Sort.SORT_DESC : BanyandbModel.Sort.SORT_ASC);
            return builder.build();
        }

        public enum Type {
            ASC, DESC
        }
    }
}
