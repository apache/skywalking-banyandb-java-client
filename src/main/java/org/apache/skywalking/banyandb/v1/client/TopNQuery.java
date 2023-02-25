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
import lombok.Setter;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.measure.v1.BanyandbMeasure;
import org.apache.skywalking.banyandb.model.v1.BanyandbModel;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;

import java.util.List;

@Setter
public class TopNQuery {
    private final String group;
    private final String name;
    private final TimestampRange timestampRange;
    private final int number;
    private final AbstractQuery.Sort sort;
    private MeasureQuery.Aggregation.Type aggregationType = MeasureQuery.Aggregation.Type.UNSPECIFIED;
    /**
     * Query conditions.
     */
    private List<PairQueryCondition<?>> conditions;

    public TopNQuery(String group, String name, TimestampRange timestampRange, int number, AbstractQuery.Sort sort) {
        Preconditions.checkArgument(sort != AbstractQuery.Sort.UNSPECIFIED);
        Preconditions.checkArgument(number > 0);
        this.group = group;
        this.name = name;
        this.timestampRange = timestampRange;
        this.number = number;
        this.sort = sort;
    }

    BanyandbMeasure.TopNRequest build() throws BanyanDBException {
        BanyandbMeasure.TopNRequest.Builder bld = BanyandbMeasure.TopNRequest.newBuilder()
                .setMetadata(BanyandbCommon.Metadata.newBuilder().setGroup(group).setName(name).build())
                .setTimeRange(timestampRange.build())
                .setTopN(number)
                .setFieldValueSort(AbstractQuery.Sort.DESC == sort ? BanyandbModel.Sort.SORT_DESC : BanyandbModel.Sort.SORT_ASC);
        if (aggregationType == null) {
            bld.setAgg(BanyandbModel.AggregationFunction.AGGREGATION_FUNCTION_UNSPECIFIED);
        } else {
            bld.setAgg(aggregationType.function);
        }
        if (conditions != null && !conditions.isEmpty()) {
            for (final PairQueryCondition<?> expr : conditions) {
                if (expr.op != BanyandbModel.Condition.BinaryOp.BINARY_OP_EQ) {
                    throw new UnsupportedOperationException("only equality is supported");
                }
                bld.addConditions(expr.build().getCondition());
            }
        }
        return bld.build();
    }
}
