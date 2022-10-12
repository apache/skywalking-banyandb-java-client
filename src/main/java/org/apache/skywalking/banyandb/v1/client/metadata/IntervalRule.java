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
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;

@AutoValue
public abstract class IntervalRule implements Serializable<BanyandbCommon.IntervalRule> {
    public enum Unit {
        UNSPECIFIED, HOUR, DAY
    }

    abstract Unit unit();

    abstract int num();

    public static IntervalRule create(Unit unit, int num) {
        return new AutoValue_IntervalRule(unit, num);
    }

    @Override
    public BanyandbCommon.IntervalRule serialize() {
        BanyandbCommon.IntervalRule.Builder builder = BanyandbCommon.IntervalRule.newBuilder();
        switch (unit()) {
            case DAY:
                builder.setUnit(BanyandbCommon.IntervalRule.Unit.UNIT_DAY);
                break;
            case HOUR:
                builder.setUnit(BanyandbCommon.IntervalRule.Unit.UNIT_HOUR);
                break;
        }
        builder.setNum(num());
        return builder.build();
    }

    public static IntervalRule fromProtobuf(BanyandbCommon.IntervalRule intervalRule) {
        Unit unit;
        switch (intervalRule.getUnit()) {
            case UNIT_DAY:
                unit = Unit.DAY;
                break;
            case UNIT_HOUR:
                unit = Unit.HOUR;
                break;
            default:
                unit = Unit.UNSPECIFIED;
                break;
        }
        return new AutoValue_IntervalRule(unit, intervalRule.getNum());
    }
}
