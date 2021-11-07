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

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.banyandb.database.v1.metadata.BanyandbMetadata;

@EqualsAndHashCode
public class Duration implements Serializable<BanyandbMetadata.Duration> {
    private final int val;
    private final Unit unit;

    private Duration(int val, Unit unit) {
        this.val = val;
        this.unit = unit;
    }

    public static Duration ofHours(int val) {
        return new Duration(val, Unit.HOUR);
    }

    public static Duration ofDays(int val) {
        return new Duration(val, Unit.DAY);
    }

    public static Duration ofWeeks(int val) {
        return new Duration(val, Unit.WEEK);
    }

    public static Duration ofMonths(int val) {
        return new Duration(val, Unit.MONTH);
    }

    @Override
    public BanyandbMetadata.Duration serialize() {
        return BanyandbMetadata.Duration.newBuilder()
                .setVal(this.val)
                .setUnit(this.unit.getDurationUnit())
                .build();
    }

    @RequiredArgsConstructor
    public enum Unit {
        HOUR(BanyandbMetadata.Duration.DurationUnit.DURATION_UNIT_HOUR),
        DAY(BanyandbMetadata.Duration.DurationUnit.DURATION_UNIT_DAY),
        WEEK(BanyandbMetadata.Duration.DurationUnit.DURATION_UNIT_WEEK),
        MONTH(BanyandbMetadata.Duration.DurationUnit.DURATION_UNIT_MONTH);

        @Getter(AccessLevel.PRIVATE)
        private final BanyandbMetadata.Duration.DurationUnit durationUnit;
    }

    static Duration fromProtobuf(BanyandbMetadata.Duration duration) {
        switch (duration.getUnit()) {
            case DURATION_UNIT_DAY:
                return ofDays(duration.getVal());
            case DURATION_UNIT_HOUR:
                return ofHours(duration.getVal());
            case DURATION_UNIT_MONTH:
                return ofMonths(duration.getVal());
            case DURATION_UNIT_WEEK:
                return ofWeeks(duration.getVal());
        }
        throw new IllegalArgumentException("unrecognized DurationUnit");
    }
}
