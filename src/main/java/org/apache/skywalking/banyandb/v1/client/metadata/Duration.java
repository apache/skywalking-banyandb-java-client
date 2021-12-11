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
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase;

@EqualsAndHashCode
public class Duration implements Serializable<BanyandbDatabase.Duration> {
    private final int val;
    private final Unit unit;

    private Duration(int val, Unit unit) {
        this.val = val;
        this.unit = unit;
    }

    /**
     * Create duration with hours
     *
     * @param hours the number of hours
     * @return Duration in the unit of hour
     */
    public static Duration ofHours(int hours) {
        return new Duration(hours, Unit.HOUR);
    }

    /**
     * Create duration with days
     *
     * @param days the number of days
     * @return Duration in the unit of day
     */
    public static Duration ofDays(int days) {
        return new Duration(days, Unit.DAY);
    }

    /**
     * Create duration with weeks
     *
     * @param weeks the number of weeks
     * @return Duration in the unit of week
     */
    public static Duration ofWeeks(int weeks) {
        return new Duration(weeks, Unit.WEEK);
    }

    /**
     * Create duration with months
     *
     * @param months the number of months
     * @return Duration in the unit of month
     */
    public static Duration ofMonths(int months) {
        return new Duration(months, Unit.MONTH);
    }

    @Override
    public BanyandbDatabase.Duration serialize() {
        return BanyandbDatabase.Duration.newBuilder()
                .setVal(this.val)
                .setUnit(this.unit.getDurationUnit())
                .build();
    }

    @RequiredArgsConstructor
    public enum Unit {
        HOUR(BanyandbDatabase.Duration.DurationUnit.DURATION_UNIT_HOUR),
        DAY(BanyandbDatabase.Duration.DurationUnit.DURATION_UNIT_DAY),
        WEEK(BanyandbDatabase.Duration.DurationUnit.DURATION_UNIT_WEEK),
        MONTH(BanyandbDatabase.Duration.DurationUnit.DURATION_UNIT_MONTH);

        @Getter(AccessLevel.PRIVATE)
        private final BanyandbDatabase.Duration.DurationUnit durationUnit;
    }

    public static Duration fromProtobuf(BanyandbDatabase.Duration duration) {
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
