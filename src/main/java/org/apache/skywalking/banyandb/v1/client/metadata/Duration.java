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

import com.google.common.base.Strings;
import lombok.EqualsAndHashCode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@EqualsAndHashCode
public class Duration {
    private static final Pattern DURATION_PATTERN =
            Pattern.compile("(((?<year>[0-9]+)y)?((?<week>[0-9]+)w)?((?<day>[0-9]+)d)?((?<hour>[0-9]+)h)?((?<minute>[0-9]+)m)?|0)");
    private static final long MINUTES_PER_HOUR = 60;
    private static final long MINUTES_PER_DAY = MINUTES_PER_HOUR * 24;
    private static final long MINUTES_PER_WEEK = MINUTES_PER_DAY * 7;
    private static final long MINUTES_PER_YEAR = MINUTES_PER_DAY * 365;

    @EqualsAndHashCode.Exclude
    private volatile String text;
    private final long minutes;

    private Duration(long minutes) {
        this.minutes = minutes;
    }

    public String format() {
        if (!Strings.isNullOrEmpty(text)) {
            return text;
        }

        final StringBuilder builder = new StringBuilder();
        long minutes = this.minutes;
        if (minutes >= MINUTES_PER_YEAR) {
            long years = minutes / MINUTES_PER_YEAR;
            builder.append(years).append("y");
            minutes = minutes % MINUTES_PER_YEAR;
        }
        if (minutes >= MINUTES_PER_WEEK) {
            long weeks = minutes / MINUTES_PER_WEEK;
            builder.append(weeks).append("w");
            minutes = minutes % MINUTES_PER_WEEK;
        }
        if (minutes >= MINUTES_PER_DAY) {
            long weeks = minutes / MINUTES_PER_DAY;
            builder.append(weeks).append("d");
            minutes = minutes % MINUTES_PER_DAY;
        }
        if (minutes >= MINUTES_PER_HOUR) {
            long weeks = minutes / MINUTES_PER_HOUR;
            builder.append(weeks).append("h");
            minutes = minutes % MINUTES_PER_HOUR;
        }
        if (minutes > 0) {
            builder.append(minutes).append("m");
        }
        this.text = builder.toString();
        return this.text;
    }

    public Duration add(Duration duration) {
        return new Duration(this.minutes + duration.minutes);
    }

    public static Duration parse(String text) {
        if (Strings.isNullOrEmpty(text)) {
            return new Duration(0);
        }
        Matcher matcher = DURATION_PATTERN.matcher(text);
        if (!matcher.find()) {
            return new Duration(0);
        }
        long total = 0;
        final String years = matcher.group("year");
        if (!Strings.isNullOrEmpty(years)) {
            total += Long.parseLong(years) * MINUTES_PER_YEAR;
        }
        final String weeks = matcher.group("week");
        if (!Strings.isNullOrEmpty(weeks)) {
            total += Long.parseLong(weeks) * MINUTES_PER_WEEK;
        }
        final String days = matcher.group("day");
        if (!Strings.isNullOrEmpty(days)) {
            total += Long.parseLong(days) * MINUTES_PER_DAY;
        }
        final String hours = matcher.group("hour");
        if (!Strings.isNullOrEmpty(hours)) {
            total += Long.parseLong(hours) * MINUTES_PER_HOUR;
        }
        final String minutes = matcher.group("minute");
        if (!Strings.isNullOrEmpty(minutes)) {
            total += Long.parseLong(minutes);
        }
        return new Duration(total);
    }

    public static Duration ofMinutes(long minutes) {
        return new Duration(minutes);
    }

    public static Duration ofHours(long hours) {
        return new Duration(hours * MINUTES_PER_HOUR);
    }

    public static Duration ofDays(long days) {
        return ofHours(days * MINUTES_PER_DAY);
    }

    public static Duration ofWeeks(long weeks) {
        return ofHours(weeks * MINUTES_PER_WEEK);
    }

    public static Duration ofYears(long years) {
        return ofHours(years * MINUTES_PER_YEAR);
    }
}
