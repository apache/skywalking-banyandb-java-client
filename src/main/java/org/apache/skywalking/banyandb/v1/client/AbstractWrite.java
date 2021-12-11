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

import com.google.protobuf.Timestamp;
import lombok.Getter;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;

public abstract class AbstractWrite<P extends com.google.protobuf.GeneratedMessageV3> {
    @Getter
    protected final String group;
    /**
     * Owner name of current entity
     */
    @Getter
    protected final String name;
    /**
     * Timestamp represents the time of current stream
     * in the timeunit of milliseconds.
     */
    @Getter
    protected final long timestamp;

    public AbstractWrite(String group, String name, long timestamp) {
        this.group = group;
        this.name = name;
        this.timestamp = timestamp;
    }

    P build() {
        BanyandbCommon.Metadata metadata = BanyandbCommon.Metadata.newBuilder()
                .setGroup(this.group).setName(this.name).build();
        Timestamp ts = Timestamp.newBuilder()
                .setSeconds(timestamp / 1000)
                .setNanos((int) (timestamp % 1000 * 1_000_000)).build();
        return build(metadata, ts);
    }

    protected abstract P build(BanyandbCommon.Metadata metadata, Timestamp ts);
}
