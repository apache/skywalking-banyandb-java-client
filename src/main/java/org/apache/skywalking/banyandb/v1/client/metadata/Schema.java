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

import com.google.protobuf.GeneratedMessageV3;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.skywalking.banyandb.v1.Banyandb;

import java.time.ZonedDateTime;

/**
 * Schema is a kind of metadata registered in the BanyanDB.
 *
 * @param <P> In BanyanDB, we have Stream, IndexRule, IndexRuleBinding and Measure
 */
@Getter
@EqualsAndHashCode
public abstract class Schema<P extends GeneratedMessageV3> {
    /**
     * name of the Schema
     */
    protected final String name;

    /**
     * last updated timestamp
     * This field can only be set by the server.
     */
    @EqualsAndHashCode.Exclude
    protected final ZonedDateTime updatedAt;

    protected Schema(String name, ZonedDateTime updatedAt) {
        this.name = name;
        this.updatedAt = updatedAt;
    }

    public abstract P serialize(String group);

    protected Banyandb.Metadata buildMetadata(String group) {
        return Banyandb.Metadata.newBuilder().setName(this.name).setGroup(group).build();
    }
}
