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
import com.google.protobuf.GeneratedMessageV3;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.v1.client.util.IgnoreHashEquals;

import javax.annotation.Nullable;
import java.time.ZonedDateTime;

/**
 * NamedSchema is a kind of metadata registered in the BanyanDB.
 *
 * @param <P> In BanyanDB, we have Stream, IndexRule, IndexRuleBinding and Measure
 */
public abstract class NamedSchema<P extends GeneratedMessageV3> {
    /**
     * group of the NamedSchema
     */
    @Nullable
    public abstract String group();

    /**
     * name of the NamedSchema
     */
    public abstract String name();

    /**
     * last updated timestamp
     * This field can only be set by the server.
     */
    @Nullable
    @IgnoreHashEquals
    abstract ZonedDateTime updatedAt();

    public abstract P serialize();

    protected BanyandbCommon.Metadata buildMetadata() {
        if (Strings.isNullOrEmpty(group())) {
            return BanyandbCommon.Metadata.newBuilder().setName(name()).build();
        } else {
            return BanyandbCommon.Metadata.newBuilder().setName(name()).setGroup(group()).build();
        }
    }

    protected BanyandbCommon.Metadata buildMetadata(long modRevision) {
        BanyandbCommon.Metadata.Builder builder =
                BanyandbCommon.Metadata.newBuilder().setName(name()).setModRevision(modRevision);
        if (!Strings.isNullOrEmpty(group())) {
            builder.setGroup(group());
        }
        return builder.build();
    }
}
