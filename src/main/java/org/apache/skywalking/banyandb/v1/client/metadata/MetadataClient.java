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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.GeneratedMessageV3;

import java.util.List;

/**
 * abstract metadata client which defines CRUD operations for a specific kind of schema.
 *
 * @param <P> ProtoBuf: schema defined in ProtoBuf format
 * @param <S> Schema: Java implementation (POJO) which can be serialized to P
 */
public abstract class MetadataClient<P extends GeneratedMessageV3, S extends Schema<P>> {
    protected final String group;

    protected MetadataClient(String group) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group), "group must not be null or empty");
        this.group = group;
    }

    abstract void create(S payload);

    abstract void update(S payload);

    abstract boolean delete(String name);

    abstract S get(String name);

    abstract List<S> list();
}
