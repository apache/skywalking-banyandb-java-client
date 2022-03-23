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
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;

@RequiredArgsConstructor
public enum Catalog {
    UNSPECIFIED(BanyandbCommon.Catalog.CATALOG_UNSPECIFIED),
    STREAM(BanyandbCommon.Catalog.CATALOG_STREAM),
    MEASURE(BanyandbCommon.Catalog.CATALOG_MEASURE);

    @Getter(AccessLevel.PACKAGE)
    private final BanyandbCommon.Catalog catalog;
}
