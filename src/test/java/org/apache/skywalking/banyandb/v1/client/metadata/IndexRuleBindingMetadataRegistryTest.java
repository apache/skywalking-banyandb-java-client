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

import org.apache.skywalking.banyandb.v1.client.AbstractBanyanDBClientTest;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

public class IndexRuleBindingMetadataRegistryTest extends AbstractBanyanDBClientTest {
    private IndexRuleBindingMetadataRegistry registry;

    @Before
    public void setUp() throws IOException {
        super.setUp();
        this.registry = new IndexRuleBindingMetadataRegistry(this.channel);
    }

    @Test
    public void testIndexRuleBindingRegistry_create() throws BanyanDBException {
        IndexRuleBinding indexRuleBinding = IndexRuleBinding.create(
                "default",
                "sw-index-rule-binding",
                IndexRuleBinding.Subject.referToStream("sw"),
                Arrays.asList("trace_id", "duration", "endpoint_id"),
                ZonedDateTime.now().minusDays(15),
                ZonedDateTime.now().plusYears(100)
        );
        this.registry.create(indexRuleBinding);
        Assert.assertEquals(indexRuleBindingRegistry.size(), 1);
    }

    @Test
    public void testIndexRuleBindingRegistry_createAndGet() throws BanyanDBException {
        IndexRuleBinding indexRuleBinding = IndexRuleBinding.create(
                "default",
                "sw-index-rule-binding",
                IndexRuleBinding.Subject.referToStream("sw"),
                Arrays.asList("trace_id", "duration", "endpoint_id"),
                ZonedDateTime.now().minusDays(15),
                ZonedDateTime.now().plusYears(100)
        );
        this.registry.create(indexRuleBinding);
        IndexRuleBinding getIndexRuleBinding = this.registry.get("default", "sw-index-rule-binding");
        Assert.assertNotNull(getIndexRuleBinding);
        Assert.assertEquals(indexRuleBinding, getIndexRuleBinding);
        Assert.assertNotNull(getIndexRuleBinding.updatedAt());
    }

    @Test
    public void testIndexRuleBindingRegistry_createAndList() throws BanyanDBException {
        IndexRuleBinding indexRuleBinding = IndexRuleBinding.create(
                "default",
                "sw-index-rule-binding",
                IndexRuleBinding.Subject.referToStream("sw"),
                Arrays.asList("trace_id", "duration", "endpoint_id"),
                ZonedDateTime.now().minusDays(15),
                ZonedDateTime.now().plusYears(100)
        );
        this.registry.create(indexRuleBinding);
        List<IndexRuleBinding> listIndexRuleBinding = this.registry.list("default");
        Assert.assertNotNull(listIndexRuleBinding);
        Assert.assertEquals(1, listIndexRuleBinding.size());
        Assert.assertEquals(listIndexRuleBinding.get(0), indexRuleBinding);
    }

    @Test
    public void testIndexRuleBindingRegistry_createAndDelete() throws BanyanDBException {
        IndexRuleBinding indexRuleBinding = IndexRuleBinding.create(
                "default",
                "sw-index-rule-binding",
                IndexRuleBinding.Subject.referToStream("sw"),
                Arrays.asList("trace_id", "duration", "endpoint_id"),
                ZonedDateTime.now().minusDays(15),
                ZonedDateTime.now().plusYears(100)
        );
        this.registry.create(indexRuleBinding);
        boolean deleted = this.registry.delete("default", "sw-index-rule-binding");
        Assert.assertTrue(deleted);
        Assert.assertEquals(0, indexRuleBindingRegistry.size());
    }
}
