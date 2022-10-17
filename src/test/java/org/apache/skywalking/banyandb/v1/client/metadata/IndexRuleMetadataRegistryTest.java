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
import java.util.List;

public class IndexRuleMetadataRegistryTest extends AbstractBanyanDBClientTest {
    private IndexRuleMetadataRegistry registry;

    @Before
    public void setUp() throws IOException {
        super.setUp();
        this.registry = new IndexRuleMetadataRegistry(this.channel);
    }

    @Test
    public void testIndexRuleRegistry_create() throws BanyanDBException {
        IndexRule indexRule = IndexRule.create("default", "db.instance", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES);
        this.registry.create(indexRule);
        Assert.assertEquals(indexRuleRegistry.size(), 1);
    }

    @Test
    public void testIndexRuleRegistry_createAndGet() throws BanyanDBException {
        IndexRule indexRule = IndexRule.create("default", "db.instance", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES);
        this.registry.create(indexRule);
        IndexRule getIndexRule = this.registry.get("default", "db.instance");
        Assert.assertNotNull(getIndexRule);
        Assert.assertEquals(indexRule, getIndexRule);
        Assert.assertNotNull(getIndexRule.updatedAt());
    }

    @Test
    public void testIndexRuleRegistry_createAndList() throws BanyanDBException {
        IndexRule indexRule = IndexRule.create("default", "db.instance", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES);
        this.registry.create(indexRule);
        List<IndexRule> listIndexRule = this.registry.list("default");
        Assert.assertNotNull(listIndexRule);
        Assert.assertEquals(1, listIndexRule.size());
        Assert.assertEquals(listIndexRule.get(0), indexRule);
    }

    @Test
    public void testIndexRuleRegistry_createAndDelete() throws BanyanDBException {
        IndexRule indexRule = IndexRule.create("default", "db.instance", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES);
        this.registry.create(indexRule);
        boolean deleted = this.registry.delete("default", "db.instance");
        Assert.assertTrue(deleted);
        Assert.assertEquals(0, indexRuleRegistry.size());
    }
}
