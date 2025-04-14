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

import com.google.common.base.Strings;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ITBanyanDBCommonTests extends BanyanDBClientTestCI {
    @Before
    public void setUp() throws IOException, BanyanDBException, InterruptedException {
        super.setUpConnection();
    }

    @After
    public void tearDown() throws IOException {
        this.closeClient();
    }

    @Test
    public void test_GetAPIVersion() throws BanyanDBException {
        BanyandbCommon.APIVersion version = this.client.getAPIVersion();
        Assert.assertEquals("0.9", version.getVersion());
        Assert.assertFalse(Strings.isNullOrEmpty(version.getRevision()));
    }

}
