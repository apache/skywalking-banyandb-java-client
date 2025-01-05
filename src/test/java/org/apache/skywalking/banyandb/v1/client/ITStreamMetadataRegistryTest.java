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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon.Metadata;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.Entity;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.Stream;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TagFamilySpec;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TagSpec;
import org.apache.skywalking.banyandb.database.v1.BanyandbDatabase.TagType;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ITStreamMetadataRegistryTest extends BanyanDBClientTestCI {
    @Before
    public void setUp() throws IOException, BanyanDBException, InterruptedException {
        super.setUpConnection();
        BanyandbCommon.Group expectedGroup = buildStreamGroup();
        client.define(expectedGroup);
        Assert.assertNotNull(expectedGroup);
    }

    @After
    public void tearDown() throws IOException {
        this.closeClient();
    }

    @Test
    public void testStreamRegistry_createAndGet() throws BanyanDBException {
        Stream expectedStream = buildStream();
        this.client.define(expectedStream);
        Stream actualStream = client.findStream("sw_record", "trace");
        Assert.assertNotNull(actualStream);
        Assert.assertNotNull(actualStream.getUpdatedAt());
        actualStream = actualStream.toBuilder().clearUpdatedAt().setMetadata(actualStream.getMetadata().toBuilder().clearModRevision().clearCreateRevision()).build();
        Assert.assertEquals(expectedStream, actualStream);
    }

    @Test
    public void testStreamRegistry_createAndUpdate() throws BanyanDBException {
        Stream expectedStream = buildStream();
        this.client.define(expectedStream);
        Stream beforeStream = client.findStream("sw_record", "trace");
        Assert.assertNotNull(beforeStream);
        Assert.assertNotNull(beforeStream.getUpdatedAt());
        Stream updatedStream = beforeStream.toBuilder().addTagFamilies(TagFamilySpec.newBuilder()
                .setName("ex")
                .addTags(TagSpec.newBuilder()
                        .setName("ex")
                        .setType(TagType.TAG_TYPE_INT))).build();
        this.client.update(updatedStream);
        Stream afterStream = client.findStream("sw_record", "trace");
        Assert.assertNotNull(afterStream);
        Assert.assertNotNull(afterStream.getUpdatedAt());
        Assert.assertNotEquals(beforeStream, afterStream);
    }

    @Test
    public void testStreamRegistry_createAndList() throws BanyanDBException {
        Stream expectedStream = buildStream();
        client.define(expectedStream);
        List<Stream> actualStreams = client.findStreams("sw_record");
        Assert.assertNotNull(actualStreams);
        Assert.assertEquals(1, actualStreams.size());
        Stream actualStream = actualStreams.get(0);
        actualStream = actualStream.toBuilder().clearUpdatedAt().setMetadata(actualStream.getMetadata().toBuilder().clearModRevision().clearCreateRevision()).build();
        Assert.assertEquals(expectedStream, actualStream);
    }

    @Test
    public void testStreamRegistry_createAndDelete() throws BanyanDBException {
        Stream expectedStream = buildStream();
        this.client.define(expectedStream);
        boolean deleted = this.client.deleteStream(expectedStream.getMetadata().getGroup(), expectedStream.getMetadata().getName());
        Assert.assertTrue(deleted);
        Assert.assertNull(client.findMeasure(expectedStream.getMetadata().getGroup(), expectedStream.getMetadata().getName()));
    }

    private Stream buildStream() {
        Stream.Builder builder = Stream.newBuilder()
                                       .setMetadata(Metadata.newBuilder()
                                                            .setGroup("sw_record")
                                                            .setName("trace"))
                                       .setEntity(Entity.newBuilder().addAllTagNames(
                                           Arrays.asList("service_id", "service_instance_id", "is_error")))
                                       .addTagFamilies(TagFamilySpec.newBuilder()
                                                                    .setName("data")
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("data_binary")
                                                                                    .setType(TagType.TAG_TYPE_DATA_BINARY)))
                                       .addTagFamilies(TagFamilySpec.newBuilder()
                                                                    .setName("searchable")
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("trace_id")
                                                                                    .setType(TagType.TAG_TYPE_STRING))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("is_error")
                                                                                    .setType(TagType.TAG_TYPE_INT))
                                                                    .addTags(TagSpec.newBuilder()
                                                                                    .setName("service_id")
                                                                                    .setType(TagType.TAG_TYPE_STRING)
                                                                                    .setIndexedOnly(true)));
        return builder.build();
    }
}
