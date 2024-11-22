BanyanDB Java Client
==========

<img src="http://skywalking.apache.org/assets/logo.svg" alt="Sky Walking logo" height="90px" align="right" />

The client implement for SkyWalking BanyanDB in Java.

[![GitHub stars](https://img.shields.io/github/stars/apache/skywalking.svg?style=for-the-badge&label=Stars&logo=github)](https://github.com/apache/skywalking)
[![Twitter Follow](https://img.shields.io/twitter/follow/asfskywalking.svg?style=for-the-badge&label=Follow&logo=twitter)](https://twitter.com/AsfSkyWalking)

[![CI/IT Tests](https://github.com/apache/skywalking-banyandb-java-client/workflows/CI%20AND%20IT/badge.svg?branch=main)](https://github.com/apache/skywalking-banyandb-java-client/actions?query=workflow%3ACI%2BAND%2BIT+event%3Aschedule+branch%main)

# Usage

## Create a client

Create a `BanyanDBClient` with the server's several addresses and then use `connect()` to establish a connection.

```java
// use `default` group
BanyanDBClient client = new BanyanDBClient("banyandb.svc:17912", "10.0.12.9:17912");
// to send any request, a connection to the server must be estabilished
client.connect();
```

These addresses are either IP addresses or DNS names. 

The client will try to connect to the server in a round-robin manner. The client will periodically refresh the server
addresses. The refresh interval can be configured by `refreshInterval` option.

Besides, you may pass a customized options while building a `BanyanDBClient`. Supported
options are listed below,


| Option                     | Description                                                          | Default                  |
|----------------------------|----------------------------------------------------------------------|--------------------------|
| maxInboundMessageSize      | Max inbound message size                                             | 1024 * 1024 * 50 (~50MB) |
| deadline                   | Threshold of gRPC blocking query, unit is second                     | 30 (seconds)             |
| refreshInterval            | Refresh interval for the gRPC channel, unit is second                | 30 (seconds)             |
| resolveDNSInterval         | DNS resolve interval, unit is second                                 | 30 (minutes)             |
| forceReconnectionThreshold | Threshold of force gRPC reconnection if network issue is encountered | 1                        |
| forceTLS                   | Force use TLS for gRPC                                               | false                    |
| sslTrustCAPath             | SSL: Trusted CA Path                                                 |                          |

## Schema Management

### Stream and index rules

#### Define a Group
```java
// build a group sw_record for Stream with 2 shards and ttl equals to 3 days
Group g = Group.newBuilder().setMetadata(Metadata.newBuilder().setName("sw_record"))
            .setCatalog(Catalog.CATALOG_STREAM)
            .setResourceOpts(ResourceOpts.newBuilder()
                                         .setShardNum(2)
                                         .setSegmentInterval(
                                             IntervalRule.newBuilder()
                                                         .setUnit(
                                                             IntervalRule.Unit.UNIT_DAY)
                                                         .setNum(
                                                             1))
                                         .setTtl(
                                             IntervalRule.newBuilder()
                                                         .setUnit(
                                                             IntervalRule.Unit.UNIT_DAY)
                                                         .setNum(
                                                             3)))
            .build();
client.define(g);
```

Then we may define a stream with customized configurations.

#### Define a Stream
```java
// build a stream trace with above group
Stream s = Stream.newBuilder()
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
                                                              .setIndexedOnly(true)))
                 .build();
client.define(s);
```

#### Define a IndexRules
```java
IndexRule.Builder ir = IndexRule.newBuilder()
                                     .setMetadata(Metadata.newBuilder()
                                                          .setGroup("sw_record")
                                                          .setName("trace_id"))
                                     .addTags("trace_id")
                                     .setType(IndexRule.Type.TYPE_INVERTED)
                                     .setAnalyzer("simple");
client.define(ir.build());
```

#### Define a IndexRuleBinding
```java
IndexRuleBinding.Builder irb = IndexRuleBinding.newBuilder()
                                                   .setMetadata(BanyandbCommon.Metadata.newBuilder()
                                                                                       .setGroup("sw_record")
                                                                                       .setName("trace_binding"))
                                                   .setSubject(BanyandbDatabase.Subject.newBuilder()
                                                                                       .setCatalog(
                                                                                           BanyandbCommon.Catalog.CATALOG_STREAM)
                                                                                       .setName("trace"))
                                                   .addAllRules(
                                                       Arrays.asList("trace_id"))
                                                   .setBeginAt(TimeUtils.buildTimestamp(ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)))
                                                   .setExpireAt(TimeUtils.buildTimestamp(DEFAULT_EXPIRE_AT));
client.define(irb.build());
```

For the last line in the code block, a simple API (i.e. `BanyanDBClient.define(Stream)`) is used to define the schema of `Stream`.
The same works for `Measure` which will be demonstrated later.

### Measure and index rules

`Measure` can also be defined directly with `BanyanDBClient`,

#### Define a Group
```java
// build a group sw_metrics for Measure with 2 shards and ttl equals to 7 days
Group g = Group.newBuilder().setMetadata(Metadata.newBuilder().setName("sw_metric"))
            .setCatalog(Catalog.CATALOG_MEASURE)
            .setResourceOpts(ResourceOpts.newBuilder()
                                         .setShardNum(2)
                                         .setSegmentInterval(
                                             IntervalRule.newBuilder()
                                                         .setUnit(
                                                             IntervalRule.Unit.UNIT_DAY)
                                                         .setNum(
                                                             1))
                                         .setTtl(
                                             IntervalRule.newBuilder()
                                                         .setUnit(
                                                             IntervalRule.Unit.UNIT_DAY)
                                                         .setNum(
                                                             7)))
            .build();
client.define(g);
```

#### Define a Measure
```java
// create a new measure schema with an additional interval
// the interval is used to specify how frequently to send a data point
Measure m = Measure.newBuilder()
                   .setMetadata(Metadata.newBuilder()
                                        .setGroup("sw_metric")
                                        .setName("service_cpm_minute"))
                   .setInterval(Duration.ofMinutes(1).format())
                   .setEntity(Entity.newBuilder().addTagNames("entity_id"))
                   .addTagFamilies(
                       TagFamilySpec.newBuilder()
                                    .setName("default")
                                    .addTags(
                                        TagSpec.newBuilder()
                                               .setName("entity_id")
                                               .setType(
                                                   TagType.TAG_TYPE_STRING))
                                    .addTags(
                                        TagSpec.newBuilder()
                                               .setName("scope")
                                               .setType(
                                                   TagType.TAG_TYPE_STRING)))
                   .addFields(
                       FieldSpec.newBuilder()
                                .setName("total")
                                .setFieldType(
                                    FieldType.FIELD_TYPE_INT)
                                .setCompressionMethod(
                                    CompressionMethod.COMPRESSION_METHOD_ZSTD)
                                .setEncodingMethod(
                                    EncodingMethod.ENCODING_METHOD_GORILLA))
                   .addFields(
                       FieldSpec.newBuilder()
                                .setName("value")
                                .setFieldType(
                                    FieldType.FIELD_TYPE_INT)
                                .setCompressionMethod(
                                    CompressionMethod.COMPRESSION_METHOD_ZSTD)
                                .setEncodingMethod(
                                    EncodingMethod.ENCODING_METHOD_GORILLA))
                   .build();
// define a measure, as we've mentioned above
client.define(m);
```

If you want to create an `index_mode` `Measure`:

```java
Measure m = Measure.newBuilder()
                   .setMetadata(Metadata.newBuilder()
                                        .setGroup("sw_metric")
                                        .setName("service_traffic"))
                   .setEntity(Entity.newBuilder().addTagNames("service_id"))
                   .setIndexMode(true)
                   .addTagFamilies(
                       TagFamilySpec.newBuilder()
                                    .setName("default")
                                    .addTags(
                                        TagSpec.newBuilder()
                                               .setName("service_id")
                                               .setType(
                                                   TagType.TAG_TYPE_STRING))
                                    .addTags(
                                        TagSpec.newBuilder()
                                               .setName("layer")
                                               .setType(
                                                   TagType.TAG_TYPE_STRING)))
                    .build();
// define a "index_mode" measure, as we've mentioned above
client.define(m);
```

For more APIs usage, refer to test cases and API docs.

## Query

### Stream

Construct a `StreamQuery` instance with given time-range and other conditions.

> Note: time-range is left-inclusive and right-exclusive.

For example, 

```java
// [begin, end) = [ now - 15min, now )
Instant end = Instant.now();
Instant begin = end.minus(15, ChronoUnit.MINUTES);
// with stream schema, group=default, name=sw
StreamQuery query = new StreamQuery(Lists.newArrayList("sw_record"), "trace",
        new TimestampRange(begin.toEpochMilli(), end.toEpochMilli()),
        // projection tags which are indexed
        ImmutableSet.of("state", "start_time", "duration", "trace_id"));
// search for all states
query.and(PairQueryCondition.StringQueryCondition.eq("searchable", "trace_id" , "1a60e0846817447eac4cd498eefd3743.1.17261060724190003"));
// set order by condition
query.setOrderBy(new AbstractQuery.OrderBy(AbstractQuery.Sort.DESC));
// set projection for un-indexed tags
query.setDataProjections(ImmutableSet.of("data_binary"));
// send the query request
client.query(query);
```

After response is returned, `elements` can be fetched,

```java
StreamQueryResponse resp = client.query(query);
List<RowEntity> entities = resp.getElements();
```

Every item `RowEntity` in the list contains `elementId`, `timestamp` and tag families requested.

The `StreamQueryResponse`, `RowEntity`, `TagFamily` and `Tag` (i.e. `TagAndValue`) forms a hierarchical structure, where
the order of the tag families and containing tags, i.e. indexes of these objects in the List, follow the order specified 
in the projection condition we've used in the request.

If you want to trace the query, you can use `query.enableTrace()` to get the trace spans.

```java
// enable trace
query.enableTrace();
// send the query request
client.query(query);
```

After response is returned, `trace` can be extracted,

```java
// send the query request
StreamQueryResponse resp = client.queryStreams(query);
Trace t = resp.getTrace();
```

### Measure

For `Measure`, it is similar to the `Stream`,

```java
// [begin, end) = [ now - 15min, now )
Instant end = Instant.now();
Instant begin = end.minus(15, ChronoUnit.MINUTES);
// with stream schema, group=sw_metrics, name=service_instance_cpm_day
MeasureQuery query = new MeasureQuery(Lists.newArrayList("sw_metrics"), "service_instance_cpm_day",
    new TimestampRange(begin.toEpochMilli(), end.toEpochMilli()),
    ImmutableSet.of("id", "scope", "service_id"),
    ImmutableSet.of("total"));
// query max "total" with group by tag "service_id"
query.maxBy("total", ImmutableSet.of("service_id"));
// use conditions
query.and(PairQueryCondition.StringQueryCondition.eq("default", "service_id", "abc"));
// send the query request
client.query(query);
```

After response is returned, `dataPoints` can be extracted,

```java
MeasureQueryResponse resp = client.query(query);
List<DataPoint> dataPointList = resp.getDataPoints();
```

Measure API supports `TopN`/`BottomN` search. The results or (grouped-)results are
ordered by the given `field`,

```java
MeasureQuery query = new MeasureQuery("sw_metrics", "service_instance_cpm_day",
        new TimestampRange(begin.toEpochMilli(), end.toEpochMilli()),
        ImmutableSet.of("id", "scope", "service_id"),
        ImmutableSet.of("total"));
query.topN(5, "total"); // bottomN
```

Besides, `limit` and `offset` are used to support pagination. `Tag`-based sort can also 
be done to the final results,

```java
query.limit(5);
query.offset(1);
query.orderBy("service_id", Sort.DESC);
```

If you want to trace the query, you can use `query.enableTrace()` to get the trace spans.

```java
// enable trace
query.enableTrace();
// send the query request
client.query(query);
```

After response is returned, `trace` can be extracted,

```java
// send the query request
MeasureQueryResponse resp = client.query(query);
Trace trace = resp.getTrace();
```
### Criteria

Both `StreamQuery` and `MeausreQuery` support the `criteria` flag to filter data.
`criteria` supports logical expressions and binary condition operations.

#### Example of `criteria`

The expression `(a=1 and b = 2) or (a=4 and b=5)` could use below operations to support.

```java
query.criteria(Or.create(
                And.create(
                        PairQueryCondition.LongQueryCondition.eq("a", 1L),
                        PairQueryCondition.LongQueryCondition.eq("b", 1L)),
                And.create(
                        PairQueryCondition.LongQueryCondition.eq("a", 4L),
                        PairQueryCondition.LongQueryCondition.eq("b", 5L)
                )
        ));
```
The execution order of conditions is from the inside to outside. The deepest condition
will get executed first.

The client also provides syntactic sugar for using `and` or `or` methods.
The `criteria` method has a higher priority, overwriting these sugar methods.

> Caveat: Sugar methods CAN NOT handle nested query. `criteria` is the canonical
> method to take such tasks as above example shows.

#### Example of `and`

When filtering data matches all the conditions, the query can append several `and`:
```java
query.and(PairQueryCondition.LongQueryCondition.eq("state", 1L))
        .and(PairQueryCondition.StringQueryCondition.eq("service_id", serviceId))
        .and(PairQueryCondition.StringQueryCondition.eq("service_instance_id", serviceInstanceId))
        .and(PairQueryCondition.StringQueryCondition.match("endpoint_id", endpointId))
        .and(PairQueryCondition.LongQueryCondition.ge("duration", minDuration))
        .and(PairQueryCondition.LongQueryCondition.le("duration", maxDuration))
```

#### Example of `or`

When gathering all data matches any of the conditions, the query can combine a series of `or`:

```java
segmentIds.forEach(id -> query.or(PairQueryCondition.LongQueryCondition.eq("segment_id", id)))
```

## Write

### Stream

Since grpc bidi streaming is used for write protocol, build a `StreamBulkWriteProcessor` which would handle back-pressure for you.
Adjust `maxBulkSize`, `flushInterval`, `concurrency` and `timeout` of the consumer in different scenarios to meet requirements.

```java
// build a StreamBulkWriteProcessor from client
StreamBulkWriteProcessor streamBulkWriteProcessor = client.buildStreamWriteProcessor(maxBulkSize, flushInterval, concurrency, timeout);
```

The `StreamBulkWriteProcessor` is thread-safe and thus can be used across threads.
We highly recommend you to reuse it.

The procedure of constructing `StreamWrite` entity must comply with the schema of the stream, e.g.
the order of tags must exactly be the same with that defined in the schema.
And the non-existing tags must be fulfilled (with NullValue) instead of compacting all non-null tag values.

```java
StreamWrite streamWrite = client.createStreamWrite("default", "sw", segmentId, now.toEpochMilli())
    .tag("data_binary", Value.binaryTagValue(byteData))
    .tag("trace_id", Value.stringTagValue(traceId)) // 0
    .tag("state", Value.longTagValue(state)) // 1
    .tag("service_id", Value.stringTagValue(serviceId)) // 2
    .tag("service_instance_id", Value.stringTagValue(serviceInstanceId)) // 3
    .tag("endpoint_id", Value.stringTagValue(endpointId)) // 4
    .tag("duration", Value.longTagValue(latency)) // 5
    .tag("http.method", Value.stringTagValue(null)) // 6
    .tag("status_code", Value.stringTagValue(httpStatusCode)) // 7
    .tag("db.type", Value.stringTagValue(dbType)) // 8
    .tag("db.instance", Value.stringTagValue(dbInstance)) // 9
    .tag("mq.broker", Value.stringTagValue(broker)) // 10
    .tag("mq.topic", Value.stringTagValue(topic)) // 11
    .tag("mq.queue", Value.stringTagValue(queue)); // 12

CompletableFuture<Void> f = streamBulkWriteProcessor.add(streamWrite);
f.get(10, TimeUnit.SECONDS);
```

### Measure

The writing procedure for `Measure` is similar to the above described process and leverages the bidirectional streaming of gRPC,

```java
// build a MeasureBulkWriteProcessor from client
MeasureBulkWriteProcessor measureBulkWriteProcessor = client.buildMeasureWriteProcessor(maxBulkSize, flushInterval, concurrency, timeout);
```

A `BulkWriteProcessor` is created by calling `buildMeasureWriteProcessor`. Then build the `MeasureWrite` object and send with bulk processor,

```java
Instant now = Instant.now();
MeasureWrite measureWrite = client.createMeasureWrite("sw_metric", "service_cpm_minute", now.toEpochMilli());
    measureWrite.tag("id", TagAndValue.stringTagValue("1"))
    .tag("entity_id", TagAndValue.stringTagValue("entity_1"))
    .field("total", TagAndValue.longFieldValue(100))
    .field("value", TagAndValue.longFieldValue(1));

CompletableFuture<Void> f = measureBulkWriteProcessor.add(measureWrite);
f.get(10, TimeUnit.SECONDS);
```

## Property APIs

Property APIs are used to store key-value pairs.

### Apply(Create/Update)

`apply` will always succeed whenever the property exists or not.
The old value will be overwritten if already existed, otherwise a new value will be set.

```java
Property property = Property.create("default", "sw", "ui_template")
    .addTag(TagAndValue.newStringTag("name", "hello"))
    .addTag(TagAndValue.newStringTag("state", "successd"))
    .build();
this.client.apply(property); //created:true tagsNum:2
```

The operation supports updating partial tags.

```java
Property property = Property.create("default", "sw", "ui_template")
    .addTag(TagAndValue.newStringTag("state", "failed"))
    .build();
this.client.apply(property); //created:false tagsNum:1
```

The property supports setting TTL.

```java
Property property = Property.create("default", "sw", "temp_date")
    .addTag(TagAndValue.newStringTag("state", "failed"))
    .ttl("30m")
    .build();
this.client.apply(property); //created:false tagsNum:1 lease_id:1
```
The property's TTL can be extended by calling `Client.keepAliveProperty`,

```java
this.client.keepAliveProperty(1); //lease_id:1
```

The property's live time is reset to 30 minutes.

### Query

Property can be queried via `Client.findProperty`,

```java
Property gotProperty = this.client.findProperty("default", "sw", "ui_template");
```

The query operation could filter tags,

```java
Property gotProperty = this.client.findProperty("default", "sw", "ui_template", "state");
```

### Delete

Property can be deleted by calling `Client.deleteProperty`,

```java
this.client.deleteProperty("default", "sw", "ui_template"); //deleted:true tagsNum:2
```

The delete operation could remove specific tags instead of the whole property.

```java
this.client.deleteProperty("default", "sw", "ui_template", "state"); //deleted:true tagsNum:1
```

# Compiling project
> ./mvnw clean package

# Code of conduct
This project adheres to the Contributor Covenant [code of conduct](https://www.apache.org/foundation/policies/conduct). By participating, you are expected to uphold this code.
Please follow the [REPORTING GUIDELINES](https://www.apache.org/foundation/policies/conduct#reporting-guidelines) to report unacceptable behavior.

# Contact Us
* Mail list: **dev@skywalking.apache.org**. Mail to `dev-subscribe@skywalking.apache.org`, follow the reply to subscribe the mail list.
* Send `Request to join SkyWalking slack` mail to the mail list(`dev@skywalking.apache.org`), we will invite you in.
* Twitter, [ASFSkyWalking](https://twitter.com/ASFSkyWalking)
* QQ Group: 901167865(Recommended), 392443393
* [bilibili B站 视频](https://space.bilibili.com/390683219)

# License
[Apache 2.0 License.](LICENSE)
