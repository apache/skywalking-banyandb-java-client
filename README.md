BanyanDB Java Client
==========

<img src="http://skywalking.apache.org/assets/logo.svg" alt="Sky Walking logo" height="90px" align="right" />

The client implement for SkyWalking BanyanDB in Java.

[![GitHub stars](https://img.shields.io/github/stars/apache/skywalking.svg?style=for-the-badge&label=Stars&logo=github)](https://github.com/apache/skywalking)
[![Twitter Follow](https://img.shields.io/twitter/follow/asfskywalking.svg?style=for-the-badge&label=Follow&logo=twitter)](https://twitter.com/AsfSkyWalking)

[![CI/IT Tests](https://github.com/apache/skywalking-banyandb-java-client/workflows/CI%20AND%20IT/badge.svg?branch=main)](https://github.com/apache/skywalking-banyandb-java-client/actions?query=workflow%3ACI%2BAND%2BIT+event%3Aschedule+branch%main)

# Usage

## Create a client

Create a `BanyanDBClient` with host, port and then use `connect()` to establish a connection.

```java
// use `default` group
client = new BanyanDBClient("127.0.0.1", 17912);
// to send any request, a connection to the server must be estabilished
client.connect();
```

Besides, you may pass a customized options while building a `BanyanDBClient`. Supported
options are listed below,


| Option                     | Description                                                          | Default                  |
|----------------------------|----------------------------------------------------------------------|--------------------------|
| maxInboundMessageSize      | Max inbound message size                                             | 1024 * 1024 * 50 (~50MB) |
| deadline                   | Threshold of gRPC blocking query, unit is second                     | 30 (seconds)             |
| refreshInterval            | Refresh interval for the gRPC channel, unit is second                | 30 (seconds)             |
| forceReconnectionThreshold | Threshold of force gRPC reconnection if network issue is encountered | 1                        |
| forceTLS                   | Force use TLS for gRPC                                               | false                    |
| sslTrustCAPath             | SSL: Trusted CA Path                                                 |                          |
| sslCertChainPath           | SSL: Cert Chain Path                                                 |                          |
| sslKeyPath                 | SSL: Cert Key Path                                                   |                          |

## Schema Management

### Stream and index rules

Then we may define a stream with customized configurations. The following example uses `SegmentRecord` in SkyWalking OAP
as an illustration,

```java
// build a stream default(group)/sw(name) with 2 shards and ttl equals to 30 days
Stream s = Stream.create("default", "sw")
        // set entities
        .setEntityRelativeTags("service_id", "service_instance_id", "is_error")
        // add a tag family "data"
        .addTagFamily(TagFamilySpec.create("data")
            .addTagSpec(TagFamilySpec.TagSpec.newBinaryTag("data_binary"))
            .build())
        // add a tag family "searchable"
        .addTagFamily(TagFamilySpec.create("searchable")
            // create a string tag "trace_id"
            .addTagSpec(TagFamilySpec.TagSpec.newStringTag("trace_id"))
            .addTagSpec(TagFamilySpec.TagSpec.newIntTag("is_error"))
             // service_id is not stored, but can be searched through the index
            .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_id").indexedOnly())
            .build())
        .build();
client.define(s);
```

For the last line in the code block, a simple API (i.e. `BanyanDBClient.define(Stream)`) is used to define the schema of `Stream`.
The same works for `Measure` which will be demonstrated later.

### Measure and index rules

`Measure` can also be defined directly with `BanyanDBClient`,

```java
// create a new measure schema with an additional interval
// the interval is used to specify how frequently to send a data point
Measure m = Measure.create("sw_metric", "service_cpm_minute", Duration.ofHours(1))
        // set entity
        .setEntityRelativeTags("entity_id")
        // define a tag family "default"
        .addTagFamily(TagFamilySpec.create("default")
            .addTagSpec(TagFamilySpec.TagSpec.newIDTag("id"))
            .addTagSpec(TagFamilySpec.TagSpec.newStringTag("entity_id"))
            .build())
        // define field specs
        // compressMethod and encodingMethod can be specified
        .addField(Measure.FieldSpec.newIntField("total").compressWithZSTD().encodeWithGorilla().build())
        .addField(Measure.FieldSpec.newIntField("value").compressWithZSTD().encodeWithGorilla().build())
        .build();
// define a measure, as we've mentioned above
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
StreamQuery query = new StreamQuery("default", "sw",
        new TimestampRange(begin.toEpochMilli(), end.toEpochMilli()),
        // projection tags which are indexed
        ImmutableSet.of("state", "start_time", "duration", "trace_id"));
// search for all states
query.and(PairQueryCondition.LongQueryCondition.eq("searchable", "state" , 0L));
// set order by condition
query.setOrderBy(new StreamQuery.OrderBy("duration", StreamQuery.OrderBy.Type.DESC));
// set projection for un-indexed tags
query.setDataProjections(ImmutableSet.of("data_binary"));
// send the query request
client.query(query);
```

After response is returned, `elements` can be fetched,

```java
StreamQueryResponse resp = client.queryStreams(query);
List<RowEntity> entities = resp.getElements();
```

Every item `RowEntity` in the list contains `elementId`, `timestamp` and tag families requested.

The `StreamQueryResponse`, `RowEntity`, `TagFamily` and `Tag` (i.e. `TagAndValue`) forms a hierarchical structure, where
the order of the tag families and containing tags, i.e. indexes of these objects in the List, follow the order specified 
in the projection condition we've used in the request.

### Measure

For `Measure`, it is similar to the `Stream`,

```java
// [begin, end) = [ now - 15min, now )
Instant end = Instant.now();
Instant begin = end.minus(15, ChronoUnit.MINUTES);
// with stream schema, group=sw_metrics, name=service_instance_cpm_day
MeasureQuery query = new MeasureQuery("sw_metrics", "service_instance_cpm_day",
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

### Criteria

Both `StreamQuery` and `MeausreQuery` support the `criteria` flag to filter data.
`criteria` supports logical expressions and binary condition operations.

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

The client also provides syntactic sugar for using `and` or `or` methods.
The `criteria` method has a higher priority, overwriting these sugar methods.

> Caveat: mixing up `and` and `or` is not a good practice. `criteria` is a better
> method to support complex operations.

```java
query.and(PairQueryCondition.LongQueryCondition.eq("state", 1L))
        .and(PairQueryCondition.StringQueryCondition.eq("service_id", serviceId))
        .and(PairQueryCondition.StringQueryCondition.eq("service_instance_id", serviceInstanceId))
        .and(PairQueryCondition.StringQueryCondition.match("endpoint_id", endpointId))
        .and(PairQueryCondition.LongQueryCondition.ge("duration", minDuration))
        .and(PairQueryCondition.LongQueryCondition.le("duration", maxDuration))
```

```java
segmentIds.forEach(id -> query.or(PairQueryCondition.LongQueryCondition.eq("segment_id", id)))
```

## Write

### Stream

Since grpc bidi streaming is used for write protocol, build a `StreamBulkWriteProcessor` which would handle back-pressure for you.
Adjust `maxBulkSize`, `flushInterval` and `concurrency` of the consumer in different scenarios to meet requirements.

```java
// build a StreamBulkWriteProcessor from client
StreamBulkWriteProcessor streamBulkWriteProcessor = client.buildStreamWriteProcessor(maxBulkSize, flushInterval, concurrency);
```

The `StreamBulkWriteProcessor` is thread-safe and thus can be used across threads.
We highly recommend you to reuse it.

The procedure of constructing `StreamWrite` entity must comply with the schema of the stream, e.g.
the order of tags must exactly be the same with that defined in the schema.
And the non-existing tags must be fulfilled (with NullValue) instead of compacting all non-null tag values.

```java
StreamWrite streamWrite = new StreamWrite("default", "sw", segmentId, now.toEpochMilli())
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
MeasureBulkWriteProcessor bulkWriteProcessor = client.buildMeasureWriteProcessor(maxBulkSize, flushInterval, concurrency);
```

A `BulkWriteProcessor` is created by calling `buildMeasureWriteProcessor`. Then build the `MeasureWrite` object and send with bulk processor,

```java
Instant now = Instant.now();
MeasureWrite measureWrite = new MeasureWrite("sw_metric", "service_cpm_minute", now.toEpochMilli());
    measureWrite.tag("id", TagAndValue.idTagValue("1"))
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