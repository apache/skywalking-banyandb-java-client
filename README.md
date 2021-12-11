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

## Schema Management

### Stream

Then we may define a stream with customized configurations. The following example uses `SegmentRecord` in SkyWalking OAP
as an illustration,

```java
// build a stream default(group)/sw(name) with 2 shards and ttl equals to 30 days
Stream s = new Stream("default", "sw");
s.addTagNameAsEntity("service_id").addTagNameAsEntity("service_instance_id").addTagNameAsEntity("is_error");
// data
TagFamilySpec dataFamily = new TagFamilySpec("data");
dataFamily.addTagSpec(TagFamilySpec.TagSpec.newBinaryTag("data_binary"));
s.addTagFamilySpec(dataFamily);
// searchable
TagFamilySpec searchableFamily = new TagFamilySpec("searchable");
searchableFamily.addTagSpec(TagFamilySpec.TagSpec.newStringTag("trace_id"))
    .addTagSpec(TagFamilySpec.TagSpec.newIntTag("is_error"))
    .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_id"));
s.addTagFamilySpec(searchableFamily);
registry.create(s);
```

For the last line in the code block, a simple API (i.e. `BanyanDBClient.define(Stream)`) is used to define the schema of `Stream`.
The same works for `Measure` which will be demonstrated later.

### IndexRules

For better search performance, index rules are necessary for `Stream` and `Measure`. You have to
specify a full list of index rules that would be bounded to the target `Stream` and `Measure`.

```java
// create IndexRule with inverted index type and save it to series store
IndexRule indexRule = new IndexRule("default", "db.instance", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES);
// tag name specifies the indexed tag
indexRule.addTag("db.instance");
// create the index rule default(group)/db.instance(name)
registry.create(indexRule);
```

For convenience, `BanyanDBClient.defineIndexRule` supports binding multiple index rules with a single call.
Internally, an `IndexRuleBinding` is created automatically for users, which will be active between `beginAt` and `expireAt`.
With this shorthand API, the indexRuleBinding object will be active from the time it is created to the far future (i.e. `2099-01-01 00:00:00.000 UTC`).

### Measure

`Measure` can also be defined directly with `BanyanDBClient`,

```java
// create a new measure schema with an additional interval
// the interval is used to specify how frequently to send a data point
Measure m = new Measure("sw_metric", "service_cpm_minute", Duration.ofHours(1));
// set entity
m.addTagNameAsEntity("entity_id");
// define tag specs - add a default tag family
TagFamilySpec defaultFamily = new TagFamilySpec("default");
defaultFamily.addTagSpec(TagFamilySpec.TagSpec.newIDTag("id"));
defaultFamily.addTagSpec(TagFamilySpec.TagSpec.newStringTag("entity_id"));
m.addTagFamilySpec(defaultFamily);
// define field specs
// compressMethod and encodingMethod can be specified
m.addFieldSpec(Measure.FieldSpec.newIntField("total").compressWithZSTD().encodeWithGorilla().build())
    .addFieldSpec(Measure.FieldSpec.newIntField("value").compressWithZSTD().encodeWithGorilla().build());
// define a measure, as we've mentioned above
registry.create(m);
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
query.appendCondition(PairQueryCondition.LongQueryCondition.eq("searchable", "state" , 0L));
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
query.appendCondition(PairQueryCondition.StringQueryCondition.eq("default", "service_id", "abc"));
// send the query request
client.query(query);
```

After response is returned, `dataPoints` can be extracted,

```java
MeasureQueryResponse resp = client.query(query);
List<DataPoint> dataPointList = resp.getDataPoints();
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
StreamWrite streamWrite = new StreamWrite("default", "sw", segmentId, now.toEpochMilli(), 1, 13)
    .dataTag(0, Value.binaryTagValue(byteData))
    .searchableTag(0, Value.stringTagValue(traceId)) // 0
    .searchableTag(1, Value.stringTagValue(serviceId))
    .searchableTag(2, Value.stringTagValue(serviceInstanceId))
    .searchableTag(3, Value.stringTagValue(endpointId))
    .searchableTag(4, Value.longTagValue(latency)) // 4
    .searchableTag(5, Value.longTagValue(state))
    .searchableTag(6, Value.stringTagValue(httpStatusCode))
    .searchableTag(7, Value.nullTagValue()) // 7
    .searchableTag(8, Value.stringTagValue(dbType))
    .searchableTag(9, Value.stringTagValue(dbInstance))
    .searchableTag(10, Value.stringTagValue(broker))
    .searchableTag(11, Value.stringTagValue(topic))
    .searchableTag(12, Value.stringTagValue(queue)); // 12

streamBulkWriteProcessor.add(streamWrite);
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
MeasureWrite measureWrite = new MeasureWrite("sw_metrics", "service_cpm_minute", now.toEpochMilli(), 2, 2);
measureWrite.defaultTag(0, Value.idTagValue("1"))
    .defaultTag(1, Value.stringTagValue("entity_1"))
    .field(0, Value.longFieldValue(100L))
    .field(1, Value.longFieldValue(1L));

measureBulkWriteProcessor.add(measureWrite);
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