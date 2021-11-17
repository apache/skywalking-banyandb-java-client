BanyanDB Java Client
==========

<img src="http://skywalking.apache.org/assets/logo.svg" alt="Sky Walking logo" height="90px" align="right" />

The client implement for SkyWalking BanyanDB in Java.

[![GitHub stars](https://img.shields.io/github/stars/apache/skywalking.svg?style=for-the-badge&label=Stars&logo=github)](https://github.com/apache/skywalking)
[![Twitter Follow](https://img.shields.io/twitter/follow/asfskywalking.svg?style=for-the-badge&label=Follow&logo=twitter)](https://twitter.com/AsfSkyWalking)

[![CI/IT Tests](https://github.com/apache/skywalking-banyandb-java-client/workflows/CI%20AND%20IT/badge.svg?branch=main)](https://github.com/apache/skywalking-banyandb-java-client/actions?query=workflow%3ACI%2BAND%2BIT+event%3Aschedule+branch%main)

# Usage

## Create a client

Create a `BanyanDBClient` with host, port and a user-specified group and then establish a connection.

```java
// use `default` group
client = new BanyanDBClient("127.0.0.1", 17912, "default");
// to send any request, a connection to the server must be estabilished
client.connect(channel);
```

## Schema Management

### Stream

Then we may define a stream with customized configurations,

```java
// build a stream "sw" with 2 shards and ttl equals to 30 days
Stream s = new Stream("sw", 2, Duration.ofDays(30));
s.addTagNameAsEntity("service_id").addTagNameAsEntity("service_instance_id").addTagNameAsEntity("state");
// TagFamily - tagFamily1
TagFamilySpec tf1 = new TagFamilySpec("tagFamily1"); // tagFamily1 as the name of the tag family
tf1.addTagSpec(TagFamilySpec.TagSpec.newBinaryTag("data_binary"));
s.addTagFamilySpec(tf1);
// TagFamily - tagFamily2
TagFamilySpec tf2 = new TagFamilySpec("tagFamily2");
tf2.addTagSpec(TagFamilySpec.TagSpec.newStringTag("trace_id"))
        .addTagSpec(TagFamilySpec.TagSpec.newIntTag("state"))
        .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_id"));
s.addTagFamilySpec(tf2);
// create with the stream schema, client is the BanyanDBClient created above
s = client.define(s);
```

For the last line in the code block, a simple API (i.e. `BanyanDBClient.define(Stream)`) is used to define the schema of `Stream`.
The same works for `Measure` which will be demonstrated later.

### IndexRules

For better search performance, index rules are necessary for `Stream` and `Measure`. You have to
specify a full list of index rules that would be bounded to the target `Stream` and `Measure`.

```java
// create IndexRule with inverted index type and save it to series store
IndexRule indexRule = new IndexRule("db.instance", IndexRule.IndexType.INVERTED, IndexRule.IndexLocation.SERIES);
// tag name specifies the indexed tag
indexRule.addTag("db.instance");
// create the index rule "db.instance"
client.defineIndexRules(stream, indexRule);
```

For convenience, `BanyanDBClient.defineIndexRule` supports binding multiple index rules with a single call.
Internally, an `IndexRuleBinding` is created automatically for users, which will be active between `beginAt` and `expireAt`.
With this shorthand API, the indexRuleBinding object will be active from the time it is created to the far future (i.e. `2099-01-01 00:00:00.000 UTC`).

### Measure

`Measure` can also be defined directly with `BanyanDBClient`,

```java
// create a measure registry
// create a new measure schema with 2 shards and ttl 30 days.
Measure m = new Measure("measure-example", 2, Duration.ofDays(30));
// set entity
m.addTagNameAsEntity("service_id").addTagNameAsEntity("service_instance_id").addTagNameAsEntity("state");
// TagFamily - tagFamilyName
TagFamilySpec tf = new TagFamilySpec("tagFamilyName");
tf.addTagSpec(TagFamilySpec.TagSpec.newStringTag("trace_id"))
    .addTagSpec(TagFamilySpec.TagSpec.newIntTag("state"))
    .addTagSpec(TagFamilySpec.TagSpec.newStringTag("service_id"));
s.addTagFamilySpec(tf);
// set interval rules for different scopes
m.addIntervalRule(Measure.IntervalRule.matchStringLabel("scope", "day", "1d"));
m.addIntervalRule(Measure.IntervalRule.matchStringLabel("scope", "hour", "1h"));
m.addIntervalRule(Measure.IntervalRule.matchStringLabel("scope", "minute", "1m"));
// add field spec
// compressMethod and encodingMethod can be specified
m.addFieldSpec(Measure.FieldSpec.newIntField("tps").compressWithZSTD().encodeWithGorilla().build());
// define a measure, as we've mentioned above
m = client.define(m);
```

For more APIs usage, refer to test cases and API docs.

## Query

Construct a `StreamQuery` instance with given time-range and other conditions.

> Note: time-range is left-inclusive and right-exclusive.

For example, 

```java
// [begin, end) = [ now - 15min, now )
Instant end = Instant.now();
Instant begin = end.minus(15, ChronoUnit.MINUTES);
// with stream schema, group=default, name=sw
StreamQuery query = new StreamQuery("sw",
        new TimestampRange(begin.toEpochMilli(), end.toEpochMilli()),
        // projection tags
        Arrays.asList("state", "start_time", "duration", "trace_id"));
// search for all states
query.appendCondition(PairQueryCondition.LongQueryCondition.eq("searchable", "state" , 0L));
// set order by condition
query.setOrderBy(new StreamQuery.OrderBy("duration", StreamQuery.OrderBy.Type.DESC));
// send the query request
client.queryStreams(query);
```

After response is returned, `elements` can be fetched,

```java
StreamQueryResponse resp = client.queryStreams(query);
List<RowEntity> entities = resp.getElements();
```

where `RowEntity` contains `elementId`, `timestamp` and tag families requested.

The `StreamQueryResponse`, `RowEntity`, `TagFamily` and `Tag` (i.e. `TagAndValue`) forms a hierarchical structure, where
the order of the tag families and containing tags, i.e. indexes of these objects in the List, follow the order specified 
in the projection condition we've used in the request.

## Write

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
StreamWrite streamWrite = StreamWrite.builder()
    .elementId(segmentId)
    .binary(byteData)
    .timestamp(now.toEpochMilli())
    .name("sw")
    .tag(Tag.stringField(traceId))
    .tag(Tag.stringField(serviceId))
    .tag(Tag.stringField(serviceInstanceId))
    .tag(Tag.stringField(endpointId))
    .tag(Tag.longField(latency))
    .tag(Tag.longField(state))
    .tag(Tag.stringField(httpStatusCode))
    .tag(Tag.nullField())
    .tag(Tag.stringField(dbType))
    .tag(Tag.stringField(dbInstance))
    .tag(Tag.stringField(broker))
    .tag(Tag.stringField(topic))
    .tag(Tag.stringField(queue))
    .build();

streamBulkWriteProcessor.add(streamWrite);
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