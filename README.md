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
// establish a connection
client.connect(channel);
```

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
        // projection tags which are indexed
        Arrays.asList("state", "start_time", "duration", "trace_id"));
// search for all states
query.appendCondition(PairQueryCondition.LongQueryCondition.eq("searchable", "state" , 0L));
// set order by condition
query.setOrderBy(new StreamQuery.OrderBy("duration", StreamQuery.OrderBy.Type.DESC));
// set projection for un-indexed tags
query.setDataProjections(ImmutableList.of("data_binary"));
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
    // write binary data to "data" tag family
    .dataTag(Tag.binaryField(byteData))
    .timestamp(now.toEpochMilli())
    .name("sw")
    // write indexed tags to "searchable" tag family
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