Changes by Version
==================
Release Notes.

0.9.0
------------------

### Features

* Bump up the API to support sharding_key.
* Bump up the API to support version 0.9.
* Support stage query on TopN.

0.8.0
------------------

### Features

* Bump up the API to support the index mode of Measure.
* Bump up the API to support the new property.
* Bump up the API to adopt the status field which is changed to the string type due to the compatibility issue.
* Bump up the API to support getting the API version.
* Bump up the API to support the lifecycle management.

0.7.0
------------------

### Features

* Bump up the API of BanyanDB Server to support the query trace.
* Add trace to response.
* Add ToString annotation to Tag.
* Enhance the BulkWriteProcessor.
* Provide a new method to order data by timestamp.
* Refactor metadata object to original protocol.
* Complemented the Schema management API.
* Enhance the MetadataCache.
* Add more IT tests.
* Remove analyze DNS name to get/refresh IP for create connection.
* Support new Match Query proto.

### Bugs

* Fix MeasureQuery.SumBy to use SUM instead of COUNT
* Add missing FloatFieldValue type in the Measure write operation
* Fix wrong result of the Duration.ofDay
* Remove duplicate orderBy method in measure query.

0.7.0-rc4
------------------

### Features

* Remove analyze DNS name to get/refresh IP for create connection.
* Support new Match Query proto.

0.7.0-rc3
------------------

### Features

* Refactor metadata object to original protocol.
* Complemented the Schema management API.
* Enhance the MetadataCache.
* Add more IT tests.

### Bugs

0.7.0-rc2
------------------

### Features

* Provide a new method to order data by timestamp.

### Bugs
* Remove duplicate orderBy method in measure query.

0.7.0-rc1
------------------

### Features

* Add trace to response.
* Add ToString annotation to Tag.
* Enhance the BulkWriteProcessor.

### Bugs

0.7.0-rc0
------------------

### Features

* Bump up the API of BanyanDB Server to support the query trace.

### Bugs

* Fix MeasureQuery.SumBy to use SUM instead of COUNT
* Add missing FloatFieldValue type in the Measure write operation
* Fix wrong result of the Duration.ofDay

0.6.0
------------------

### Features

* Support JDK21 build. Upgrade lombok version to 1.18.30.
* Bump up the API of BanyanDB Server.

### Bugs

* Fix the number of channel and the channel size in the BulkWriteProcessor.


0.5.0
------------------

### Features

* Add mod revision check to write requests
* Add TTL to property.
* Support setting multiple server addresses 
* Support DNS name resolution
* Support round-robin load balancing

0.4.0
------------------

### Features
* Support new TopN query protocol
* Remove ID type of TAG
* Make the global singleton MetadataCache client-local
* Add createStreamWrite API to allow late timestamp set

0.3.1
------------------

### Features
* Tweak Group builders for property operations by @hanahmily in https://github.com/apache/skywalking-banyandb-java-client/pull/36


0.3.0
------------------

### Features
* Introduce the Float field type by @hanahmily in https://github.com/apache/skywalking-banyandb-java-client/pull/34

0.3.0-rc1
------------------

### Features
* Remove Measure ID by @hanahmily in https://github.com/apache/skywalking-banyandb-java-client/pull/30
* Drop indexRuleBinding for empty indexRules by @hanahmily in https://github.com/apache/skywalking-banyandb-java-client/pull/31


0.3.0-rc0
------------------

### Features

* Support in and notIn for stringArray by @lujiajing1126 in https://github.com/apache/skywalking-banyandb-java-client/pull/27
* Drop invalid entries on writing. by @hanahmily in https://github.com/apache/skywalking-banyandb-java-client/pull/29


0.2.0
------------------

### Features

- Support `indexed_only` flag to the tag specification.
- Support `Analyzer` to the index rule.
- Add `exist` endpoints to the metadata registry.
- Set `CompletableFuture<Void>` to the return type of write processor.
- Refactor property operations.

### Bugs

- Fix UTs failures with JDK16,17(https://github.com/apache/skywalking/issues/9771)


0.1.0
------------------

### Features

- Support Measure, Stream and Property Query and Write APIs
- Support Metadata Management APIs for Measure, Stream, IndexRule and IndexRuleBinding

### Chores

- Set up GitHub actions to check code styles, licenses, and tests.
