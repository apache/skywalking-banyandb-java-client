Changes by Version
==================
Release Notes.

0.4.0
------------------

### Features
* Support new TopN query protocol
* Remove ID type of TAG

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
