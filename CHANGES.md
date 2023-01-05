Changes by Version
==================
Release Notes.

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
