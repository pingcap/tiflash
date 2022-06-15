# TiFlash Design Documents

- Author(s): [guo-shaoge](http://github.com/guo-shaoge)
- Discussion PR: https://github.com/pingcap/tiflash/issues/4631
- Tracking Issue: https://github.com/pingcap/tiflash/issues/4631

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

By retaining the hash attribute of the output result of the operator, the subsequent operators can obtain performance improvements for free.

Taking WindowFunction as an example, after implementing fine-grained partition, the structure of MPPTask changes as follows:

![](.images/2022-06-14-fine-grained-shuffle-WindowFunction.png)

By retaining the hash attribute of the ExchangeSender of the hash partition, fine-grained shuffle is performed in advance,
and the most time-consuming MergeSorting + WindowFunction can be changed from single thread to multi-threaded, making full use of multi-threading capabilities.

HashAgg and HashJoin can also use the same method to improve performance, but this document only describes the implementation of fine-grained Shuffle with WindowFunction.

## Motivation or Background
TiFlash does not make full use of the hash attribute of the operator output, such as HashJoin, HashAgg and the hash partition of the Exchange operator.

For example, WindowFunction can use the hash attribute of ExchangeSender to partition data within nodes in advance and perform multi-threaded processing.
Or HashAgg can use the hash attribute of hash partition ExchangeSender to save the final merge stage.

## Detailed Design

The modification consists of three parts:
1. TiDB.
2. TiFlash Interpreter.
3. TiFlash execution.

### Changes in TiDB
TiDB can see the entire plan tree of SQL, so it's reasonable to check whether we can enable fine-grained shuffle optimization when segmenting MPP fragments.

When all the following conditions are met, a single Fragment can use fine-grained shuffle optimization:
1. If there is a WindowFunction in Fragment, it must have partition by, otherwise fine-grained shuffle cannot be performed.
2. If there is WindowFunction in Fragment, its child must be WindowFunction, Sort or ExchangeReciever: because the current implementation only supports WindowFunction.
3. If there is no WindowFunction in Fragment, it must be the source fragment, i.e., the fragment containing TableScan.

In order to simplify the design, whether fine-grained shuffle can be enabled will be query level,
so if all fragments of a query can use fine-grained shuffle, we can use fine-grained shuffle for this query.

If the query can use fine-grained shuffle, `mpp.DispatchTaskRequest.FineGrainedShuffleStreamCount` will be set to `@@tiflash_fine_grained_shuffle_stream_count`.
When `mpp.DispatchTaskRequest.FineGrainedShuffleStreamCount` is 0, it means fine-grained shuffle is disabled.

### Changes in TiFlash Interpreter
When compiling WindowFunction and WindowSort, if fine-grained shuffle is enabled (fine_grained_shuffle_stream_count is greater than 0),
it will be compiled into a multi-threaded structure, otherwise the original single-threaded structure will be used.

### Changes in TiFlash execution
#### ExchangeSender
Like the original logic, ExchangeSender performs a hash partition every time it receives batch_size rows.

The difference is that the hash partition not only distinguishes which node should send for each row,
but also distinguishes which specific stream within the node should be sent to, as shown in the following figure:

![](images/2022-06-14-fine-grained-shuffle-ExchangeSender.png)

An array will be added in `MPPDataPacket`, which will corresponds to the `MPPDataPacket.chunks` array.
Then `MPPDataPacket.chunks[i]` should be processed by `streams[MPPDataPacket.stream_ids[i]]`.

#### ExchangeReceiver
In the current implementation, ExchangeReceiver has only one MPMCQueue.

If fine-grained shuffle optimization is enabled, there should be multiple MPMCQueues, which correspond to each stream.

## Test Design

Testing includes end-to-end test as well as unit test.

### Functional Tests

- End-to-end random testing for correctness.

### Compatibility Tests

Mainly considers the upgrade and downgrade compatibility of TiFlash:
1. Old version TiDB sends MPPTask to new version TiFlash.
2. New version TiDB sends MPPTask to old version TiFlash.
3. Cluster of TiFlash with mix of old and new versions.

### Benchmark Tests

- Even if fine-grained shuffle is disabled, there should be no performance regression.
- Expect linear performance improvement for E2E performance test and microbenchmark.

## Impacts & Risks

1. The value of `@@tiflash_fine_grained_shuffle_stream_count` and `@@tiflash_fine_grained_shuffle_batch_size` can affect performance.
2. When severe data skew occurs, may have performance regressions if fine-grained shuffle is enabled.

## Investigation & Alternatives

TODO: How do other systems solve this issue? What other designs have been considered and what is the rationale for not choosing them?

## Unresolved Questions
None.
