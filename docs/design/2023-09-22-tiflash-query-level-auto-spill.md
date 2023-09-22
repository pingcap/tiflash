# TiFlash query level auto spill

- Author: [windtalker](https://github.com/windtalker)

## Introduction

This document introduces the design and implemantion of TiFlash's query level auto spill framework.

## Motivation and Background

TiFlash has already supported operator level auto spill since V7.0.0, some memory intensive opeartors can auto spill if the memory usage exceeds threshold. However, there are some shortcomings for the operator level auto spill

* The memory usage threshold is per operator, so if a query contains multiple memory intensive operator, users have to set multiple threshold for each kind of operator
* Even operator's memory usage threshold is set, there is no way to limit the overall memory usage at query level, and since there will be significant differences between queries, it is hard to set a suitable threshold for all kinds of queries

In practise, a common need from user is that they wants to set a simple threshold to limit the overall memory usage for a query, and the database should auto trigger spill if the memory usage might exceeds the overall limit. It is hard or even not possible with current operator level spill framework. So we need to support automatic spill at query level.

## Detailed Design

In order to support query level auto spill, there are five basic problems to deal with

### How to tracker memory usage at query level

Currently, TiFlash already has a memory tracker framework, but, it tracks memory at the level of `MPPTask`, a query may have multiple `MPPTask`s, so in order to track memory usage at query level, we need to merge the memory tracker of all `MPPTask`s that belong to the same query.
A simplest way to do this is to let all `MPPTask`s use the same memory tracker. In order to do this, we need to put the memory tracker to a common place that can be seen by all the `MPPTask`s. In implementation,  we put the memory tracker in [`MPPQuery`](https://github.com/pingcap/tiflash/blob/ce24115c147293df10a9d9f1c745eb0e81f0f7b5/dbms/src/Flash/Mpp/MPPTaskManager.h#L75) . `MPPQuery` is a structure that contains all the `MPPTask`s for the same query, each `MPPTask` need to register to `MPPQuery`, and during register, it will get memory tracker from `MPPQuery`, so the memory tracker in `MPPQuery` can track all the memory usage for the `MPPQuery`

### How to choose operators to spill

In operator's spill, each operator will check the memory usage itself and trigger spill if the usage exceeds the threshold, but for query level auto spill, this trigger method does not work well because a query may have many operators that support spill, if each opeartor trigger spill by itself, it may end up all the operators will spill data, which is not efficient and may make the query extremely slow. An intuitive way to spill at query level is each time spill the operator that takes up most memories. In order to do this, there are serveral new data structures

* [OperatorSpillContext](https://github.com/pingcap/tiflash/blob/ce24115c147293df10a9d9f1c745eb0e81f0f7b5/dbms/src/Core/OperatorSpillContext.h#L33), it is a structure that contains information about how many memories an operator may release when spill, for each operator that supports spill, it will hold an `OperatorSpillContext`
* [TaskOperatorSpillContexts](https://github.com/pingcap/tiflash/blob/ce24115c147293df10a9d9f1c745eb0e81f0f7b5/dbms/src/Core/TaskOperatorSpillContexts.h#L21), it is a collection of all `OperatorSpillContext`s that belong to a `MPPTask`
* [QueryOperatorSpillContexts](https://github.com/pingcap/tiflash/blob/ce24115c147293df10a9d9f1c745eb0e81f0f7b5/dbms/src/Core/QueryOperatorSpillContexts.h#L23), it is a collection of all `TaskOperatorSpillContexts` that belong to a query

As we can see, `QueryOperatorSpillContexts` has all the information about how many memory each opeartor can release when spill, so in `QueryOperatorSpillContexts`, it is easy to find out the opeartor that takes up most memory and mark it to spill. In current implementation, `QueryOperatorSpillContexts` will first find out the `TaskOperatorSpillContexts` that takes up most memories, and the choosen `TaskOperatorSpillContexts` will trigger spill for the `OperatorSpillContext` that takes up most memories.

### When to trigger auto spill

In operator's spill, spill is triggered when opeartor's memory usage exceeds the threshold, it does not work for query level's spill, because the query level's memory usage threshold is a hard limit, that is to say if the memory usage exceeds the threshold, TiFlash will throw error and abort the query immediately. So for query level auto spill, it has to trigger auto spill check before the memory usage is too large. In the implementation, there is a new variable to control when to trigger auto spill check: [auto_memory_revoke_trigger_threshold](https://github.com/pingcap/tiflash/blob/ce24115c147293df10a9d9f1c745eb0e81f0f7b5/dbms/src/Interpreters/Settings.h#L317). If the memory usage exceeds `auto_memory_revoke_trigger_threshold * query_memory_usage_limit`, TiFlash will trigger auto spill check, and try to mark operator to spill.

### How to trigger auto spill check

In operator's spill, auto spill check is triggered after each calculation of a input block in each opeartor that supports spill. For query level auto spill, the basic idea is almost the same: check auto spill after each calculation of a input block. However, the difference is for query level auto spill, each operator, even if it does not support spill, will consume memory during calculation, so unlike operator's spill, in order to support query level auto spill, auto spill check should be triggered in all operators. In order to avoid adding trigger code inside each operator, the implementation adds this trigger code to the runtime framework

* For pull mode, auto spill check will be triggered after each `read` in [IProfilingBlockInputStream](https://github.com/pingcap/tiflash/blob/ce24115c147293df10a9d9f1c745eb0e81f0f7b5/dbms/src/DataStreams/IProfilingBlockInputStream.cpp#L122)
* For push mode, auto spill check will be triggered after each `read/write/tryOutput/transform` in [Operator](https://github.com/pingcap/tiflash/blob/ce24115c147293df10a9d9f1c745eb0e81f0f7b5/dbms/src/Operators/Operator.h)

### When to spill

There are two possible choices

* Synchronous way: Once auto spill check found out an operator need to spill, it stops current work, and spill synchronously
* Asynchronous way: Seperate spill check and spill data, auto spill trigger only need to mark the opeartor to spill, and for each operator, it need to check the spill flag and spill data if needed

The synchronous way can release memory as soon as possible, but it requires that spill can be triggered at any time by any other threads, this will need a lot of synchronization mechanism, which will cause performance degradation and will greatly increase the probability of deadlock during runtime, so current implementation chooses the async way. 
