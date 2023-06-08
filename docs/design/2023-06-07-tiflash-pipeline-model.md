# TiFlash pipeline model design doc

* Author(s): [SeaRise](https://github.com/SeaRise), [ywqzzy](https://github.com/ywqzzy)
* Tracking Issue: <https://github.com/pingcap/tiflash/issues/6518>

## Table of Contents

* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Impacts & Risks](#impacts-risks)

## Motivation or Background

Currently, TiFlash's parallel execution model is a thread scheduling execution model, where each query will independently apply for several threads to perform collaborative execution.
The thread scheduling model has two problems: 
- In high-concurrency scenarios, too many threads will cause many context switches and incur high thread scheduling costs. When the number of threads reaches a certain number, the application of threads will report an error `thread constructor failed: Resource temporarily unavailable`. 
- The thread scheduling model cannot accurately measure the resource usage of the query or perform fine-grained resource control.

Although TiFlash has introduced several features to reduce the impact of high concurrency on the thread scheduling model, such as DynamicThreadPool, Async GRPC, and MinTsoScheduler, we can still improve the existing parallel execution model to better adapt to high-concurrency scenarios and support future resource control functions.

Referring to [Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age](https://15721.courses.cs.cmu.edu/spring2016/papers/p743-leis.pdf), we introduce a new concurrent execution model pipeline model and use a more efficient task scheduling mechanism.

## Detailed Design

![pipeline_model_overview](./images/2023-06-07-tiflash-pipeline-model.png)

### Generate pipeline dag

The plan tree sent to the query is divided into several pipelines according to the pipeline breaker and then assembled into a directed acyclic graph based on the dependency relationship. The definition of pipeline and pipeline breaker can refer to [Efficiently Compiling Efficient Query Plans for Modern Hardware](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf).

After the query is split into a pipeline dag, it will be submitted to the task scheduler for execution in sequence according to the DAG relationship.

### Schedule and execute task

The pipeline will be instantiated into several tasks according to the query concurrency and then executed in a fixed-sized thread pool.
```
┌────────────────────────────┐
│      task scheduler        │
│                            │
│    ┌───────────────────┐   │
│ ┌──┤io task thread pool◄─┐ │
│ │  └──────▲──┬─────────┘ │ │
│ │         │  │           │ │
│ │ ┌───────┴──▼─────────┐ │ │
│ │ │cpu task thread pool│ │ │
│ │ └───────▲──┬─────────┘ │ │
│ │         │  │           │ │
│ │    ┌────┴──▼────┐      │ │
│ └────►wait reactor├──────┘ │
│      └────────────┘        │
│                            │
└────────────────────────────┘
```
The task will dynamically switch the component in the task scheduler where it is executed on based on the different execution logic.

#### CPU Task Thread Pool

CPU Task Thread Pool performs CPU-intensive calculation logic in operators, such as executing filters, projections, join probes, etc. Usually, most of the logic in an operator is CPU-intensive calculation logic.

#### IO Task Thread Pool

IO Task Thread Pool performs IO-intensive calculation logic in operators, such as executing spill, restore, read from storage, etc.

#### Wait Reactor

Wait Reactor executes the waiting logic in operators, such as 
- Exchange receiver/sender waiting for network layer packet transmission and reception
- Waiting for storage read thread to pass blocks to the computing layer

## Impacts & Risks

Currently, the pipeline model does not support disk-based join and disaggregated mode with S3.
