# TiFlash pipeline model design doc

*   Author(s): [SeaRise](https://github.com/SeaRise), [ywqzzy](https://github.com/ywqzzy)
*   Tracking Issue:  <https://github.com/pingcap/tiflash/issues/6518>

## Table of Contents

*   [Motivation or Background](#motivation-or-background)
*   [Detailed Design](#detailed-design)
*   [Impacts & Risks](#impacts-risks)

## Motivation or Background

目前 tiflash 的并行执行模型是线程调度执行模型，每一个查询会独立申请若干条线程协同执行。
线程调度模型存在两个问题
- 在高并发场景下，过多的线程会引起较多上下文切换，导致较高的线程调度代价。甚至在线程数达到一定数量时，申请线程会报错 `thread constructor failed: Resource temporarily unavailable`。
- 线程调度模型无法精准计量查询的资源使用量以及做细粒度的资源管控。

尽管 tiflash 已经引入了若干功能来减少高并发对线程调度模型的影响，如 DynamicThreadPool，Async GRPC 和 MinTsoScheduler，但是我们仍然可以用改进现有的并行执行模型以更好地适应高并发场景以及支持未来的资源管控功能。

参考 ![Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age](https://15721.courses.cs.cmu.edu/spring2016/papers/p743-leis.pdf)，我们引入了新的并发执行模型 pipeline model，使用更高效的任务调度机制。

## Detailed Design

### generate pipeline dag

将发给 query 中的 plan tree 按照 pipeline breaker 为边界划分为若干个 pipeline。pipeline 按照 依赖关系会组成一个有向无环图。
pipeline 和 pipeline breaker 的定义可参考 ![Efficiently Compiling Efficient Query Plans for Modern Hardware](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf)。

在 query 被拆分成 pipeline dag 后，会按照 dag 关系依次调度提交到 task scheduler 执行。

### schedule and execute task

pipeline 会根据 query 并发度被实例化成若干个 task 在固定大小线程池上执行。
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

- cpu task thread pool: for operator cpu intensive compute.
- io task thread pool: for operator io intensive block.
- wait reactor: for polling asynchronous io status, etc.
```

#### CPU/IO Task Thread Pool

#### Wait Reactor

## Impacts & Risks
