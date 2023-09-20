# TiFlash Resource Control
## Introduction
本文介绍 TiFlash 的资源管理功能的设计与实现

## Motivation and Backgroud
TiDB 目前支持全局资源管理，用于在一个集群中支撑多个业务，并达到各个业务之间相互隔离的效果。

TiFlash 作为 TiDB 架构的一部分，也需要支持资源管理，这样针对 AP/HTAP 场景，也能达到隔离不同业务的能力

## Detailed Design
基本思想类似 TiDB/TiKV 的 resource control. 采用流控+优先级调度的方式:
1. 流控: 对 task 进行调度时, 根据 TokenBucket 算法进行限流
2. 优先级: 当物理资源不足时, 根据 task 的优先级进行调度, 决定先运行哪个 task

这个设计背后的思想是: 如果各个 resource group 的 `RU_PER_SEC` 设置比较小, 其总和没有超过真实的物理资源上限, 则流控发挥主要作用, 对各个 resource group 进行限速. 而为了让资源更有效的得到利用，防止因为 `RU_PER_SEC` 设置过小导致有空闲的物理资源没法利用, TiDB 资源管理机制允许超卖, 即 `RU_PER_SEC` 的值超过物理资源, 这种情况下, 就需要优先级调度发挥作用, 防止各个 resource group 相互争抢资源。

为了能更充分地利用物理资源, resource control 机制允许超卖, 即允许 `RU_PER_SEC` 总和超过物理资源上限. 这样当有的 resource group 没有查询时, 其他 resource group 就可以更充分地利用资源, 而不会导致有空闲的物理资源被白白浪费.

但是当所有 resource group 都使用时开始使用时, 各个 resource group 的查询会开始争抢资源, 为了处理这种情况, 引入了优先级机制, 即按照 user_priority + `RU_PER_SEC` 来规定优先级, 优先执行优先级高的任务.

### 基本概念
1. Pipeline Execution Engine: 借鉴 [Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age](https://dl.acm.org/doi/10.1145/2588555.2610507) 实现的新的执行模型, 提供了更加精细的任务调度模型
2. pipeline task: Pipeline 执行模型的最小执行单元
3. TaskScheduler: 负责调度 Pipeline Task
4. TaskQueue: 负责保存所有 Pipeline Task 的队列, TaskScheduler 会从中取 task 并执行
4. LocalAdmissionController(LAC): 作为分布式令牌桶算法的一部分, 会从 GAC 获取 token 并进行流控
5. GlobalAdmissionController(GAC): 作为分布式令牌桶算法的中心节点, 控制各个 LAC 的 token 发放
6. resource group: 一个 [resource group](https://github.com/pingcap/tidb/blob/master/docs/design/2022-11-25-global-resource-control.md#detailed-design) 对应一组业务集合, 在每个 LAC 上都会对应各自的令牌桶

### 整体架构
整体设计包括如下两部分:
1. ResourceControlQueue: 包含主要的流控以及优先级调度逻辑
2. LocalAdmissionController: 用于与 [GAC](https://github.com/pingcap/tidb/blob/master/docs/design/2022-11-25-global-resource-control.md#global-quota-control--global-admission-control) 通信，获取 token

### ResourceControlQueue
#### task 的获取
为了支持 resource control, 新增了一种 TaskQueue, 即 ResourceControlQueue. 流控以及优先级调度的核心逻辑封装在该 TaskQueue 中.

ResourceControlQueue 中的核心数据结构是一个优先队列, 队列中每个元素代表一个 ResourceGroupInfo, 该优先队列以 resource group 的优先级排序. 

TaskScheduler 从 ResourceControlQueue 中获取 task 执行时, 会优先拿优先级高的 resource group . 同时为了达到流控的效果, 如果该 resource group 对应的 token 已经消耗完, 则会等待直到 token 重新填充.

ResourceControlQueue 中使用 MultiLevelFeedbackQueue 保存某个 resource group 的所有 pipeline task. 根据优先级策略获取到特定 resource group 对应的 MultiLevelFeedbackQueue 后, 调用其 take() 方法就可以拿到具体的 task.

#### resource usage 的更新
TaskScheduler 拿到 task 后, 会执行 100ms 并更新其 cpu 使用情况. 此时会调用 LAC 相关接口负责更新 token 消耗.

### LocalAdmissionController(LAC)
LAC 负责管理某个 TiFlash 节点上所有 resource group 的元信息, 具体包括:
1. 记录所有目前已知的 resource group 的优先级, 配置, TokenBucket
2. 负责 watch GAC etcd, 即使更新 resource group 配置, 感知 resource group 的删除
3. 同 GAC 通信, 定期获取 token

## Test Design
### ResourceControlQueue Test
1. 测试 RU 充足但是物理资源不足时, 不同 resource group 之间的调度基本符合优先级调度
2. 测试 RU 不足但是物理资源充足时, 不同 resource group 之间的物理资源用量比例基本等于 `RU_PER_SEC` 比例
3. 测试 RU 和物理资源交替不足时, 不同 resource group 之间物理资源的用量依然符合预期, 即 `RU_PER_SEC` 大的物理资源使用更多

### LAC Test
1. 创建两个 resource group(rg1, rg2), 都设置比较大的 `RU_PER_SEC` , 例如 3000, 跑 tpch workload, 观察 CPU 使用情况以及查询耗时. 预期两个 rg 的查询耗时基本一致.
2. 将 rg1 `RU_PER_SEC` 改为 1000, 观察 CPU 用量以及查询耗时, 预期 CPU 用量减少且 rg1 查询耗时增加
3. 将 rg2 `RU_PER_SEC` 改为 1000, 观察 CPU 用量以及查询耗时, 预期 CPU 用量减少且 rg2 查询耗时增加, 且与 rg1 耗时基本一致
4. 将 rg1 burstable 设置为 true, 观察 CPU 用量以及查询耗时, 预期 CPU 用量增加且 rg1 查询耗时降低很多
5. 测试 LAC 汇报给 GAC 的 RU 用量是准确的
6. 测试 TokenBucket 的三种模式的相互转换是符合预期的:
7. 测试多个 TiFlash 节点情况, 使用 tpch 跑上述 1/2/3 测试同样符合预期
8. 测试反复删除, 重建 resource group. 预期查询会报错 resource group not found 

## Impacts & Risks
1. ResourceControlQueue 的引入可能会导致性能回退. 因为TaskScheduler 的调度多了一层 TaskQueue

## Unresolved Questions
1. 目前 resource control 只有在开启 pipeline execution engine 时才能使用
2. resource control 只追踪了 cpu 用量, 没有根据对其他维度的资源进行控制. 例如 read bytes, storage size, network io 等
