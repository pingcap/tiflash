# TiFlash Resource Control
## Introduction
This document introduces the design and implementation of TiFlash's resource management feature.

## Motivation and Background
TiDB currently supports global resource management to support multiple businesses in a cluster and achieve isolation among them.

As part of the TiDB architecture, TiFlash also needs to support resource management to achieve the capability of isolating different businesses, especially in AP/HTAP scenarios.

## Detailed Design
The basic idea is similar to TiDB/TiKV's resource control. It adopts a combination of rate limiting and priority scheduling:
1. Rate Limiting: When scheduling tasks, it limits them based on the Token Bucket algorithm.
2. Priority: When physical resources are scarce, tasks are scheduled based on their priority, determining which task should run first.

The underlying idea behind this design is as follows: If the `RU_PER_SEC` setting for each resource group is relatively small(a.k.a. the sum of these settings does not exceed the real physical resource limit), then rate limiting plays the primary role in throttling each resource group. However, to ensure that resources are used more efficiently and to prevent situations where a small `RU_PER_SEC` setting leads to unused physical resources, TiDB's resource management mechanism allows oversubscription, meaning that the total `RU_PER_SEC` values can exceed the physical resources. In such cases, priority scheduling comes into play to prevent resource contention among resource groups.

### Basic Concepts
1. **Pipeline Execution Engine:** A new execution model inspired by [Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age](https://dl.acm.org/doi/10.1145/2588555.2610507), providing a more refined task scheduling model.
2. **Pipeline task:** The smallest execution unit in the Pipeline execution model.
3. **TaskScheduler:** Responsible for scheduling Pipeline tasks.
4. **TaskQueue:** Responsible for storing all Pipeline tasks in a queue, from which TaskScheduler retrieves tasks and executes them.
5. **LocalAdmissionController (LAC):** As part of the distributed token bucket algorithm, it retrieves tokens from GAC and performs flow control.
6. **GlobalAdmissionController (GAC):** As the central node of the distributed token bucket algorithm, it controls token distribution to various LACs.
7. **Resource group:** A [resource group](https://github.com/pingcap/tidb/blob/master/docs/design/2022-11-25-global-resource-control.md#detailed-design) corresponds to a set of business collections, and each LAC has its token bucket associated with it.

### Overall Architecture
The overall design consists of the following two parts:

1. **ResourceControlQueue:** This includes the main flow control and priority scheduling logic.
2. **LocalAdmissionController:** Used for communication with [GAC](https://github.com/pingcap/tidb/blob/master/docs/design/2022-11-25-global-resource-control.md#global-quota-control--global-admission-control) to obtain tokens.

#### ResourceControlQueue
##### Task Retrieval
To support resource control, a new type of TaskQueue, called ResourceControlQueue, has been introduced. The core logic for flow control and priority scheduling is encapsulated in this TaskQueue.

The core data structure in ResourceControlQueue is a priority queue, where each element represents a ResourceGroupInfo. This priority queue is sorted by the priority of resource groups.

When TaskScheduler retrieves a task to execute from the ResourceControlQueue, it gives priority to resource groups with higher priorities. Additionally, to achieve flow control, if the tokens for the corresponding resource group have been depleted, it will wait until tokens are refilled.

ResourceControlQueue uses a MultiLevelFeedbackQueue to store all pipeline tasks for a specific resource group. After obtaining the MultiLevelFeedbackQueue corresponding to a specific resource group based on priority strategy, calling its take() method can fetch the specific task.

##### Updating Resource Usage
After TaskScheduler obtains a task, it will execute for 100ms and update its CPU usage status. During this time, it will call relevant interfaces of the LocalAdmissionController (LAC) to update token consumption.

### LocalAdmissionController (LAC)
LAC is responsible for managing the metadata of all resource groups on a TiFlash node, including:
1. Recording the priorities, configurations, and TokenBuckets of all currently known resource groups.
2. Watching GAC etcd, updating resource group configurations and detecting resource group deletions.(The creation of the resource group does not rely on the watching etcd mechanism. Instead, it is automatically created after the first query of that resource group is dispatched to TiFlash.)
3. Communicating with GAC to periodically obtain tokens.

## Test Design
### ResourceControlQueue Test
1. Test when Resource Units (RU) are sufficient but physical resources are insufficient, the scheduling between different resource groups generally adheres to priority scheduling.
2. Test when RU is insufficient but physical resources are sufficient, the proportion of physical resource usage between different resource groups is roughly equal to the `RU_PER_SEC` ratio.
3. Test when RU and physical resources alternately become insufficient, the physical resource usage between different resource groups still meets expectations, meaning that the resource group with a higher `RU_PER_SEC` uses more physical resources.

### LAC Test
1. Create two resource groups (rg1, rg2), both with relatively large `RU_PER_SEC`, e.g., 3000, run a tpch workload, observe CPU usage, and query execution times. Expect that the query execution times of the two resource groups are roughly the same.
2. Change the `RU_PER_SEC` of rg1 to 1000, observe CPU usage, and query execution times. Expect reduced CPU usage and increased query execution times for rg1.
3. Change the `RU_PER_SEC` of rg2 to 1000, observe CPU usage, and query execution times. Expect reduced CPU usage and increased query execution times for rg2.
4. Set the burstable attribute of rg1 to true, observe CPU usage, and query execution times. Expect increased CPU usage and significantly reduced query execution times for rg1.
5. Test that LAC reports accurate RU usage to GAC.
6. Test that the conversion between the three TokenBucket modes works as expected.
7. Test with multiple TiFlash nodes, running the above tests (1/2/3) with tpch workload, expecting similar results.
8. Test repeated deletion and reconstruction of resource groups. Expect queries to fail with "resource group not found" errors.

## Impacts & Risks
1. The introduction of ResourceControlQueue may lead to performance degradation as TaskScheduler's scheduling adds an extra layer of TaskQueue.
2. TiFlash resource control only works when TiDB system variable `tidb_enable_resource_control` and TiFlash config `enable_resource_control` are both true. Otherwise, flow control and priority scheduling will not take effects.

## Unresolved Questions
1. Currently, resource control can only be used when the pipeline execution engine is enabled.
2. Resource control only tracks CPU usage and read bytes, it does not control other resource dimensions. For example, storage size, network IO, etc.
