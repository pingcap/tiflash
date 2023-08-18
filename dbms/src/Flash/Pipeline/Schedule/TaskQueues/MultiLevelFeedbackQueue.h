// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/Logger.h>
#include <Flash/Pipeline/Schedule/TaskQueues/FIFOQueryIdCache.h>
#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueue.h>

#include <array>
#include <atomic>
#include <deque>
#include <mutex>

namespace DB
{
///    +-----------+       +-----------+       +-----------+            +-----------+
///    |UnitQueue 1|       |UnitQueue 2|       |UnitQueue 3|    ...     |UnitQueue 8|
///    +-----------+       +-----------+       +-----------+            +-----------+
///          ^                   ^                   ^                        ^
///          |                   |                   |                        |
/// +--------+--------+  +-------+--------+  +-------+--------+       +-------+--------+
/// | Task 1          |  | Task 6         |  | Task 11        |       | Task 16        |
/// +-----------------+  +----------------+  +----------------+       +----------------+
///          ^                   ^                   ^                        ^
///          |                   |                   |                        |
/// +--------+--------+  +-------+--------+  +-------+--------+       +-------+--------+
/// | Task 2          |  | Task 7         |  | Task 12        |       | Task 17        |
/// +-----------------+  +----------------+  +----------------+       +----------------+
///          ^                   ^                   ^                        ^
///          |                   |                   |                        |
/// +--------+--------+  +-------+--------+  +-------+--------+       +-------+--------+
/// | Task 3          |  | Task 8         |  | Task 13        |       | Task 18        |
/// +-----------------+  +----------------+  +----------------+       +----------------+
///          ^                   ^                   ^                        ^
///          |                   |                   |                        |
/// +--------+--------+  +-------+--------+  +-------+--------+       +-------+--------+
/// | Task 4          |  | Task 9         |  | Task 14        |       | Task 19        |
/// +-----------------+  +----------------+  +----------------+       +----------------+

struct UnitQueueInfo
{
    UnitQueueInfo(UInt64 time_slice_, double factor_for_normal_)
        : time_slice(time_slice_)
        , factor_for_normal(factor_for_normal_)
    {
        assert(time_slice > 0);
        assert(factor_for_normal > 0);
    }

    // The total duration of tasks executed in the queue must be less than `time_slice`.
    UInt64 time_slice;

    // factor for normalization.
    // The priority value is equal to `accu_consume_time_microsecond / factor_for_normal`.
    // The smaller the value, the higher the priority.
    // Therefore, the higher the priority of the queue, the larger the value of factor_for_normal.
    double factor_for_normal;
};

class UnitQueue
{
public:
    UnitQueue(UInt64 time_slice_, double factor_for_normal_)
        : info(time_slice_, factor_for_normal_)
    {}

    void submit(TaskPtr && task);

    void take(TaskPtr & task);

    bool empty() const;

    double normalizedTimeMicrosecond();

public:
    const UnitQueueInfo info;
    std::atomic_uint64_t accu_consume_time_microsecond{0};

    std::list<TaskPtr> task_queue;
};
using UnitQueuePtr = std::unique_ptr<UnitQueue>;

template <typename TimeGetter>
class MultiLevelFeedbackQueue : public TaskQueue
{
public:
    MultiLevelFeedbackQueue();

    ~MultiLevelFeedbackQueue() override;

    void submit(TaskPtr && task) override;

    void submit(std::vector<TaskPtr> & tasks) override;

    bool take(TaskPtr & task) override;

    void updateStatistics(const TaskPtr & task, ExecTaskStatus, UInt64 inc_ns) override;

    bool empty() const override;

    void finish() override;

    const UnitQueueInfo & getUnitQueueInfo(size_t level);

    void cancel(const String & query_id, const String & resource_group_name) override;

public:
    void collectCancelledTasks(std::deque<TaskPtr> & cancel_queue, const String & query_id);

    static constexpr size_t QUEUE_SIZE = 8;

    // The time slice of the i-th level is (i+1)*LEVEL_TIME_SLICE_BASE ns,
    // so when a task's execution time exceeds 0.2s, 0.6s, 1.2s, 2s, 3s, 4.2s, 5.6s, and 7.2s,
    // it will move to next level.
    static constexpr int64_t LEVEL_TIME_SLICE_BASE_NS = 200'000'000L;

private:
    void computeQueueLevel(const TaskPtr & task);

    void submitTaskWithoutLock(TaskPtr && task);

    void drainTaskQueueWithoutLock();

private:
    mutable std::mutex mu;
    std::condition_variable cv;
    std::atomic_bool is_finished = false;

    // From high priority to low priority.
    // The higher the priority of the queue,
    // the longer the total execution time of all tasks in the queue,
    // and the shorter the execution time of each individual task.
    std::array<UnitQueuePtr, QUEUE_SIZE> level_queues;

    FIFOQueryIdCache cancel_query_id_cache;
    std::deque<TaskPtr> cancel_task_queue;
};

struct CPUTimeGetter
{
    static UInt64 get(const TaskPtr & task)
    {
        assert(task);
        return task->profile_info.getCPUExecuteTimeNs();
    }
};
using CPUMultiLevelFeedbackQueue = MultiLevelFeedbackQueue<CPUTimeGetter>;

struct IOTimeGetter
{
    static UInt64 get(const TaskPtr & task)
    {
        assert(task);
        return task->profile_info.getIOExecuteTimeNs();
    }
};
using IOMultiLevelFeedbackQueue = MultiLevelFeedbackQueue<IOTimeGetter>;

} // namespace DB
