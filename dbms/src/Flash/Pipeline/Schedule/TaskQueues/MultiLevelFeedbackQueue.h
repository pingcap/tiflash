// Copyright 2023 PingCAP, Ltd.
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
#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueue.h>

#include <array>
#include <atomic>
#include <deque>
#include <mutex>

namespace DB
{
struct UnitQueueInfo
{
    UnitQueueInfo(UInt64 time_slice_, double factor_for_normal_)
        : time_slice(time_slice_)
        , factor_for_normal(factor_for_normal_)
    {
        assert(time_slice > 0);
        assert(factor_for_normal > 0);
    }

    UInt64 time_slice;

    // factor for normalization
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

    bool empty();

    double accuTimeAfterDivisor();

public:
    const UnitQueueInfo info;
    std::atomic_uint64_t accu_consume_time{0};

private:
    std::deque<TaskPtr> task_queue;
};
using UnitQueuePtr = std::unique_ptr<UnitQueue>;

template <typename TimeGetter>
class MultiLevelFeedbackQueue : public TaskQueue
{
public:
    MultiLevelFeedbackQueue();

    void submit(TaskPtr && task) noexcept override;

    void submit(std::vector<TaskPtr> & tasks) noexcept override;

    bool take(TaskPtr & task) noexcept override;

    void updateStatistics(const TaskPtr & task, size_t inc_value) noexcept override;

    bool empty() noexcept override;

    void close() noexcept override;

    const UnitQueueInfo & getUnitQueueInfo(size_t level);

public:
    static constexpr size_t QUEUE_SIZE = 8;

    // The time slice of the i-th level is (i+1)*LEVEL_TIME_SLICE_BASE ns,
    // so when a task's execution time exceeds 0.2s, 0.6s, 1.2s, 2s, 3s, 4.2s, 5.6s, and 7.2s,
    // it will move to next level.
    static constexpr int64_t LEVEL_TIME_SLICE_BASE_NS = 200'000'000L;

    static constexpr double RATIO_OF_ADJACENT_QUEUE = 1.2;

private:
    void computeQueueLevel(const TaskPtr & task);

private:
    std::mutex mu;
    std::condition_variable cv;
    bool is_closed = false;

    LoggerPtr logger = Logger::get("MultiLevelFeedbackQueue");

    // from high level to low level.
    std::array<UnitQueuePtr, QUEUE_SIZE> level_queues;
};

struct CPUTimeGetter
{
    static UInt64 get(const TaskPtr & task)
    {
        assert(task);
        return task->profile_info.cpu_execute_time;
    }
};
using CPUMultiLevelFeedbackQueue = MultiLevelFeedbackQueue<CPUTimeGetter>;

struct IOTimeGetter
{
    static UInt64 get(const TaskPtr & task)
    {
        assert(task);
        return task->profile_info.io_execute_time;
    }
};
using IOMultiLevelFeedbackQueue = MultiLevelFeedbackQueue<IOTimeGetter>;
} // namespace DB
