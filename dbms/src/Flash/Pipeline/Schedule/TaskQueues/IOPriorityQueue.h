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

#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueue.h>

#include <deque>
#include <mutex>

namespace DB
{
/// The queue only used by io thread pool.
/// In IOPriorityQueue, the priority of io_out is higher than io_in, which means the ratio of the total execution time of io_out to io_in is `ratio_of_in_to_out`:1.
/// Because the IO_OUT task usually writes the data in the memory to the external storage and releases the occupied memory,
/// while the IO_IN task usually reads the data from the external storage into the memory and occupies the memory.
/// Prioritizing the execution of IO_OUT tasks can effectively reduce the memory usage.
class IOPriorityQueue : public TaskQueue
{
public:
    // // The ratio of total execution time between io_in and io_out is 3:1.
    static constexpr size_t ratio_of_out_to_in = 3;

    ~IOPriorityQueue() override;

    void submit(TaskPtr && task) override;

    void submit(std::vector<TaskPtr> & tasks) override;

    bool take(TaskPtr & task) override;

    void updateStatistics(const TaskPtr &, ExecTaskStatus exec_task_status, UInt64 inc_ns) override;

    bool empty() const override;

    void finish() override;

private:
    void submitTaskWithoutLock(TaskPtr && task);

private:
    mutable std::mutex mu;
    std::condition_variable cv;
    std::atomic_bool is_finished = false;

    std::deque<TaskPtr> io_in_task_queue;
    std::atomic_uint64_t total_io_in_time_microsecond{0};

    std::deque<TaskPtr> io_out_task_queue;
    std::atomic_uint64_t total_io_out_time_microsecond{0};
};
} // namespace DB
