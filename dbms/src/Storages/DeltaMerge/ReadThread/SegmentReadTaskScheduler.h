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

#include <Storages/DeltaMerge/ReadThread/MergedTask.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

#include <condition_variable>

namespace DB
{
struct Settings;
}

namespace DB::DM
{
namespace tests
{
class SegmentReadTasksPoolTest;
}
// `SegmentReadTaskScheduler` is a global singleton. All `SegmentReadTaskPool` will be added to it and be scheduled by it.
// 1. `UnorderedInputStream`/`UnorderedSourceOps` will call `SegmentReadTaskScheduler::add` to add a `SegmentReadTaskPool`
// object to the `read_pools` list and index segments information into `merging_segments`.
// 2. A schedule-thread will scheduling read tasks:
//   a. It scans the `read_pools` list and check if `SegmentReadTaskPool` need be scheduled.
//   b. Chooses a `SegmentReadTask` of the `SegmentReadTaskPool`, if other `SegmentReadTaskPool` will read the same
//      `SegmentReadTask`, pop them, and build a `MergedTask`.
//   c. Sends the MergedTask to read threads(SegmentReader).
class SegmentReadTaskScheduler
{
public:
    static SegmentReadTaskScheduler & instance()
    {
        static SegmentReadTaskScheduler scheduler;
        return scheduler;
    }

    ~SegmentReadTaskScheduler();
    DISALLOW_COPY_AND_MOVE(SegmentReadTaskScheduler);

    // Add SegmentReadTaskPool to `read_pools` and index segments into merging_segments.
    void add(const SegmentReadTaskPoolPtr & pool) LOCKS_EXCLUDED(add_mtx, mtx);

    void pushMergedTask(const MergedTaskPtr & p) { merged_task_pool.push(p); }

    void updateConfig(const Settings & settings);

    void notify() LOCKS_EXCLUDED(mtx);

private:
    // `run_sched_thread` is used for test.
    explicit SegmentReadTaskScheduler(bool run_sched_thread = true);

    void setStop();
    bool isStop() const;
    bool needScheduleToRead(const SegmentReadTaskPoolPtr & pool);
    bool needSchedule(const SegmentReadTaskPoolPtr & pool);

    // `scheduleOneRound()` traverses all pools in `read_pools`, try to schedule `SegmentReadTask` of each pool.
    // It returns summary information for a round of scheduling: <erased_pool_count, sched_null_count, sched_succ_count>
    // `erased_pool_count` - how many stale pools have beed erased.
    // `sched_null_count` - how many pools do not require scheduling.
    // `sched_succ_count` - how many pools is scheduled.
    std::tuple<UInt64, UInt64, UInt64> scheduleOneRound() EXCLUSIVE_LOCKS_REQUIRED(mtx);
    // `schedule()` calls `scheduleOneRound()` in a loop
    // until there are no tasks to schedule or need to release lock to other tasks.
    bool schedule() LOCKS_EXCLUDED(mtx);
    // `schedLoop()` calls `schedule()` in infinite loop.
    void schedLoop() LOCKS_EXCLUDED(mtx);
    void waitForTimeoutOrNotify() LOCKS_EXCLUDED(mtx);

    MergedTaskPtr scheduleMergedTask(SegmentReadTaskPoolPtr & pool) EXCLUSIVE_LOCKS_REQUIRED(mtx);
    // Returns <seg_id, pool_ids>.
    std::optional<std::pair<GlobalSegmentID, std::vector<UInt64>>> scheduleSegmentUnlock(
        const SegmentReadTaskPoolPtr & pool) EXCLUSIVE_LOCKS_REQUIRED(mtx);
    SegmentReadTaskPools getPoolsUnlock(const std::vector<uint64_t> & pool_ids) EXCLUSIVE_LOCKS_REQUIRED(mtx);

    // To restrict the instantaneous concurrency of `add` and avoid `schedule` from always failing to acquire the lock.
    std::mutex add_mtx ACQUIRED_BEFORE(mtx);

    std::mutex mtx;
    std::condition_variable cv;
    // pool_id -> pool
    std::unordered_map<UInt64, SegmentReadTaskPoolPtr> read_pools GUARDED_BY(mtx);
    // GlobalSegmentID -> pool_ids
    MergingSegments merging_segments GUARDED_BY(mtx);

    MergedTaskPool merged_task_pool;

    std::atomic<bool> stop{false};
    bool enable_data_sharing{true};
    std::thread sched_thread;

    LoggerPtr log;

    // To count how many threads are waitting to add tasks.
    std::atomic<Int64> add_waittings{0};

    friend class tests::SegmentReadTasksPoolTest;
};
} // namespace DB::DM
