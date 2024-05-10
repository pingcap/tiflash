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

namespace DB::DM
{
<<<<<<< HEAD

// `SegmentReadTaskScheduler` is a global singleton. All `SegmentReadTaskPool` will be added to it and be scheduled by it.
// 1. `UnorderedInputStream`/`UnorderedSourceOps` will call `SegmentReadTaskScheduler::add` to add a `SegmentReadTaskPool`
// object to the `read_pools` list and index segments information into `merging_segments`.
// 2. A schedule-thread will scheduling read tasks:
//   a. It scans the `read_pools` list and check if `SegmentReadTaskPool` need be scheduled.
//   b. Chooses a `SegmentReadTask` of the `SegmentReadTaskPool`, if other `SegmentReadTaskPool` will read the same
//      `SegmentReadTask`, pop them, and build a `MergedTask`.
//   c. Sends the MergedTask to read threads(SegmentReader).
=======
namespace tests
{
class SegmentReadTasksPoolTest;
}
// SegmentReadTaskScheduler is a global singleton. All SegmentReadTaskPool objects will be added to it and be scheduled by it.
// - Threads of computational layer will call SegmentReadTaskScheduler::add to add a SegmentReadTaskPool object to the `pending_pools`.
// - Call path: UnorderedInputStream/UnorderedSourceOps -> SegmentReadTaskScheduler::add -> SegmentReadTaskScheduler::submitPendingPool
//
// - `sched_thread` will scheduling read tasks.
// - Call path: schedLoop -> schedule -> reapPendingPools -> scheduleOneRound
// - reapPeningPools will swap the `pending_pools` and add these pools to `read_pools` and `merging_segments`.
// - scheduleOneRound will scan `read_pools` and choose segments to read.
>>>>>>> bddd270b16 (Storages: Refine SegmentReadTaskScheduler::add to reduce lock contention (#9027))
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

<<<<<<< HEAD
    // Add SegmentReadTaskPool to `read_pools` and index segments into merging_segments.
=======
    // Add `pool` to `pending_pools`.
>>>>>>> bddd270b16 (Storages: Refine SegmentReadTaskScheduler::add to reduce lock contention (#9027))
    void add(const SegmentReadTaskPoolPtr & pool);

    void pushMergedTask(const MergedTaskPtr & p) { merged_task_pool.push(p); }

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    // `run_sched_thread` is used for test.
    explicit SegmentReadTaskScheduler(bool run_sched_thread = true);

    // Choose segment to read.
    MergedTaskPtr scheduleMergedTask(SegmentReadTaskPoolPtr & pool);

    void setStop();
    bool isStop() const;
    bool schedule();
    void schedLoop();
    bool needScheduleToRead(const SegmentReadTaskPoolPtr & pool);
    SegmentReadTaskPools getPoolsUnlock(const std::vector<uint64_t> & pool_ids);
    // <seg_id, pool_ids>
    std::optional<std::pair<uint64_t, std::vector<uint64_t>>> scheduleSegmentUnlock(
        const SegmentReadTaskPoolPtr & pool);
    SegmentReadTaskPoolPtr scheduleSegmentReadTaskPoolUnlock();
    bool needSchedule(const SegmentReadTaskPoolPtr & pool);

    // `scheduleOneRound()` traverses all pools in `read_pools`, try to schedule `SegmentReadTask` of each pool.
    // It returns summary information for a round of scheduling: <erased_pool_count, sched_null_count, sched_succ_count>
    // `erased_pool_count` - how many stale pools have beed erased.
    // `sched_null_count` - how many pools do not require scheduling.
    // `sched_succ_count` - how many pools is scheduled.
<<<<<<< HEAD
    std::tuple<UInt64, UInt64, UInt64> scheduleOneRound() EXCLUSIVE_LOCKS_REQUIRED(mtx);

    // To restrict the instantaneous concurrency of `add` and avoid `schedule` from always failing to acquire the lock.
    std::mutex add_mtx;
    std::mutex mtx;
    // pool_id -> pool
    std::unordered_map<UInt64, SegmentReadTaskPoolPtr> read_pools;
    // table_id -> {seg_id -> pool_ids, seg_id -> pool_ids, ...}
    std::unordered_map<int64_t, std::unordered_map<uint64_t, std::vector<uint64_t>>> merging_segments;
=======
    std::tuple<UInt64, UInt64, UInt64> scheduleOneRound();
    // `schedule()` calls `scheduleOneRound()` in a loop
    // until there are no tasks to schedule or need to release lock to other tasks.
    bool schedule();
    // `schedLoop()` calls `schedule()` in infinite loop.
    void schedLoop();

    MergedTaskPtr scheduleMergedTask(SegmentReadTaskPoolPtr & pool);
    // Returns <seg_id, pool_ids>.
    std::optional<std::pair<GlobalSegmentID, std::vector<UInt64>>> scheduleSegmentUnlock(
        const SegmentReadTaskPoolPtr & pool);
    SegmentReadTaskPools getPoolsUnlock(const std::vector<uint64_t> & pool_ids);

    void submitPendingPool(SegmentReadTaskPoolPtr pool);
    void reapPendingPools();
    void addPool(const SegmentReadTaskPoolPtr & pool);

    // To restrict the instantaneous concurrency of `add` and avoid `schedule` from always failing to acquire the lock.
    std::mutex add_mtx;

    // `read_pools` and `merging_segment` are only accessed by `sched_thread`.
    // pool_id -> pool
    std::unordered_map<UInt64, SegmentReadTaskPoolPtr> read_pools;
    // GlobalSegmentID -> pool_ids
    MergingSegments merging_segments;
>>>>>>> bddd270b16 (Storages: Refine SegmentReadTaskScheduler::add to reduce lock contention (#9027))

    MergedTaskPool merged_task_pool;

    std::atomic<bool> stop;
    std::thread sched_thread;

    LoggerPtr log;

<<<<<<< HEAD
    // To count how many threads are waitting to add tasks.
    std::atomic<Int64> add_waittings{0};
=======
    std::mutex pending_mtx;
    SegmentReadTaskPools pending_pools GUARDED_BY(pending_mtx);

    friend class tests::SegmentReadTasksPoolTest;
>>>>>>> bddd270b16 (Storages: Refine SegmentReadTaskScheduler::add to reduce lock contention (#9027))
};
} // namespace DB::DM
