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

#include <Storages/DeltaMerge/ReadThread/CircularScanList.h>
#include <Storages/DeltaMerge/ReadThread/MergedTask.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

#include <memory>
namespace DB::DM
{
using SegmentReadTaskPoolList = CircularScanList<SegmentReadTaskPool>;

// SegmentReadTaskScheduler is a global singleton.
// All SegmentReadTaskPool will be added to it and be scheduled by it.

// 1. DeltaMergeStore::read/readRaw will call SegmentReadTaskScheduler::add to add a SegmentReadTaskPool object to the `read_pools` list and
// index segments information into `merging_segments`.
// 2. A schedule-thread will scheduling read tasks:
//   a. It scans the read_pools list and choosing a SegmentReadTaskPool.
//   b. Chooses a segment of the SegmentReadTaskPool and build a MergedTask.
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
    void add(const SegmentReadTaskPoolPtr & pool);

    void pushMergedTask(const MergedTaskPtr & p)
    {
        merged_task_pool.push(p);
    }

private:
    SegmentReadTaskScheduler();

    // Choose segment to read.
    // Returns <MergedTaskPtr, run_next_schedule_immediately>
    std::pair<MergedTaskPtr, bool> scheduleMergedTask();

    void setStop();
    bool isStop() const;
    bool schedule();
    void schedLoop();
    bool needScheduleToRead(const SegmentReadTaskPoolPtr & pool);
    SegmentReadTaskPools getPoolsUnlock(const std::vector<uint64_t> & pool_ids);
    // <seg_id, pool_ids>
    std::optional<std::pair<uint64_t, std::vector<uint64_t>>> scheduleSegmentUnlock(const SegmentReadTaskPoolPtr & pool);
    SegmentReadTaskPoolPtr scheduleSegmentReadTaskPoolUnlock();

    std::mutex mtx;
    SegmentReadTaskPoolList read_pools;
    // table_id -> {seg_id -> pool_ids, seg_id -> pool_ids, ...}
    std::unordered_map<int64_t, std::unordered_map<uint64_t, std::vector<uint64_t>>> merging_segments;

    MergedTaskPool merged_task_pool;

    std::atomic<bool> stop;
    std::thread sched_thread;

    LoggerPtr log;
};
} // namespace DB::DM
