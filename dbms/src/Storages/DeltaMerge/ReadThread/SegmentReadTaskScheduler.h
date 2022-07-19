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
    SegmentReadTaskScheduler(const SegmentReadTaskScheduler &) = delete;
    SegmentReadTaskScheduler & operator=(const SegmentReadTaskScheduler &) = delete;
    SegmentReadTaskScheduler(SegmentReadTaskScheduler &&) = delete;
    SegmentReadTaskScheduler & operator=(SegmentReadTaskScheduler &&) = delete;

    // Add SegmentReadTaskPool to `read_pools` and index segments into merging_segments.
    void add(const SegmentReadTaskPoolPtr & pool);

    void pushMergedTask(const MergedTaskPtr & p)
    {
        merged_task_pool.push(p);
    }

private:
    SegmentReadTaskScheduler();

    // Choose segment to read.
    // Returns <MergedTaskPtr, run_next_schedule_immediatly>
    std::pair<MergedTaskPtr, bool> scheduleMergedTask();

    void setStop();
    bool isStop() const;
    bool schedule();
    void schedThread();

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

    Poco::Logger * log;
};
} // namespace DB::DM