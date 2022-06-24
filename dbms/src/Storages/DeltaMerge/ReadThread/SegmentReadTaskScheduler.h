#pragma once

#include <Storages/DeltaMerge/ReadThread/CircularScanList.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB::DM
{

using SegmentReadTaskPoolList = CircularScanList<SegmentReadTaskPool>;

// MergedTask merges the same segment of different SegmentReadTaskPools.
// Read segment input streams of different SegmentReadTaskPools sequentially to improve cache sharing.
class MergedTask
{
public:
    MergedTask(uint64_t seg_id_, SegmentReadTaskPools && pools_)
        : seg_id(seg_id_)
        , pools(std::forward<SegmentReadTaskPools>(pools_))
        , finished_count(0)
        , finished(pools.size(), 0)
    {}

    void init()
    {
        if (!streams.empty())
        {
            return;
        }
        streams.resize(pools.size(), nullptr);
        for (size_t i = 0; i < pools.size(); i++)
        {
            if (pools[i]->expired())
            {
                pools[i].reset();
                setFinished(i);
            }
            else
            {
                streams[i] = pools[i]->getInputStream(seg_id);
            }
        }
    }

    void readOneBlock()
    {
        for (size_t i = 0; i < pools.size(); i++)
        {
            if (isFinished(i))
            {
                continue;
            }

            auto & pool = pools[i];

            if (pool->expired())
            {
                pool.reset();
                setFinished(i);
                continue;
            }

            auto block = streams[i]->read();
            if (!block)
            {
                setFinished(i);
                pool->finishSegment(seg_id);
            }
            else
            {
                pool->pushBlock(std::move(block));
            }
        }
    }

    bool allFinished() const
    {
        return finished_count >= finished.size();
    }

    std::pair<int64_t, int64_t> getMinMaxPendingBlockCount()
    {
        int64_t min = std::numeric_limits<int64_t>::max();
        int64_t max = std::numeric_limits<int64_t>::min();
        for (size_t i = 0; i < pools.size(); i++)
        {
            if (isFinished(i))
            {
                continue;
            }
            if (pools[i]->expired())
            {
                pools[i].reset();
                setFinished(i);
                continue;
            }

            auto pbc = pools[i]->pendingBlockCount();
            min = std::min(min, pbc);
            max = std::max(max, pbc);
        }
        return {min, max};
    }

    uint64_t getSegmentId() const
    {
        return seg_id;
    }

    size_t getPoolCount() const
    {
        return pools.size();
    }

private:
    uint64_t seg_id;
    SegmentReadTaskPools pools;
    BlockInputStreams streams;

    bool isFinished(size_t i)
    {
        return finished[i];
    }
    void setFinished(size_t i)
    {
        if (!isFinished(i))
        {
            finished[i] = 1;
            finished_count++;
        }
    }
    size_t finished_count;
    std::vector<int8_t> finished;
};

using MergedTaskPtr = std::shared_ptr<MergedTask>;

// SegmentReadTaskScheduler is a global singleton.
// All SegmentReadTaskPool will be added to it and be scheduled by it.

// 1. DeltaMergeStore::read will call SegmentReadTaskScheduler::add to add a SegmentReadTaskPool object to `the read_pools list` and
// index segments information into `merging_segments`.
// 2. A schedule-thread will scheduling read tasks:
//   a. It scans the read_pools list and choosing a SegmentReadTaskPool.
//   b. Chooses a segment of the SegmentReadTaskPool in step a, and build a MergedTask.
//   c. Sends the MergedTask to read threads.
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

    void add(const SegmentReadTaskPoolPtr & pool);
    MergedTaskPtr scheduleMergedTask();

private:
    SegmentReadTaskScheduler();
    void setStop();
    bool isStop() const;
    bool schedule();
    void schedThread();

    // `unsafe*` means these functions are not thread-safe.
    SegmentReadTaskPools unsafeGetPools(const std::vector<uint64_t> & pool_ids);
    std::pair<uint64_t, std::vector<uint64_t>> unsafeScheduleSegment(const SegmentReadTaskPoolPtr & pool);
    SegmentReadTaskPoolPtr unsafeScheduleSegmentReadTaskPool();

    std::mutex mtx;
    SegmentReadTaskPoolList read_pools;
    int64_t max_unexpired_pool_count;
    // table_id -> {seg_id -> pool_ids, seg_id -> pool_ids, ...}
    std::unordered_map<int64_t, std::unordered_map<uint64_t, std::vector<uint64_t>>> merging_segments;

    std::atomic<bool> stop;
    std::thread sched_thread;

    Poco::Logger * log;
};

class DMFileReader;
class DMFileReaderPool
{
public:
    static DMFileReaderPool & instance();

    void add(DMFileReader & reader);
    void del(DMFileReader & reader);
    void set(DMFileReader & from_reader, int64_t col_id, size_t start, size_t count, ColumnPtr & col);

private:
    std::mutex mtx;
    std::unordered_map<uint64_t, std::unordered_set<DMFileReader *>> readers;
};
} // namespace DB::DM