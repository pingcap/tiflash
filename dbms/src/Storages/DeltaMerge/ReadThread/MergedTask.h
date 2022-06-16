#pragma once

#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB::DM
{
// MergedTask merges the same segment of different SegmentReadTaskPools.
// Read segment input streams of different SegmentReadTaskPools sequentially to improve cache sharing.
// MergedTask is NOT thread-safe.
class MergedTask
{
public:
    static int64_t getPassiveMergedSegments()
    {
        return passive_merged_segments.load(std::memory_order_relaxed);
    }

    MergedTask(uint64_t seg_id_, SegmentReadTaskPools && pools_, std::vector<SegmentReadTaskPtr> && tasks_)
        : seg_id(seg_id_)
        , pools(std::move(pools_))
        , tasks(std::move(tasks_))
        , cur_idx(-1)
        , finished_count(0)
        , log(&Poco::Logger::get("MergedTask"))
    {
        passive_merged_segments.fetch_add(pools.size() - 1, std::memory_order_relaxed);
    }
    ~MergedTask()
    {
        passive_merged_segments.fetch_sub(pools.size() - 1, std::memory_order_relaxed);
    }

    int readBlock();

    bool allStreamsFinished() const
    {
        return finished_count >= pools.size();
    }

    uint64_t getSegmentId() const
    {
        return seg_id;
    }

    size_t getPoolCount() const
    {
        return pools.size();
    }

    std::vector<uint64_t> getPoolIds() const
    {
        std::vector<uint64_t> ids;
        ids.reserve(pools.size());
        for (const auto & pool : pools)
        {
            if (pool != nullptr)
            {
                ids.push_back(pool->poolId());
            }
        }
        return ids;
    }

    bool containPool(uint64_t pool_id) const
    {
        for (const auto & pool : pools)
        {
            if (pool != nullptr && pool->poolId() == pool_id)
            {
                return true;
            }
        }
        return false;
    }
    void setException(const DB::Exception & e);

private:
    void initOnce();
    int readOneBlock();

    bool isStreamFinished(size_t i)
    {
        return streams[i] == nullptr;
    }
    void setStreamFinished(size_t i)
    {
        if (!isStreamFinished(i))
        {
            streams[i] = nullptr;
            pools[i] = nullptr;
            finished_count++;
        }
    }

    uint64_t seg_id;
    SegmentReadTaskPools pools;
    std::vector<SegmentReadTaskPtr> tasks;
    BlockInputStreams streams;
    int cur_idx;
    size_t finished_count;
    Poco::Logger * log;

    inline static std::atomic<int64_t> passive_merged_segments{0};
};

using MergedTaskPtr = std::shared_ptr<MergedTask>;

// MergedTaskPool is a MergedTask list.
// When SegmentReadTaskPool's block queue reaching limit, read thread will push MergedTask into it.
// The scheduler thread will try to pop a MergedTask of a related pool_id before build a new MergedTask object.
class MergedTaskPool
{
public:
    MergedTaskPool()
        : log(&Poco::Logger::get("MergedTaskPool"))
    {}

    MergedTaskPtr pop(uint64_t pool_id);
    void push(const MergedTaskPtr & t);

private:
    std::mutex mtx;
    std::list<MergedTaskPtr> merged_task_pool;
    Poco::Logger * log;
};
} // namespace DB::DM