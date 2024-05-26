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
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB::DM
{
namespace tests
{
class SegmentReadTasksPoolTest;
}

struct MergedUnit
{
    MergedUnit(const SegmentReadTaskPoolPtr & pool_, const SegmentReadTaskPtr & task_)
        : pool(pool_)
        , task(task_)
    {}

    ~MergedUnit()
    {
        // Calling `setFinish()` for updating memory statistics of `MemoryTracker`.
        [[maybe_unused]] auto res = setFinish();
    }

    [[nodiscard]] bool isFinished() const { return pool == nullptr && task == nullptr && stream == nullptr; }

    // If setted return true else return false.
    [[nodiscard]] bool setFinish()
    {
        if (!isFinished())
        {
            {
                // For updating memory statistics of `MemoryTracker`.
                MemoryTrackerSetter setter(true, pool->mem_tracker.get());
                task = nullptr;
                stream = nullptr;
            }
            // `SegmentReadTaskScheduler` will release `pool` if there is no other component/thread hold it.
            // So `pool` should release after `setter` destructed because it holds a raw pointer of `mem_tracker`.
            pool = nullptr;
            return true;
        }
        return false;
    }

    SegmentReadTaskPoolPtr pool; // The information of a read request.
    SegmentReadTaskPtr task; // The information of a segment that want to read.
    BlockInputStreamPtr stream; // BlockInputStream of a segment, will be created by read threads.
};

// MergedTask merges the same segment of different SegmentReadTaskPools.
// Read segment input streams of different SegmentReadTaskPools sequentially to improve cache sharing.
// MergedTask is NOT thread-safe.
class MergedTask
{
public:
    static int64_t getPassiveMergedSegments() { return passive_merged_segments.load(std::memory_order_relaxed); }

    MergedTask(const GlobalSegmentID & seg_id_, std::vector<MergedUnit> && units_)
        : seg_id(seg_id_)
        , units(std::move(units_))
        , inited(false)
        , cur_idx(-1)
        , finished_count(0)
    {
        passive_merged_segments.fetch_add(units.size() - 1, std::memory_order_relaxed);
        GET_METRIC(tiflash_storage_read_thread_gauge, type_merged_task).Increment();
    }
    ~MergedTask()
    {
        passive_merged_segments.fetch_sub(units.size() - 1, std::memory_order_relaxed);
        GET_METRIC(tiflash_storage_read_thread_gauge, type_merged_task).Decrement();
        GET_METRIC(tiflash_storage_read_thread_seconds, type_merged_task).Observe(sw.elapsedSeconds());
    }

    int readBlock();

    bool allStreamsFinished() const { return finished_count >= units.size(); }

    const GlobalSegmentID & getSegmentId() const { return seg_id; }

    bool containPool(uint64_t pool_id) const
    {
        return std::any_of(units.begin(), units.end(), [pool_id](const auto & u) {
            return u.pool != nullptr && u.pool->pool_id == pool_id;
        });
    }

    void setException(const DB::Exception & e);

    String toString() const
    {
        std::vector<UInt64> ids;
        ids.reserve(units.size());
        std::for_each(units.begin(), units.end(), [&ids](const auto & u) {
            if (u.pool != nullptr)
                ids.push_back(u.pool->pool_id);
        });
        return fmt::format("seg_id:{} pool_id:{}", seg_id, ids);
    }

    LoggerPtr getCurrentLogger() const
    {
        // `std::cmp_*` is safety to compare negative signed integers and unsigned integers.
        if (likely(
                std::cmp_less_equal(0, cur_idx) && std::cmp_less(cur_idx, units.size())
                && units[cur_idx].task != nullptr))
        {
            return units[cur_idx].task->read_snapshot->log;
        }
        else if (!units.empty() && units.front().task != nullptr)
        {
            return units.front().task->read_snapshot->log;
        }
        else
        {
            return Logger::get();
        }
    }

private:
    void initOnce();
    int readOneBlock();
    void setUnitFinish(int i) { finished_count += units[i].setFinish(); }

    GlobalSegmentID seg_id;
    std::vector<MergedUnit> units;
    bool inited;
    int cur_idx;
    size_t finished_count;
    Stopwatch sw;
    inline static std::atomic<int64_t> passive_merged_segments{0};

    friend class tests::SegmentReadTasksPoolTest;
};

using MergedTaskPtr = std::shared_ptr<MergedTask>;

// MergedTaskPool is a MergedTask list.
// When SegmentReadTaskPool's block queue reaching limit, read thread will push MergedTask into it.
// The scheduler thread will try to pop a MergedTask of a related pool_id before build a new MergedTask object.
class MergedTaskPool
{
public:
    MergedTaskPtr pop(uint64_t pool_id);
    void push(const MergedTaskPtr & t);
    bool has(UInt64 pool_id);

private:
    std::mutex mtx;
    std::list<MergedTaskPtr> merged_task_pool GUARDED_BY(mtx);
};
} // namespace DB::DM
