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
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB::DM
{
SegmentReadTaskScheduler::SegmentReadTaskScheduler()
    : stop(false)
    , log(Logger::get())
{
    sched_thread = std::thread(&SegmentReadTaskScheduler::schedLoop, this);
}

SegmentReadTaskScheduler::~SegmentReadTaskScheduler()
{
    setStop();
    sched_thread.join();
}

void SegmentReadTaskScheduler::add(const SegmentReadTaskPoolPtr & pool)
{
    if (pool->getTasks().empty())
    {
        return;
    }
    Stopwatch sw_add;
    std::lock_guard lock(mtx);
    Stopwatch sw_do_add;
    read_pools.add(pool);

    const auto & tasks = pool->getTasks();
    for (const auto & pa : tasks)
    {
        auto seg_id = pa.first;
        merging_segments[pool->tableId()][seg_id].push_back(pool->poolId());
    }
    LOG_INFO(log, "Added, pool_id={} table_id={} block_slots={} segment_count={} pool_count={} cost={:3f}us do_add_cost={:.3f}us", //
             pool->poolId(),
             pool->tableId(),
             pool->getFreeBlockSlots(),
             tasks.size(),
             read_pools.size(),
             sw_add.elapsed() / 1000.0,
             sw_do_add.elapsed() / 1000.0);
}

std::pair<MergedTaskPtr, bool> SegmentReadTaskScheduler::scheduleMergedTask()
{
    auto pool = scheduleSegmentReadTaskPoolUnlock();
    if (pool == nullptr)
    {
        // No SegmentReadTaskPool to schedule. Maybe no read request or
        // block queue of each SegmentReadTaskPool reaching the limit.
        return {nullptr, false};
    }

    // If pool->valid(), read blocks.
    // If !pool->valid(), read path will clean it.
    auto merged_task = merged_task_pool.pop(pool->poolId());
    if (merged_task != nullptr)
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_from_cache).Increment();
        return {merged_task, true};
    }

    if (!pool->valid())
    {
        return {nullptr, true};
    }

    auto segment = scheduleSegmentUnlock(pool);
    if (!segment)
    {
        // The number of active segments reaches the limit.
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_segment).Increment();
        return {nullptr, true};
    }
    auto pools = getPoolsUnlock(segment->second);
    if (pools.empty())
    {
        // Maybe SegmentReadTaskPools are expired because of upper threads finish the request.
        return {nullptr, true};
    }

    std::vector<MergedUnit> units;
    units.reserve(pools.size());
    for (auto & pool : pools)
    {
        units.emplace_back(pool, pool->getTask(segment->first));
    }
    GET_METRIC(tiflash_storage_read_thread_counter, type_sche_new_task).Increment();

    return {std::make_shared<MergedTask>(segment->first, std::move(units)), true};
}

SegmentReadTaskPools SegmentReadTaskScheduler::getPoolsUnlock(const std::vector<uint64_t> & pool_ids)
{
    SegmentReadTaskPools pools;
    pools.reserve(pool_ids.size());
    for (uint64_t id : pool_ids)
    {
        auto p = read_pools.get(id);
        if (p != nullptr)
        {
            pools.push_back(p);
        }
    }
    return pools;
}

bool SegmentReadTaskScheduler::needScheduleToRead(const SegmentReadTaskPoolPtr & pool)
{
    return pool->getFreeBlockSlots() > 0 && // Block queue is not full and
        (merged_task_pool.has(pool->poolId()) || // can schedule a segment from MergedTaskPool or
         pool->getFreeActiveSegments() > 0); // schedule a new segment.
}

SegmentReadTaskPoolPtr SegmentReadTaskScheduler::scheduleSegmentReadTaskPoolUnlock()
{
    int64_t pool_count = read_pools.size(); // All read task pool need to be scheduled, including invalid read task pool.
    for (int64_t i = 0; i < pool_count; i++)
    {
        auto pool = read_pools.next();
        // If !pool->valid(), schedule it for clean MergedTaskPool.
        if (pool != nullptr && (needScheduleToRead(pool) || !pool->valid()))
        {
            return pool;
        }
    }
    if (pool_count == 0)
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_pool).Increment();
    }
    else
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_slot).Increment();
    }
    return nullptr;
}

std::optional<std::pair<uint64_t, std::vector<uint64_t>>> SegmentReadTaskScheduler::scheduleSegmentUnlock(const SegmentReadTaskPoolPtr & pool)
{
    auto expected_merge_seg_count = std::min(read_pools.size(), 2); // Not accurate.
    auto itr = merging_segments.find(pool->tableId());
    if (itr == merging_segments.end())
    {
        // No segment of tableId left.
        return std::nullopt;
    }
    std::optional<std::pair<uint64_t, std::vector<uint64_t>>> result;
    auto & segments = itr->second;
    auto target = pool->scheduleSegment(segments, expected_merge_seg_count);
    if (target != segments.end())
    {
        if (MergedTask::getPassiveMergedSegments() < 100 || target->second.size() == 1)
        {
            result = *target;
            segments.erase(target);
            if (segments.empty())
            {
                merging_segments.erase(itr);
            }
        }
        else
        {
            result = std::pair{target->first, std::vector<uint64_t>(1, pool->poolId())};
            auto mutable_target = segments.find(target->first);
            auto itr = std::find(mutable_target->second.begin(), mutable_target->second.end(), pool->poolId());
            *itr = mutable_target->second.back(); // SegmentReadTaskPool::scheduleSegment ensures `pool->poolId` must exists in `target`.
            mutable_target->second.resize(mutable_target->second.size() - 1);
        }
    }
    return result;
}

void SegmentReadTaskScheduler::setStop()
{
    stop.store(true, std::memory_order_relaxed);
}

bool SegmentReadTaskScheduler::isStop() const
{
    return stop.load(std::memory_order_relaxed);
}

bool SegmentReadTaskScheduler::schedule()
{
    Stopwatch sw_sche_all;
    std::lock_guard lock(mtx);
    Stopwatch sw_do_sche_all;
    static constexpr size_t max_sche_count = 8;
    auto pool_count = read_pools.size();
    auto sche_count = std::min(pool_count, max_sche_count);
    bool run_sche = false;
    size_t count = 0;
    while (count < sche_count)
    {
        count++;
        Stopwatch sw_sche_once;
        MergedTaskPtr merged_task;
        std::tie(merged_task, run_sche) = scheduleMergedTask();
        if (merged_task != nullptr)
        {
            auto elapsed_ms = sw_sche_once.elapsedMilliseconds();
            if (elapsed_ms >= 5)
            {
                LOG_DEBUG(log, "scheduleMergedTask segment_id={} pool_ids={} cost={}ms pool_count={}", merged_task->getSegmentId(), merged_task->getPoolIds(), elapsed_ms, pool_count);
            }
            SegmentReaderPoolManager::instance().addTask(std::move(merged_task));
        }
        if (!run_sche)
        {
            break;
        }
    }
    auto sche_all_elapsed_ms = sw_sche_all.elapsedMilliseconds();
    if (sche_all_elapsed_ms >= 100)
    {
        LOG_DEBUG(log, "schedule pool_count={} count={} cost={}ms do_sche_cost={}ms", pool_count, count, sche_all_elapsed_ms, sw_do_sche_all.elapsedMilliseconds());
    }
    return run_sche;
}

void SegmentReadTaskScheduler::schedLoop()
{
    while (!isStop())
    {
        if (!schedule())
        {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(2ms);
        }
    }
}

} // namespace DB::DM
