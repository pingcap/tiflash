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
#include <Common/setThreadName.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB::DM
{
SegmentReadTaskScheduler::SegmentReadTaskScheduler(bool run_sched_thread)
    : log(Logger::get())
{
    if (likely(run_sched_thread))
    {
        sched_thread = std::thread(&SegmentReadTaskScheduler::schedLoop, this);
    }
}

SegmentReadTaskScheduler::~SegmentReadTaskScheduler()
{
    setStop();
    if (likely(sched_thread.joinable()))
    {
        sched_thread.join();
    }
}

void SegmentReadTaskScheduler::add(const SegmentReadTaskPoolPtr & pool)
{
    assert(pool != nullptr);
    Stopwatch sw_add;
    // `add_lock` is only used in this function to make all threads calling `add` to execute serially.
    std::lock_guard add_lock(add_mtx);
    add_waittings.fetch_add(1, std::memory_order_relaxed);
    // `lock` is used to protect data.
    std::lock_guard lock(mtx);
    add_waittings.fetch_sub(1, std::memory_order_relaxed);
    Stopwatch sw_do_add;
    read_pools.emplace(pool->pool_id, pool);

    const auto & tasks = pool->getTasks();
    for (const auto & [seg_id, task] : tasks)
    {
        merging_segments[seg_id].push_back(pool->pool_id);
    }
    LOG_INFO(
        pool->getLogger(),
        "Added, pool_id={} block_slots={} segment_count={} pool_count={} cost={:.3f}us do_add_cost={:.3f}us", //
        pool->pool_id,
        pool->getFreeBlockSlots(),
        tasks.size(),
        read_pools.size(),
        sw_add.elapsed() / 1000.0,
        sw_do_add.elapsed() / 1000.0);
}

MergedTaskPtr SegmentReadTaskScheduler::scheduleMergedTask(SegmentReadTaskPoolPtr & pool)
{
    // If pool->valid(), read blocks.
    // If !pool->valid(), read path will clean it.
    auto merged_task = merged_task_pool.pop(pool->pool_id);
    if (merged_task != nullptr)
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_from_cache).Increment();
        return merged_task;
    }

    if (!pool->valid())
    {
        return nullptr;
    }

    auto segment = scheduleSegmentUnlock(pool);
    if (!segment)
    {
        // The number of active segments reaches the limit.
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_segment).Increment();
        return nullptr;
    }

    RUNTIME_CHECK(!segment->second.empty());
    auto pools = getPoolsUnlock(segment->second);
    if (pools.empty())
    {
        // Maybe SegmentReadTaskPools are expired because of upper threads finish the request.
        return nullptr;
    }

    std::vector<MergedUnit> units;
    units.reserve(pools.size());
    for (auto & pool : pools)
    {
        units.emplace_back(pool, pool->getTask(segment->first));
    }
    GET_METRIC(tiflash_storage_read_thread_counter, type_sche_new_task).Increment();

    return std::make_shared<MergedTask>(segment->first, std::move(units));
}

SegmentReadTaskPools SegmentReadTaskScheduler::getPoolsUnlock(const std::vector<uint64_t> & pool_ids)
{
    SegmentReadTaskPools pools;
    pools.reserve(pool_ids.size());
    for (auto pool_id : pool_ids)
    {
        auto itr = read_pools.find(pool_id);
        if (likely(itr != read_pools.end()))
        {
            pools.push_back(itr->second);
        }
    }
    return pools;
}

bool SegmentReadTaskScheduler::needScheduleToRead(const SegmentReadTaskPoolPtr & pool)
{
    if (pool->getFreeBlockSlots() <= 0)
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_slot).Increment();
        return false;
    }

    if (pool->isRUExhausted())
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_ru).Increment();
        return false;
    }

    // Check if there are segments that can be scheduled:
    // 1. There are already activated segments.
    if (merged_task_pool.has(pool->pool_id))
    {
        return true;
    }
    // 2. Not reach limitation, we can activate a segment.
    if (pool->getFreeActiveSegments() > 0 && pool->getPendingSegmentCount() > 0)
    {
        return true;
    }

    if (pool->getFreeActiveSegments() <= 0)
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_active_segment_limit).Increment();
    }
    else
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_segment).Increment();
    }
    return false;
}

bool SegmentReadTaskScheduler::needSchedule(const SegmentReadTaskPoolPtr & pool)
{
    // If `!pool->valid()` is true, schedule it for clean `MergedTaskPool`.
    return pool != nullptr && (needScheduleToRead(pool) || !pool->valid());
}

std::optional<std::pair<GlobalSegmentID, std::vector<UInt64>>> SegmentReadTaskScheduler::scheduleSegmentUnlock(
    const SegmentReadTaskPoolPtr & pool)
{
    auto expected_merge_seg_count = std::min(read_pools.size(), 2); // Not accurate.

    std::optional<std::pair<GlobalSegmentID, std::vector<uint64_t>>> result;
    auto target = pool->scheduleSegment(merging_segments, expected_merge_seg_count, enable_data_sharing);
    if (target != merging_segments.end())
    {
        if ((enable_data_sharing && MergedTask::getPassiveMergedSegments() < 100) || target->second.size() == 1)
        {
            result = *target;
            merging_segments.erase(target);
        }
        else
        {
            result = std::pair{target->first, std::vector<uint64_t>(1, pool->pool_id)};
            auto itr = std::find(target->second.begin(), target->second.end(), pool->pool_id);
            // SegmentReadTaskPool::scheduleSegment ensures `pool->poolId` must exists in `target`.
            *itr = target->second.back();
            target->second.resize(target->second.size() - 1);
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

std::tuple<UInt64, UInt64, UInt64> SegmentReadTaskScheduler::scheduleOneRound()
{
    UInt64 erased_pool_count = 0;
    UInt64 sched_null_count = 0;
    UInt64 sched_succ_count = 0;
    for (auto itr = read_pools.begin(); itr != read_pools.end(); /**/)
    {
        auto & pool = itr->second;
        // No other component or thread hold this `pool`, we can release it.
        // TODO: `weak_ptr` may be more suitable.
        if (pool.use_count() == 1)
        {
            LOG_INFO(pool->getLogger(), "Erase pool_id={}", pool->pool_id);
            ++erased_pool_count;
            itr = read_pools.erase(itr);
            continue;
        }
        ++itr;

        if (!needSchedule(pool))
        {
            ++sched_null_count;
            continue;
        }

        auto merged_task = scheduleMergedTask(pool);
        if (merged_task == nullptr)
        {
            ++sched_null_count;
            continue;
        }
        ++sched_succ_count;
        SegmentReaderPoolManager::instance().addTask(std::move(merged_task));
    }
    return std::make_tuple(erased_pool_count, sched_null_count, sched_succ_count);
}

bool SegmentReadTaskScheduler::schedule()
{
    Stopwatch sw_sched_total;
    std::lock_guard lock(mtx);
    Stopwatch sw_do_sched;

    auto pool_count = read_pools.size();
    UInt64 erased_pool_count = 0;
    UInt64 sched_null_count = 0;
    UInt64 sched_succ_count = 0;
    UInt64 sched_round = 0;
    bool can_sched_more_tasks = false;
    do
    {
        ++sched_round;
        auto [erase, null, succ] = scheduleOneRound();
        erased_pool_count += erase;
        sched_null_count += null;
        sched_succ_count += succ;

        can_sched_more_tasks = succ > 0 && !read_pools.empty();
        // If no thread is waitting to add tasks and there are some tasks to be scheduled, run scheduling again.
        // Avoid releasing and acquiring `mtx` repeatly.
        // This is common when query concurrency is low, but individual queries are heavy.
    } while (add_waittings.load(std::memory_order_relaxed) <= 0 && can_sched_more_tasks);

    if (read_pools.empty())
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_pool).Increment();
    }

    auto total_ms = sw_sched_total.elapsedMilliseconds();
    if (total_ms >= 100)
    {
        LOG_INFO(
            log,
            "schedule sched_round={} pool_count={} erased_pool_count={} sched_null_count={} sched_succ_count={} "
            "cost={}ms do_sched_cost={}ms",
            sched_round,
            pool_count,
            erased_pool_count,
            sched_null_count,
            sched_succ_count,
            total_ms,
            sw_do_sched.elapsedMilliseconds());
    }
    return can_sched_more_tasks;
}

void SegmentReadTaskScheduler::schedLoop()
{
    setThreadName("segment-sched");
    while (!isStop())
    {
        if (!schedule())
        {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(2ms);
        }
    }
}

void SegmentReadTaskScheduler::updateConfig(const Settings & settings)
{
    enable_data_sharing = settings.dt_max_sharing_column_bytes_for_all > 0;
}

} // namespace DB::DM
