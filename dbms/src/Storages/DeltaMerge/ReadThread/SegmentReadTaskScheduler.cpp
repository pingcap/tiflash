// Copyright 2022 PingCAP, Ltd.
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
    , log(&Poco::Logger::get("SegmentReadTaskScheduler"))
{
    sched_thread = std::thread(&SegmentReadTaskScheduler::schedThread, this);
}

SegmentReadTaskScheduler::~SegmentReadTaskScheduler()
{
    setStop();
    sched_thread.join();
}

void SegmentReadTaskScheduler::add(const SegmentReadTaskPoolPtr & pool)
{
    std::lock_guard lock(mtx);

    read_pools.add(pool);

    std::unordered_set<uint64_t> seg_ids;
    for (const auto & task : pool->getTasks())
    {
        auto seg_id = task->segment->segmentId();
        merging_segments[pool->tableId()][seg_id].push_back(pool->poolId());
        if (!seg_ids.insert(seg_id).second)
        {
            throw DB::Exception(fmt::format("Not support split segment task. seg_ids {} => seg_id {} already exist.", seg_ids, seg_id));
        }
    }
    auto block_slots = pool->getFreeBlockSlots();
    auto [unexpired, expired] = read_pools.count(pool->tableId());
    LOG_FMT_DEBUG(log, "add pool {} table {} block_slots {} segment count {} segments {} unexpired pool {} expired pool {}", //
                  pool->poolId(),
                  pool->tableId(),
                  block_slots,
                  seg_ids.size(),
                  seg_ids,
                  unexpired,
                  expired);
}

std::pair<MergedTaskPtr, bool> SegmentReadTaskScheduler::scheduleMergedTask()
{
    std::lock_guard lock(mtx);
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

SegmentReadTaskPoolPtr SegmentReadTaskScheduler::scheduleSegmentReadTaskPoolUnlock()
{
    auto [valid, invalid] = read_pools.count(0);
    auto pool_count = valid + invalid; // Invalid read task pool also need to be scheduled for clean MergedTaskPool.
    for (int64_t i = 0; i < pool_count; i++)
    {
        auto pool = read_pools.next();
        // If pool->getFreeBlockSlots() > 0, schedule it for read blocks.
        // If !pool->valid(), schedule it for clean MergedTaskPool.
        if (pool != nullptr && (pool->getFreeBlockSlots() > 0 || !pool->valid()))
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
    auto [unexpired, expired] = read_pools.count(pool->tableId());
    auto expected_merge_seg_count = std::min(unexpired, 2);
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
    Stopwatch sw;
    auto [merged_task, run_sche] = scheduleMergedTask();
    if (merged_task != nullptr)
    {
        LOG_FMT_DEBUG(log, "scheduleMergedTask seg_id {} pools {} => {} ms", merged_task->getSegmentId(), merged_task->getPoolIds(), sw.elapsedMilliseconds());
        SegmentReaderPoolManager::instance().addTask(std::move(merged_task));
    }
    return run_sche;
}

void SegmentReadTaskScheduler::schedThread()
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