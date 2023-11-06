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

#include <Common/FailPoint.h>
#include <DataStreams/AddExtraTableIDColumnInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

#include <magic_enum.hpp>

namespace DB::FailPoints
{
extern const char pause_when_reading_from_dt_stream[];
} // namespace DB::FailPoints

namespace DB::DM
{
SegmentReadTasksWrapper::SegmentReadTasksWrapper(bool enable_read_thread_, SegmentReadTasks && ordered_tasks_)
    : enable_read_thread(enable_read_thread_)
{
    if (enable_read_thread)
    {
        for (const auto & t : ordered_tasks_)
        {
            auto [itr, inserted] = unordered_tasks.emplace(t->getGlobalSegmentID(), t);
            if (!inserted)
            {
                throw DB::Exception(fmt::format("segment={} already exist.", t));
            }
        }
    }
    else
    {
        ordered_tasks = std::move(ordered_tasks_);
    }
}

SegmentReadTaskPtr SegmentReadTasksWrapper::nextTask()
{
    RUNTIME_CHECK(!enable_read_thread);
    if (ordered_tasks.empty())
    {
        return nullptr;
    }
    auto task = ordered_tasks.front();
    ordered_tasks.pop_front();
    return task;
}

SegmentReadTaskPtr SegmentReadTasksWrapper::getTask(const GlobalSegmentID & seg_id)
{
    RUNTIME_CHECK(enable_read_thread);
    auto itr = unordered_tasks.find(seg_id);
    if (itr == unordered_tasks.end())
    {
        return nullptr;
    }
    auto t = itr->second;
    unordered_tasks.erase(itr);
    return t;
}

const std::unordered_map<GlobalSegmentID, SegmentReadTaskPtr> & SegmentReadTasksWrapper::getTasks() const
{
    RUNTIME_CHECK(enable_read_thread);
    return unordered_tasks;
}

bool SegmentReadTasksWrapper::empty() const
{
    return ordered_tasks.empty() && unordered_tasks.empty();
}

BlockInputStreamPtr SegmentReadTaskPool::buildInputStream(SegmentReadTaskPtr & t)
{
    // Call fetchPages before MemoryTrackerSetter, because its memory usage is tracked by `fetch_pages_mem_tracker`.
    t->fetchPages();

    MemoryTrackerSetter setter(true, mem_tracker.get());

    t->initInputStream(
        columns_to_read,
        max_version,
        filter,
        read_mode,
        expected_block_size,
        t->dm_context->global_context.getSettingsRef().dt_enable_delta_index_error_fallback);
    BlockInputStreamPtr stream = std::make_shared<AddExtraTableIDColumnInputStream>(
        t->getInputStream(),
        extra_table_id_index,
        t->dm_context->physical_table_id);
    LOG_DEBUG(
        log,
        "buildInputStream: read_mode={}, pool_id={} segment={}",
        magic_enum::enum_name(read_mode),
        pool_id,
        t);
    return stream;
}

SegmentReadTaskPool::SegmentReadTaskPool(
    int extra_table_id_index_,
    const ColumnDefines & columns_to_read_,
    const PushDownFilterPtr & filter_,
    uint64_t max_version_,
    size_t expected_block_size_,
    ReadMode read_mode_,
    SegmentReadTasks && tasks_,
    AfterSegmentRead after_segment_read_,
    const String & tracing_id,
    bool enable_read_thread_,
    Int64 num_streams_)
    : pool_id(nextPoolId())
    , mem_tracker(current_memory_tracker == nullptr ? nullptr : current_memory_tracker->shared_from_this())
    , extra_table_id_index(extra_table_id_index_)
    , columns_to_read(columns_to_read_)
    , filter(filter_)
    , max_version(max_version_)
    , expected_block_size(expected_block_size_)
    , read_mode(read_mode_)
    , tasks_wrapper(enable_read_thread_, std::move(tasks_))
    , after_segment_read(after_segment_read_)
    , log(Logger::get(tracing_id))
    , unordered_input_stream_ref_count(0)
    , exception_happened(false)
    // If the queue is too short, only 1 in the extreme case, it may cause the computation thread
    // to encounter empty queues frequently, resulting in too much waiting and thread context
    // switching, so we limit the lower limit to 3, which provides two blocks of buffer space.
    , block_slot_limit(std::max(num_streams_, 3))
    // Limiting the minimum number of reading segments to 2 is to avoid, as much as possible,
    // situations where the computation may be faster and the storage layer may not be able to keep up.
    , active_segment_limit(std::max(num_streams_, 2))
{
    if (tasks_wrapper.empty())
    {
        q.finish();
    }
}

void SegmentReadTaskPool::finishSegment(const SegmentReadTaskPtr & seg)
{
    after_segment_read(seg->dm_context, seg->segment);
    bool pool_finished = false;
    {
        std::lock_guard lock(mutex);
        active_segment_ids.erase(seg->getGlobalSegmentID());
        pool_finished = active_segment_ids.empty() && tasks_wrapper.empty();
    }
    LOG_DEBUG(log, "finishSegment pool_id={} segment={} pool_finished={}", pool_id, seg, pool_finished);
    if (pool_finished)
    {
        q.finish();
    }
}

SegmentReadTaskPtr SegmentReadTaskPool::nextTask()
{
    std::lock_guard lock(mutex);
    return tasks_wrapper.nextTask();
}

SegmentReadTaskPtr SegmentReadTaskPool::getTask(const GlobalSegmentID & seg_id)
{
    std::lock_guard lock(mutex);
    auto t = tasks_wrapper.getTask(seg_id);
    RUNTIME_CHECK(t != nullptr, pool_id, seg_id);
    active_segment_ids.insert(seg_id);
    return t;
}

const std::unordered_map<GlobalSegmentID, SegmentReadTaskPtr> & SegmentReadTaskPool::getTasks()
{
    std::lock_guard lock(mutex);
    return tasks_wrapper.getTasks();
}

// Choose a segment to read.
// Returns <segment_id, pool_ids>.
MergingSegments::iterator SegmentReadTaskPool::scheduleSegment(
    MergingSegments & segments,
    uint64_t expected_merge_count)
{
    auto target = segments.end();
    std::lock_guard lock(mutex);
    if (getFreeActiveSegmentsUnlock() <= 0)
    {
        return target;
    }
    static constexpr int max_iter_count = 32;
    int iter_count = 0;
    const auto & tasks = tasks_wrapper.getTasks();
    for (const auto & [seg_id, task] : tasks)
    {
        auto itr = segments.find(seg_id);
        if (itr == segments.end())
        {
            throw DB::Exception(fmt::format("segment_id {} not found from merging segments", task));
        }
        if (std::find(itr->second.begin(), itr->second.end(), pool_id) == itr->second.end())
        {
            throw DB::Exception(
                fmt::format("pool_id={} not found from merging segment {}=>{}", pool_id, task, itr->second));
        }
        if (target == segments.end() || itr->second.size() > target->second.size())
        {
            target = itr;
        }
        if (target->second.size() >= expected_merge_count || ++iter_count >= max_iter_count)
        {
            break;
        }
    }
    return target;
}

bool SegmentReadTaskPool::readOneBlock(BlockInputStreamPtr & stream, const SegmentReadTaskPtr & seg)
{
    MemoryTrackerSetter setter(true, mem_tracker.get());
    FAIL_POINT_PAUSE(FailPoints::pause_when_reading_from_dt_stream);
    auto block = stream->read();
    if (block)
    {
        pushBlock(std::move(block));
        return true;
    }
    else
    {
        finishSegment(seg);
        return false;
    }
}

void SegmentReadTaskPool::popBlock(Block & block)
{
    q.pop(block);
    blk_stat.pop(block);
    global_blk_stat.pop(block);
    if (exceptionHappened())
    {
        throw exception;
    }
}

bool SegmentReadTaskPool::tryPopBlock(Block & block)
{
    if (q.tryPop(block))
    {
        blk_stat.pop(block);
        global_blk_stat.pop(block);
        if (exceptionHappened())
            throw exception;
        return true;
    }
    else
    {
        return false;
    }
}

void SegmentReadTaskPool::pushBlock(Block && block)
{
    blk_stat.push(block);
    global_blk_stat.push(block);
    q.push(std::move(block), nullptr);
}

Int64 SegmentReadTaskPool::increaseUnorderedInputStreamRefCount()
{
    return unordered_input_stream_ref_count.fetch_add(1, std::memory_order_relaxed);
}
Int64 SegmentReadTaskPool::decreaseUnorderedInputStreamRefCount()
{
    return unordered_input_stream_ref_count.fetch_sub(1, std::memory_order_relaxed);
}

Int64 SegmentReadTaskPool::getFreeBlockSlots() const
{
    return block_slot_limit - blk_stat.pendingCount();
}

Int64 SegmentReadTaskPool::getFreeActiveSegments() const
{
    std::lock_guard lock(mutex);
    return getFreeActiveSegmentsUnlock();
}

Int64 SegmentReadTaskPool::getFreeActiveSegmentsUnlock() const
{
    return active_segment_limit - static_cast<Int64>(active_segment_ids.size());
}

bool SegmentReadTaskPool::exceptionHappened() const
{
    return exception_happened.load(std::memory_order_relaxed);
}

bool SegmentReadTaskPool::valid() const
{
    return !exceptionHappened() && unordered_input_stream_ref_count.load(std::memory_order_relaxed) > 0;
}
void SegmentReadTaskPool::setException(const DB::Exception & e)
{
    std::lock_guard lock(mutex);
    if (!exceptionHappened())
    {
        exception = e;
        exception_happened.store(true, std::memory_order_relaxed);
        q.finish();
    }
}

} // namespace DB::DM
