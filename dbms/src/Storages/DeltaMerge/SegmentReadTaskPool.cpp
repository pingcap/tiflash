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

#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <DataStreams/AddExtraTableIDColumnInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

#include <magic_enum.hpp>

namespace CurrentMetrics
{
extern const Metric DT_SegmentReadTasks;
}

namespace DB::FailPoints
{
extern const char pause_when_reading_from_dt_stream[];
} // namespace DB::FailPoints

namespace DB::DM
{
SegmentReadTask::SegmentReadTask(
    const SegmentPtr & segment_, //
    const SegmentSnapshotPtr & read_snapshot_,
    const RowKeyRanges & ranges_)
    : segment(segment_)
    , read_snapshot(read_snapshot_)
    , ranges(ranges_)
{
    CurrentMetrics::add(CurrentMetrics::DT_SegmentReadTasks);
}

SegmentReadTask::SegmentReadTask(const SegmentPtr & segment_, const SegmentSnapshotPtr & read_snapshot_)
    : SegmentReadTask{segment_, read_snapshot_, RowKeyRanges{}}
{}

SegmentReadTask::~SegmentReadTask()
{
    CurrentMetrics::sub(CurrentMetrics::DT_SegmentReadTasks);
}

std::pair<size_t, size_t> SegmentReadTask::getRowsAndBytes() const
{
    return {
        read_snapshot->delta->getRows() + read_snapshot->stable->getRows(),
        read_snapshot->delta->getBytes() + read_snapshot->stable->getBytes()};
}

SegmentReadTasks SegmentReadTask::trySplitReadTasks(const SegmentReadTasks & tasks, size_t expected_size)
{
    if (tasks.empty() || tasks.size() >= expected_size)
        return tasks;

    // Note that expected_size is normally small(less than 100), so the algorithm complexity here does not matter.

    // Construct a max heap, determined by ranges' count.
    auto cmp = [](const SegmentReadTaskPtr & a, const SegmentReadTaskPtr & b) {
        return a->ranges.size() < b->ranges.size();
    };
    std::priority_queue<SegmentReadTaskPtr, std::vector<SegmentReadTaskPtr>, decltype(cmp)> largest_ranges_first(cmp);
    for (const auto & task : tasks)
        largest_ranges_first.push(task);

    // Split the top task.
    while (largest_ranges_first.size() < expected_size && largest_ranges_first.top()->ranges.size() > 1)
    {
        auto top = largest_ranges_first.top();
        largest_ranges_first.pop();

        size_t split_count = top->ranges.size() / 2;

        auto left = std::make_shared<SegmentReadTask>(
            top->segment,
            top->read_snapshot->clone(),
            RowKeyRanges(top->ranges.begin(), top->ranges.begin() + split_count));
        auto right = std::make_shared<SegmentReadTask>(
            top->segment,
            top->read_snapshot->clone(),
            RowKeyRanges(top->ranges.begin() + split_count, top->ranges.end()));

        largest_ranges_first.push(left);
        largest_ranges_first.push(right);
    }

    SegmentReadTasks result_tasks;
    while (!largest_ranges_first.empty())
    {
        result_tasks.push_back(largest_ranges_first.top());
        largest_ranges_first.pop();
    }

    return result_tasks;
}

bool SegmentReadTask::hasColumnFileToFetch() const
{
    auto need_to_fetch = [](const ColumnFilePtr & cf) {
        // In v7.5, Only ColumnFileTiny need too fetch.
        return cf->isTinyFile();
    };
    const auto & mem_cfs = read_snapshot->delta->getMemTableSetSnapshot()->getColumnFiles();
    const auto & persisted_cfs = read_snapshot->delta->getPersistedFileSetSnapshot()->getColumnFiles();
    return std::any_of(mem_cfs.cbegin(), mem_cfs.cend(), need_to_fetch)
        || std::any_of(persisted_cfs.cbegin(), persisted_cfs.cend(), need_to_fetch);
}

SegmentReadTasksWrapper::SegmentReadTasksWrapper(bool enable_read_thread_, SegmentReadTasks && ordered_tasks_)
    : enable_read_thread(enable_read_thread_)
{
    if (enable_read_thread)
    {
        for (const auto & t : ordered_tasks_)
        {
            auto [itr, inserted] = unordered_tasks.emplace(t->segment->segmentId(), t);
            if (!inserted)
            {
                throw DB::Exception(fmt::format("segment_id={} already exist.", t->segment->segmentId()));
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

SegmentReadTaskPtr SegmentReadTasksWrapper::getTask(UInt64 seg_id)
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

const std::unordered_map<UInt64, SegmentReadTaskPtr> & SegmentReadTasksWrapper::getTasks() const
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
    MemoryTrackerSetter setter(true, mem_tracker.get());
    BlockInputStreamPtr stream;
    auto block_size = std::max(
        expected_block_size,
        static_cast<size_t>(dm_context->db_context.getSettingsRef().dt_segment_stable_pack_rows));
    if (likely(read_mode == ReadMode::Bitmap && !res_group_name.empty()))
    {
        auto bytes = t->read_snapshot->estimatedBytesOfInternalColumns();
        LocalAdmissionController::global_instance->consumeBytesResource(res_group_name, bytesToRU(bytes));
    }
    stream = t->segment->getInputStream(
        read_mode,
        *dm_context,
        columns_to_read,
        t->read_snapshot,
        t->ranges,
        filter,
        max_version,
        block_size);
    stream = std::make_shared<AddExtraTableIDColumnInputStream>(stream, extra_table_id_index, physical_table_id);
    LOG_DEBUG(
        log,
        "getInputStream succ, read_mode={}, pool_id={} segment_id={}",
        magic_enum::enum_name(read_mode),
        pool_id,
        t->segment->segmentId());
    return stream;
}

SegmentReadTaskPool::SegmentReadTaskPool(
    int64_t physical_table_id_,
    int extra_table_id_index_,
    const DMContextPtr & dm_context_,
    const ColumnDefines & columns_to_read_,
    const PushDownFilterPtr & filter_,
    uint64_t max_version_,
    size_t expected_block_size_,
    ReadMode read_mode_,
    SegmentReadTasks && tasks_,
    AfterSegmentRead after_segment_read_,
    const String & tracing_id,
    bool enable_read_thread_,
    Int64 num_streams_,
    const String & res_group_name_)
    : pool_id(nextPoolId())
    , physical_table_id(physical_table_id_)
    , mem_tracker(current_memory_tracker == nullptr ? nullptr : current_memory_tracker->shared_from_this())
    , extra_table_id_index(extra_table_id_index_)
    , dm_context(dm_context_)
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
    // to encounter empty queues frequently, resulting in too much waiting and thread context switching.
    // We limit the length of block queue to be 1.5 times of `num_streams_`, and in the extreme case,
    // when `num_streams_` is 1, `block_slot_limit` is at least 2.
    , block_slot_limit(std::ceil(num_streams_ * 1.5))
    // Limiting the minimum number of reading segments to 2 is to avoid, as much as possible,
    // situations where the computation may be faster and the storage layer may not be able to keep up.
    , active_segment_limit(std::max(num_streams_, 2))
    , res_group_name(res_group_name_)
{
    if (tasks_wrapper.empty())
    {
        q.finish();
    }
}

void SegmentReadTaskPool::finishSegment(const SegmentPtr & seg)
{
    after_segment_read(dm_context, seg);
    bool pool_finished = false;
    {
        std::lock_guard lock(mutex);
        active_segment_ids.erase(seg->segmentId());
        pool_finished = active_segment_ids.empty() && tasks_wrapper.empty();
    }
    LOG_DEBUG(log, "finishSegment pool_id={} segment_id={} pool_finished={}", pool_id, seg->segmentId(), pool_finished);
    if (pool_finished)
    {
        q.finish();
        LOG_INFO(log, "pool_id={} finished", pool_id);
    }
}

SegmentReadTaskPtr SegmentReadTaskPool::nextTask()
{
    std::lock_guard lock(mutex);
    return tasks_wrapper.nextTask();
}

SegmentReadTaskPtr SegmentReadTaskPool::getTask(UInt64 seg_id)
{
    std::lock_guard lock(mutex);
    auto t = tasks_wrapper.getTask(seg_id);
    RUNTIME_CHECK(t != nullptr, pool_id, seg_id);
    active_segment_ids.insert(seg_id);
    return t;
}

const std::unordered_map<UInt64, SegmentReadTaskPtr> & SegmentReadTaskPool::getTasks()
{
    std::lock_guard lock(mutex);
    return tasks_wrapper.getTasks();
}

// Choose a segment to read.
// Returns <segment_id, pool_ids>.
std::unordered_map<uint64_t, std::vector<uint64_t>>::const_iterator SegmentReadTaskPool::scheduleSegment(
    const std::unordered_map<uint64_t, std::vector<uint64_t>> & segments,
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
    for (const auto & task : tasks)
    {
        auto itr = segments.find(task.first);
        if (itr == segments.end())
        {
            throw DB::Exception(fmt::format("segment_id {} not found from merging segments", task.first));
        }
        if (std::find(itr->second.begin(), itr->second.end(), pool_id) == itr->second.end())
        {
            throw DB::Exception(
                fmt::format("pool_id={} not found from merging segment {}=>{}", pool_id, itr->first, itr->second));
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

bool SegmentReadTaskPool::readOneBlock(BlockInputStreamPtr & stream, const SegmentPtr & seg)
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
    auto bytes = block.bytes();
    read_bytes_after_last_check += bytes;
    GET_METRIC(tiflash_storage_read_thread_counter, type_push_block_bytes).Increment(bytes);
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

Int64 SegmentReadTaskPool::getPendingSegmentCount() const
{
    std::lock_guard lock(mutex);
    return tasks_wrapper.getTasks().size();
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

static Int64 currentMS()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

static bool checkIsRUExhausted(const String & res_group_name)
{
    auto priority = LocalAdmissionController::global_instance->getPriority(res_group_name);
    if (unlikely(!priority.has_value()))
    {
        return false;
    }
    return LocalAdmissionController::isRUExhausted(*priority);
}

bool SegmentReadTaskPool::isRUExhausted()
{
    auto res = isRUExhaustedImpl();
    if (res)
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_ru_exhausted).Increment();
    }
    return res;
}

bool SegmentReadTaskPool::isRUExhaustedImpl()
{
    if (unlikely(res_group_name.empty() || LocalAdmissionController::global_instance == nullptr))
    {
        return false;
    }

    // To reduce lock contention in resource control,
    // check if RU is exhuasted every `bytes_of_one_hundred_ru` or every `100ms`.

    // Fast path.
    Int64 ms = currentMS();
    if (read_bytes_after_last_check < bytes_of_one_hundred_ru && ms - last_time_check_ru < check_ru_interval_ms)
    {
        return ru_is_exhausted; // Return result of last time.
    }

    std::lock_guard lock(ru_mu);
    // If last thread has check is ru exhausted, use the result of last thread.
    // Attention: `read_bytes_after_last_check` can be written concurrently in `pushBlock`.
    ms = currentMS();
    if (read_bytes_after_last_check < bytes_of_one_hundred_ru && ms - last_time_check_ru < check_ru_interval_ms)
    {
        return ru_is_exhausted; // Return result of last time.
    }

    // Check and reset everything.
    read_bytes_after_last_check = 0;
    ru_is_exhausted = checkIsRUExhausted(res_group_name);
    last_time_check_ru = ms;
    return ru_is_exhausted;
}

} // namespace DB::DM
