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

#include <Common/CurrentMetrics.h>
#include <DataStreams/AddExtraTableIDColumnInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadResultChannel.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

#include <magic_enum.hpp>

namespace CurrentMetrics
{
extern const Metric DT_SegmentReadTasks;
}

namespace DB::DM
{
SegmentReadTask::SegmentReadTask(
    const SegmentPtr & segment_,
    const SegmentSnapshotPtr & read_snapshot_,
    const RowKeyRanges & ranges_)
    : segment(segment_)
    , read_snapshot(read_snapshot_)
    , ranges(ranges_)
{
    CurrentMetrics::add(CurrentMetrics::DT_SegmentReadTasks);
}

SegmentReadTask::~SegmentReadTask()
{
    CurrentMetrics::sub(CurrentMetrics::DT_SegmentReadTasks);
}

std::pair<size_t, size_t> SegmentReadTask::getRowsAndBytes() const
{
    return {read_snapshot->delta->getRows() + read_snapshot->stable->getRows(),
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
    auto block_size = std::max(expected_block_size, static_cast<size_t>(dm_context->db_context.getSettingsRef().dt_segment_stable_pack_rows));
    stream = t->segment->getInputStream(read_mode, *dm_context, columns_to_read, t->read_snapshot, t->ranges, filter, max_version, block_size);
    stream = std::make_shared<AddExtraTableIDColumnInputStream>(stream, extra_table_id_index, physical_table_id);
    LOG_DEBUG(log, "getInputStream succ, read_mode={}, segment_id={}", magic_enum::enum_name(read_mode), t->segment->segmentId());
    return stream;
}

SegmentReadTaskPool::SegmentReadTaskPool(
    uint64_t store_id_,
    int64_t physical_table_id_,
    size_t extra_table_id_index_,
    const DMContextPtr & dm_context_,
    const ColumnDefines & columns_to_read_,
    const PushDownFilterPtr & filter_,
    uint64_t max_version_,
    size_t expected_block_size_,
    ReadMode read_mode_,
    SegmentReadTasks && tasks_,
    AfterSegmentRead after_segment_read_,
    const String & debug_tag_,
    bool enable_read_thread_,
    Int64 num_streams_,
    SegmentReadResultChannelPtr result_channel_)
    : pool_id(nextPoolId())
    , store_id(store_id_)
    , physical_table_id(physical_table_id_)
    , mem_tracker(current_memory_tracker == nullptr ? nullptr : current_memory_tracker->shared_from_this())
    , debug_tag(fmt::format("{}#{}", debug_tag_, pool_id))
    , extra_table_id_index(extra_table_id_index_)
    , dm_context(dm_context_)
    , columns_to_read(columns_to_read_)
    , filter(filter_)
    , max_version(max_version_)
    , expected_block_size(expected_block_size_)
    , read_mode(read_mode_)
    , tasks_wrapper(enable_read_thread_, std::move(tasks_))
    , after_segment_read(after_segment_read_)
    , result_channel(result_channel_)
    , log(Logger::get(debug_tag))
    // If the queue is too short, only 1 in the extreme case, it may cause the computation thread
    // to encounter empty queues frequently, resulting in too much waiting and thread context
    // switching, so we limit the lower limit to 3, which provides two blocks of buffer space.
    , block_slot_limit(std::max(num_streams_, 3))
    // Limiting the minimum number of reading segments to 2 is to avoid, as much as possible,
    // situations where the computation may be faster and the storage layer may not be able to keep up.
    , active_segment_limit(std::max(num_streams_, 2))
{
    if (store_id == STORE_ID_I_DONT_CARE)
        RUNTIME_CHECK(dm_context->db_context.getSharedContextDisagg()->notDisaggregatedMode());
}

void SegmentReadTaskPool::initDefaultResultChannel()
{
    // This function must be explicitly called because `shared_from_this`
    // is not available in constructors.

    RUNTIME_CHECK(result_channel == nullptr);

    SegmentReadTaskPoolPtr this_ptr;
    try
    {
        this_ptr = shared_from_this();
    }
    catch (std::bad_weak_ptr & e)
    {
        RUNTIME_CHECK_MSG(false, "Must be called from SegmentReadTaskPoolPtr");
    }

    result_channel = SegmentReadResultChannel::create({
        .expected_sources = tasks_wrapper.getTasks().size(),
        .header = AddExtraTableIDColumnTransformAction::buildHeader(columns_to_read, extra_table_id_index),
        .debug_tag = fmt::format("{}->result_channel", debug_tag),
        .max_pending_blocks = static_cast<UInt64>(block_slot_limit),
        .on_first_read = [this_ptr](SegmentReadResultChannelPtr) {
            // Note: We created a circular reference here:
            // ReadTaskPool stores a ResultChannel, and ResultChannel also stores the ReadTaskPool
            // due to lambda capture.
            // In order to break the circular reference, ResultChannel will clean up the lambda capture
            // after first read, and also after all consumers are detached.
            // We cannot stores a weak reference inside the ResultChannel, because we want ResultChannel
            // to keep ReadTaskPool valid, until it submits to the scheduler.
            SegmentReadTaskScheduler::instance().add(this_ptr);
        },
    });
}

void SegmentReadTaskPool::finishSegment(const SegmentPtr & seg)
{
    if (after_segment_read)
        after_segment_read(dm_context, seg);
    bool pool_finished = false;
    {
        std::lock_guard lock(mutex);
        auto erased = active_segment_ids.erase(seg->segmentId());
        RUNTIME_CHECK(erased == 1, seg->segmentId(), active_segment_ids);
        result_channel->finish(fmt::format("seg_{}_{}", store_id, seg->segmentId()));
        pool_finished = active_segment_ids.empty() && tasks_wrapper.empty();
    }
    if (pool_finished)
        all_finished = true;

    LOG_DEBUG(log, "Finished reading segment, segment_id={} pool_finished={}", seg->segmentId(), pool_finished);
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

const std::unordered_map<UInt64, SegmentReadTaskPtr> & SegmentReadTaskPool::getTasks() const
{
    std::lock_guard lock(mutex);
    return tasks_wrapper.getTasks();
}

String SegmentReadTaskPool::info() const
{
    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("<pool_id={} table_id={}", pool_id, physical_table_id);
    if (store_id != STORE_ID_I_DONT_CARE)
        fmt_buf.fmtAppend(" store_id={}", store_id);
    fmt_buf.append(" segments=");

    std::lock_guard lock(mutex);
    {
        const auto & tasks = tasks_wrapper.getTasks();
        if (tasks.size() >= 10)
            fmt_buf.fmtAppend("({} segments)", tasks.size());
        else
        {
            fmt_buf.append("{");
            fmt_buf.joinStr(
                tasks.begin(),
                tasks.end(),
                [&](
                    const std::unordered_map<UInt64, SegmentReadTaskPtr>::const_iterator::value_type & value,
                    FmtBuffer & fb) {
                    fb.fmtAppend("{}", value.second->segment->segmentId());
                },
                ",");
            fmt_buf.append("}");
        }
    }

    fmt_buf.append(">");

    return fmt_buf.toString();
}

// Choose a segment to read.
// Returns <segment_id, pool_ids>.
std::unordered_map<uint64_t, std::vector<uint64_t>>::const_iterator SegmentReadTaskPool::scheduleSegment(const std::unordered_map<uint64_t, std::vector<uint64_t>> & segments, uint64_t expected_merge_count)
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
            throw DB::Exception(fmt::format("pool_id={} not found from merging segment {}=>{}", pool_id, itr->first, itr->second));
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
    auto block = stream->read();
    if (block)
    {
        result_channel->pushBlock(std::move(block));
        return true;
    }
    else
    {
        finishSegment(seg);
        return false;
    }
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

bool SegmentReadTaskPool::valid() const
{
    return !all_finished && getResultChannel()->valid();
}

} // namespace DB::DM
