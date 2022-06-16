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
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>

#include <mutex>

#include "Interpreters/Context.h"
#include "common/logger_useful.h"

namespace CurrentMetrics
{
extern const Metric DT_SegmentReadTasks;
}

namespace DB::DM
{
SegmentReadTask::SegmentReadTask(const SegmentPtr & segment_, //
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
{
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
    for (auto & task : tasks)
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

BlockInputStreamPtr SegmentReadTaskPool::getInputStream(uint64_t seg_id)
{
    auto t = getTask(seg_id);
    if (t == nullptr)
    {
        return nullptr;
    }

    auto seg = t->segment;
    BlockInputStreamPtr stream;
    if (is_raw)
    {
        stream = seg->getInputStreamRaw(*dm_context, columns_to_read, t->read_snapshot, do_range_filter_for_raw);
    }
    else
    {
        auto block_size = std::max(expected_block_size, static_cast<size_t>(dm_context->db_context.getSettingsRef().dt_segment_stable_pack_rows));
        stream = seg->getInputStream(*dm_context, columns_to_read, t->read_snapshot, t->ranges, filter, max_version, block_size);
    }
    return stream;
}

void SegmentReadTaskPool::activeSegment(UInt64 seg_id)
{
    // No need lock
    active_segment_ids.insert(seg_id);
}

void SegmentReadTaskPool::finishSegment(UInt64 seg_id)
{
    LOG_FMT_DEBUG(log, "seg_id {}", seg_id);
    std::lock_guard lock(mutex);
    active_segment_ids.erase(seg_id);
    if (active_segment_ids.empty() && tasks.empty())
    {
        q.finish();
        SegmentReadTaskScheduler::instance().del(id);
    }
}

SegmentReadTaskPtr SegmentReadTaskPool::getTask(uint64_t seg_id)
{
    std::lock_guard lock(mutex);
    auto itr = std::find_if(tasks.begin(), tasks.end(), [seg_id](const SegmentReadTaskPtr & task) { return task->segment->segmentId() == seg_id; });
    if (itr == tasks.end())
    {
        return nullptr;
    }
    auto t = *(itr);
    tasks.erase(itr);
    activeSegment(seg_id);
    return t;
}


void SegmentReadTaskScheduler::add(SegmentReadTaskPoolPtr & pool)
{
    std::lock_guard lock(mtx);
    pools[pool->getId()] = pool;
    std::vector<UInt64> seg_ids;
    for (const auto & task : pool->getTasks())
    {
        segments[task->segment->segmentId()].push_back(pool->getId());
        seg_ids.push_back(task->segment->segmentId());
    }
    LOG_FMT_DEBUG(log, "pool {} segments {} => {}", pool->getId(), seg_ids.size(), seg_ids);
}

void SegmentReadTaskScheduler::del(UInt64 pool_id)
{
    std::lock_guard lock(mtx);
    pools.erase(pool_id);
    LOG_FMT_DEBUG(log, "pool {} segments {}", pool_id, segments.size());
}

DMFileReaderPool & DMFileReaderPool::instance()
{
    static DMFileReaderPool reader_pool;
    return reader_pool;
}

void DMFileReaderPool::add(DMFileReader & reader)
{
    std::lock_guard lock(mtx);
    readers[reader.fileId()].insert(&reader);
}

void DMFileReaderPool::del(DMFileReader & reader)
{
    std::lock_guard lock(mtx);
    auto itr = readers.find(reader.fileId());
    if (itr == readers.end())
    {
        return;
    }
    itr->second.erase(&reader);
    if (itr->second.empty())
    {
        readers.erase(itr);
    }
}

void DMFileReaderPool::set(DMFileReader & from_reader, int64_t col_id, size_t start, size_t count, ColumnPtr & col)
{
    std::lock_guard lock(mtx);
    auto itr = readers.find(from_reader.fileId());
    if (itr == readers.end())
    {
        return;
    }
    for (auto * r : itr->second)
    {
        if (&from_reader == r)
        {
            continue;
        }
        r->addCachedPacks(col_id, start, count, col);
    }
}

} // namespace DB::DM
