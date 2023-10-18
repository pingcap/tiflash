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
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Remote/RNDataProvider.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/RowKeyRangeUtils.h>
#include <Storages/DeltaMerge/SegmentReadTask.h>

namespace CurrentMetrics
{
extern const Metric DT_SegmentReadTasks;
}

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

SegmentReadTask::SegmentReadTask(
    const LoggerPtr & log,
    const Context & db_context,
    const ScanContextPtr & scan_context,
    const RemotePb::RemoteSegment & proto,
    const DisaggTaskId & snapshot_id,
    StoreID store_id,
    const String & store_address,
    KeyspaceID keyspace_id,
    TableID physical_table_id)
{
    CurrentMetrics::add(CurrentMetrics::DT_SegmentReadTasks);
    auto tracing_id = fmt::format(
        "{} segment_id={} epoch={} delta_epoch={}",
        log->identifier(),
        proto.segment_id(),
        proto.segment_epoch(),
        proto.delta_index_epoch());

    auto rb = ReadBufferFromString(proto.key_range());
    auto segment_range = RowKeyRange::deserialize(rb);

    auto dm_context = std::make_shared<DMContext>(
        db_context,
        /* path_pool */ nullptr,
        /* storage_pool */ nullptr,
        /* min_version */ 0,
        keyspace_id,
        physical_table_id,
        /* is_common_handle */ segment_range.is_common_handle,
        /* rowkey_column_size */ segment_range.rowkey_column_size,
        db_context.getSettingsRef(),
        scan_context,
        tracing_id);

    segment = std::make_shared<Segment>(
        Logger::get(),
        /*epoch*/ 0,
        segment_range,
        proto.segment_id(),
        /*next_segment_id*/ 0,
        nullptr,
        nullptr);

    read_snapshot = Remote::Serializer::deserializeSegmentSnapshotFrom(
        *dm_context,
        store_id,
        keyspace_id,
        physical_table_id,
        proto);

    ranges.reserve(proto.read_key_ranges_size());
    for (const auto & read_key_range : proto.read_key_ranges())
    {
        auto rb = ReadBufferFromString(read_key_range);
        ranges.push_back(RowKeyRange::deserialize(rb));
    }

    const auto & cfs = read_snapshot->delta->getPersistedFileSetSnapshot()->getColumnFiles();
    std::vector<UInt64> remote_page_ids;
    std::vector<size_t> remote_page_sizes;
    remote_page_ids.reserve(cfs.size());
    remote_page_sizes.reserve(cfs.size());
    for (const auto & cf : cfs)
    {
        if (auto * tiny = cf->tryToTinyFile(); tiny)
        {
            remote_page_ids.emplace_back(tiny->getDataPageId());
            remote_page_sizes.emplace_back(tiny->getDataPageSize());
        }
    }

    extra_remote_info.emplace(ExtraRemoteSegmentInfo{
        .remote_segment_id
        = {.keyspace_id = keyspace_id,
           .store_id = store_id,
           .physical_table_id = physical_table_id,
           .segment_id = proto.segment_id()},
        .store_address = store_address,
        .snapshot_id = snapshot_id,
        .remote_page_ids = std::move(remote_page_ids),
        .remote_page_sizes = std::move(remote_page_sizes),
        .dm_context = dm_context,
    });

    LOG_DEBUG(
        read_snapshot->log,
        "memtable_cfs_count={} persisted_cfs_count={} remote_page_ids={} delta_index={} store_address={}",
        read_snapshot->delta->getMemTableSetSnapshot()->getColumnFileCount(),
        cfs.size(),
        extra_remote_info->remote_page_ids,
        read_snapshot->delta->getSharedDeltaIndex()->toString(),
        store_address);
}

SegmentReadTask::~SegmentReadTask()
{
    CurrentMetrics::sub(CurrentMetrics::DT_SegmentReadTasks);
}

void SegmentReadTask::addRange(const RowKeyRange & range)
{
    ranges.push_back(range);
}

void SegmentReadTask::mergeRanges()
{
    ranges = DM::tryMergeRanges(std::move(ranges), 1);
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

void SegmentReadTask::initColumnFileDataProvider(const Remote::RNLocalPageCacheGuardPtr & pages_guard)
{
    auto & data_provider = read_snapshot->delta->getPersistedFileSetSnapshot()->data_provider;
    RUNTIME_CHECK(std::dynamic_pointer_cast<ColumnFileDataProviderNop>(data_provider));

    RUNTIME_CHECK(extra_remote_info.has_value());
    auto page_cache = extra_remote_info->dm_context->db_context.getSharedContextDisagg()->rn_page_cache;
    const auto & remote_seg_id = extra_remote_info->remote_segment_id;
    data_provider = std::make_shared<Remote::ColumnFileDataProviderRNLocalPageCache>(
        page_cache,
        pages_guard,
        remote_seg_id.store_id,
        KeyspaceTableID{remote_seg_id.keyspace_id, remote_seg_id.physical_table_id});
}

void SegmentReadTask::initInputStream(
    const ColumnDefines & columns_to_read,
    UInt64 read_tso,
    const PushDownFilterPtr & push_down_filter,
    ReadMode read_mode)
{
    RUNTIME_CHECK(input_stream == nullptr);
    input_stream = segment->getInputStream(
        read_mode,
        *(extra_remote_info->dm_context),
        columns_to_read,
        read_snapshot,
        ranges,
        push_down_filter,
        read_tso,
        DEFAULT_BLOCK_SIZE);
}

} // namespace DB::DM
