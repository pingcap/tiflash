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

#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache_fwd.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/KVStore/Types.h>

namespace DB::DM
{
struct SegmentReadTask;
using SegmentReadTaskPtr = std::shared_ptr<SegmentReadTask>;
using SegmentReadTasks = std::list<SegmentReadTaskPtr>;

struct RemoteSegmentID
{
    KeyspaceID keyspace_id;
    StoreID store_id;
    TableID physical_table_id;
    UInt64 segment_id;

    String toString() const
    {
        return fmt::format(
            "RemoteSegmentID<store_id={} keyspace={} table_id={} segment_id={}>",
            store_id,
            keyspace_id,
            physical_table_id,
            segment_id);
    }
};

struct ExtraRemoteSegmentInfo
{
    RemoteSegmentID remote_segment_id;
    String store_address;
    DisaggTaskId snapshot_id;
    std::vector<UInt64> remote_page_ids;
    std::vector<UInt64> remote_page_sizes;
    DMContextPtr dm_context;
};

struct SegmentReadTask
{
    SegmentPtr segment;
    SegmentSnapshotPtr read_snapshot;
    RowKeyRanges ranges;

    std::optional<ExtraRemoteSegmentInfo> extra_remote_info;

    BlockInputStreamPtr input_stream;

    SegmentReadTask(
        const SegmentPtr & segment_, //
        const SegmentSnapshotPtr & read_snapshot_,
        const RowKeyRanges & ranges_ = {});

    SegmentReadTask(
        const LoggerPtr & log,
        const Context & db_context,
        const ScanContextPtr & scan_context,
        const RemotePb::RemoteSegment & proto,
        const DisaggTaskId & snapshot_id,
        StoreID store_id,
        const String & store_address,
        KeyspaceID keyspace_id,
        TableID physical_table_id);

    ~SegmentReadTask();

    void addRange(const RowKeyRange & range);

    void mergeRanges();

    static SegmentReadTasks trySplitReadTasks(const SegmentReadTasks & tasks, size_t expected_size);

    String info() const { return extra_remote_info.has_value() ? extra_remote_info->remote_segment_id.toString() : ""; }

    void initColumnFileDataProvider(const Remote::RNLocalPageCacheGuardPtr & pages_guard);
    void initInputStream(
        const ColumnDefines & columns_to_read,
        UInt64 read_tso,
        const PushDownFilterPtr & push_down_filter,
        ReadMode read_mode);
    BlockInputStreamPtr getInputStream() const
    {
        RUNTIME_CHECK(input_stream != nullptr);
        return input_stream;
    }
};

} // namespace DB::DM

template <>
struct fmt::formatter<DB::DM::SegmentReadTaskPtr>
{
    template <typename FormatContext>
    auto format(const DB::DM::SegmentReadTaskPtr & t, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(
            ctx.out(),
            "{}_{}_{}",
            t->segment->segmentId(),
            t->segment->segmentEpoch(),
            t->read_snapshot->delta->getDeltaIndexEpoch());
    }
};