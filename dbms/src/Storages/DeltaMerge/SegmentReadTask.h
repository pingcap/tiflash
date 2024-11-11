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

#include <Storages/DeltaMerge/DMContext_fwd.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/KVStore/Types.h>

namespace DB::DM
{
struct SegmentReadTask;
using SegmentReadTaskPtr = std::shared_ptr<SegmentReadTask>;
using SegmentReadTasks = std::list<SegmentReadTaskPtr>;

namespace tests
{
class SegmentReadTaskTest;
class DMStoreForSegmentReadTaskTest;
} // namespace tests

// A SegmentReadTask object is identified by <store_id, keyspace_id, physical_table_id, segment_id, segment_epoch>.
// Under disagg arch, there could be SegmentReadTasks from different stores in one compute node.
struct GlobalSegmentID
{
    StoreID store_id;
    KeyspaceID keyspace_id;
    TableID physical_table_id;
    UInt64 segment_id;
    UInt64 segment_epoch;
};

struct ExtraRemoteSegmentInfo
{
    String store_address;
    // DisaggTaskId is corresponding to a storage snapshot in write node.
    // Returned by EstablishDisaggTask and used by FetchDisaggPages.
    // The index pages of ColumnFileTiny are also included.
    DisaggTaskId snapshot_id;
    std::vector<UInt64> remote_page_ids;
    std::vector<size_t> remote_page_sizes;
};

struct SegmentReadTask
{
public:
    const StoreID store_id;
    SegmentPtr segment; // Contains segment_id, segment_epoch
    SegmentSnapshotPtr read_snapshot;
    DMContextPtr dm_context; // Contains keyspace_id, physical_table_id
    RowKeyRanges ranges;

    std::optional<ExtraRemoteSegmentInfo> extra_remote_info;

    // Constructor for op-mode.
    SegmentReadTask(
        const SegmentPtr & segment_,
        const SegmentSnapshotPtr & read_snapshot_,
        const DMContextPtr & dm_context_,
        const RowKeyRanges & ranges_ = {});

    // Constructor for disaggregated-mode.
    SegmentReadTask(
        const LoggerPtr & log,
        const Context & db_context,
        const ScanContextPtr & scan_context,
        const RemotePb::RemoteSegment & proto,
        const DisaggTaskId & snapshot_id,
        StoreID store_id,
        const String & store_address,
        KeyspaceID keyspace_id,
        TableID physical_table_id,
        ColumnID pk_col_id);

    ~SegmentReadTask();

    GlobalSegmentID getGlobalSegmentID() const;

    void addRange(const RowKeyRange & range);

    void mergeRanges();

    static SegmentReadTasks trySplitReadTasks(const SegmentReadTasks & tasks, size_t expected_size);

    void fetchPages();

    void initInputStream(
        const ColumnDefines & columns_to_read,
        UInt64 start_ts,
        const PushDownFilterPtr & push_down_filter,
        ReadMode read_mode,
        size_t expected_block_size,
        bool enable_delta_index_error_fallback);

    BlockInputStreamPtr getInputStream() const
    {
        RUNTIME_CHECK(input_stream != nullptr);
        return input_stream;
    }

    // WN calls hasColumnFileToFetch to check whether a SegmentReadTask need to fetch column files from it
    bool hasColumnFileToFetch() const;

    String toString() const;

private:
    std::vector<Remote::PageOID> buildRemotePageOID() const;

    Remote::RNLocalPageCache::OccupySpaceResult blockingOccupySpaceForTask() const;

    disaggregated::FetchDisaggPagesRequest buildFetchPagesRequest(
        const std::vector<Remote::PageOID> & pages_not_in_cache) const;

    void doFetchPages(const disaggregated::FetchDisaggPagesRequest & request);
    void doFetchPagesImpl(
        std::function<bool(disaggregated::PagesPacket &)> && read_packet,
        std::unordered_set<UInt64> remaining_pages_to_fetch);
    void checkMemTableSet(const ColumnFileSetSnapshotPtr & mem_table_snap) const;
    bool needFetchMemTableSet() const;
    void checkMemTableSetReady() const;

    void initColumnFileDataProvider(const Remote::RNLocalPageCacheGuardPtr & pages_guard);

    bool doInitInputStreamWithErrorFallback(
        const ColumnDefines & columns_to_read,
        UInt64 start_ts,
        const PushDownFilterPtr & push_down_filter,
        ReadMode read_mode,
        size_t expected_block_size,
        bool enable_delta_index_error_fallback);

    void doInitInputStream(
        const ColumnDefines & columns_to_read,
        UInt64 start_ts,
        const PushDownFilterPtr & push_down_filter,
        ReadMode read_mode,
        size_t expected_block_size);

    void finishPagesPacketStream(std::unique_ptr<grpc::ClientReader<disaggregated::PagesPacket>> & stream);

    BlockInputStreamPtr input_stream;

    friend tests::SegmentReadTaskTest;
    friend tests::DMStoreForSegmentReadTaskTest;
};

// Used in SegmentReadTaskScheduler, SegmentReadTaskPool.
using MergingSegments = std::unordered_map<GlobalSegmentID, std::vector<UInt64>>;

} // namespace DB::DM

template <>
struct fmt::formatter<DB::DM::SegmentReadTask>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::DM::SegmentReadTask & t, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", t.toString());
    }
};

template <>
struct fmt::formatter<DB::DM::SegmentReadTaskPtr>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::DM::SegmentReadTaskPtr & t, FormatContext & ctx) const
    {
        return fmt::formatter<DB::DM::SegmentReadTask>().format(*t, ctx);
    }
};

template <>
struct fmt::formatter<DB::DM::GlobalSegmentID>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::DM::GlobalSegmentID & t, FormatContext & ctx) const
    {
        return fmt::format_to(
            ctx.out(),
            "s{}_ks{}_t{}_{}_{}",
            t.store_id,
            t.keyspace_id,
            t.physical_table_id,
            t.segment_id,
            t.segment_epoch);
    }
};

template <>
struct std::hash<DB::DM::GlobalSegmentID>
{
    size_t operator()(const DB::DM::GlobalSegmentID & seg) const
    {
        size_t seed = 0;
        boost::hash_combine(seed, boost::hash_value(seg.store_id));
        boost::hash_combine(seed, boost::hash_value(seg.keyspace_id));
        boost::hash_combine(seed, boost::hash_value(seg.physical_table_id));
        boost::hash_combine(seed, boost::hash_value(seg.segment_id));
        boost::hash_combine(seed, boost::hash_value(seg.segment_epoch));
        return seed;
    }
};

template <>
struct std::equal_to<DB::DM::GlobalSegmentID>
{
    bool operator()(const DB::DM::GlobalSegmentID & a, const DB::DM::GlobalSegmentID & b) const
    {
        return a.store_id == b.store_id && a.keyspace_id == b.keyspace_id && a.physical_table_id == b.physical_table_id
            && a.segment_id == b.segment_id && a.segment_epoch == b.segment_epoch;
    }
};
