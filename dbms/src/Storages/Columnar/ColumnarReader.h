// Copyright 2026 PingCAP, Inc.
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

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Common/Logger.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/RegionCache.h>
#include <tipb/executor.pb.h>
#pragma GCC diagnostic pop

#include <condition_variable>
#include <deque>
#include <exception>
#include <mutex>
#include <optional>

namespace DB
{

class DAGContext;
class TiDBTableScan;

enum class ColumnarReaderMaterializeState
{
    NotStarted,
    Creating,
    Ready,
    Failed,
    Consumed,
};

/// Immutable context shared by all ColumnarReaderWork items within a
/// ColumnarReadTaskPool. Contains the serialized table scan request,
/// filter conditions, table info, and the FFI proxy handle needed to
/// materialize ColumnarReader instances via the Rust columnar engine.
struct ColumnarReaderSharedContext
{
    using ClearSharedSnapAccessByStartTsFn = void (*)(uint64_t, RaftStoreProxyPtr);

    struct StartTsClearRegistry
    {
        enum class UnregisterResult
        {
            NotRegistered,
            NotLastOwner,
            LastOwner,
        };

        std::mutex mutex;
        std::unordered_map<UInt64, UInt64> ref_counts;

        void registerStartTs(UInt64 start_ts)
        {
            if (start_ts == 0)
                return;
            auto guard = std::lock_guard(mutex);
            ++ref_counts[start_ts];
        }

        UnregisterResult unregisterStartTs(UInt64 start_ts)
        {
            if (start_ts == 0)
                return UnregisterResult::NotRegistered;

            auto guard = std::lock_guard(mutex);
            auto it = ref_counts.find(start_ts);
            if (it == ref_counts.end() || it->second == 0)
                return UnregisterResult::NotRegistered;
            --it->second;
            if (it->second != 0)
                return UnregisterResult::NotLastOwner;

            ref_counts.erase(it);
            return UnregisterResult::LastOwner;
        }
    };

    static StartTsClearRegistry & getStartTsClearRegistry()
    {
        static StartTsClearRegistry registry;
        return registry;
    }

    LoggerPtr log;
    const Context * context = nullptr;
    UInt64 start_ts = 0;
    DM::ColumnDefinesPtr column_defines;
    int extra_table_id_index = -1;
    TableID logical_table_id = 0;
    String executor_id;
    String table_scan_data;
    String filter_conditions_data;
    String table_info_data;
    String ann_query_info_data;
    String fts_query_info_data;
    RaftStoreProxyPtr proxy_ptr{};
    ClearSharedSnapAccessByStartTsFn clear_shared_snap_access_by_start_ts = nullptr;
    std::shared_ptr<std::mutex> output_lock = std::make_shared<std::mutex>();
    bool registered_for_start_ts = false;

    ~ColumnarReaderSharedContext() noexcept
    {
        if (!registered_for_start_ts)
            return;

        auto unregister_result = getStartTsClearRegistry().unregisterStartTs(start_ts);
        if (unregister_result != StartTsClearRegistry::UnregisterResult::LastOwner)
            return;

        if (proxy_ptr.inner == nullptr || clear_shared_snap_access_by_start_ts == nullptr)
            return;

        try
        {
            clear_shared_snap_access_by_start_ts(start_ts, proxy_ptr);
        }
        catch (...)
        {
            LOG_WARNING(log, "clear shared snapaccess cache failed, start_ts={}", start_ts);
        }
    }
};

struct ColumnarReaderPlan
{
    RegionID region_id;
    RegionVersion region_ver;
    UInt64 region_conf_ver;
    std::vector<std::tuple<TableID, pingcap::coprocessor::KeyRanges>> physical_table_ranges;
};

/// A unit of work representing one region (or bucket) to be read via the
/// columnar FFI. Each work item owns a ColumnarReaderPlan and drives a
/// state machine (NotStarted → Creating → Ready/Failed → Consumed) to
/// manage the lifecycle of the underlying Rust ColumnarReader.
struct ColumnarReaderWork
{
    explicit ColumnarReaderWork(ColumnarReaderPlan plan_)
        : plan(std::move(plan_))
    {}

    ~ColumnarReaderWork();

    ColumnarReaderPlan plan;
    std::mutex mutex;
    std::condition_variable cv;
    ColumnarReaderMaterializeState state = ColumnarReaderMaterializeState::NotStarted;
    std::optional<ColumnarReaderPtr> reader;
    std::exception_ptr exception;
};

using ColumnarReaderWorkPtr = std::shared_ptr<ColumnarReaderWork>;

class ColumnarReadTaskPool;
using ColumnarReadTaskPoolPtr = std::shared_ptr<ColumnarReadTaskPool>;

/// A pool of ColumnarReaderWork items that manages concurrent consumption by
/// multiple ColumnarInputStream / ColumnarSourceOp instances.
///
/// Each work item corresponds to a region (or bucket) to be read via the
/// columnar FFI.
class ColumnarReadTaskPool
    : public boost::noncopyable
    , public std::enable_shared_from_this<ColumnarReadTaskPool>
{
public:
    using RemoteTableRange = std::pair<TableID, pingcap::coprocessor::KeyRanges>;

    /// Build task pools with exponential backoff on transient errors
    /// (region epoch mismatch, snapshot errors). Stale region caches are
    /// dropped and key ranges are re-planned on retry.
    static std::vector<ColumnarReadTaskPoolPtr> buildWithBackoff(
        const LoggerPtr & log,
        const Context & context,
        UInt64 start_ts,
        const TiDBTableScan & table_scan,
        const FilterConditions & filter_conditions,
        const std::vector<RemoteTableRange> & remote_table_ranges,
        unsigned num_streams);

    /// Build task pools from remote table ranges. When num_streams exceeds
    /// the region count and ordering is not required, key ranges are further
    /// split by bucket boundaries obtained from the columnar proxy.
    static std::vector<ColumnarReadTaskPoolPtr> build(
        const LoggerPtr & log,
        const Context & context,
        UInt64 start_ts,
        const TiDBTableScan & table_scan,
        const FilterConditions & filter_conditions,
        const std::vector<RemoteTableRange> & remote_table_ranges,
        unsigned num_streams);

    BlockInputStreams getInputStreams();

    BlockInputStreamPtr createSharedInputStream();

    BlockInputStreamPtr createInputStream(const ColumnarReaderWorkPtr & reader_work);

    ColumnarReaderPtr createColumnarReaderWithBackoff(const ColumnarReaderWorkPtr & reader_work);

    ColumnarReaderPtr getOrCreateReader(const ColumnarReaderWorkPtr & reader_work);

    /// Pop the next unclaimed work item from the front of the queue and
    /// trigger prefetch of the following item. Returns std::nullopt when
    /// the queue is empty.
    std::optional<ColumnarReaderWorkPtr> tryAcquireReaderWork();

#ifdef DBMS_PUBLIC_GTEST
    void replaceReaderWorkForTest(
        const ColumnarReaderWorkPtr & reader_work,
        std::vector<ColumnarReaderPlan> replanned_reader_plans);
#endif

    size_t getReaderCount() const { return reader_count; }

    size_t getSourceNum() const { return source_num; }

    const Context & getContext() const { return *shared_reader_context->context; }

    const LoggerPtr & getLog() const { return shared_reader_context->log; }

    const DM::ColumnDefines & getColumnsToRead() const { return *shared_reader_context->column_defines; }

    int getExtraTableIDIndex() const { return shared_reader_context->extra_table_id_index; }

    TableID getLogicalTableID() const { return shared_reader_context->logical_table_id; }

    const String & getExecutorID() const { return shared_reader_context->executor_id; }

    ColumnarReadTaskPool(
        std::vector<ColumnarReaderPlan> reader_plans,
        size_t source_num,
        std::shared_ptr<ColumnarReaderSharedContext> shared_reader_context);

private:
    /// Materialize the next pending work item asynchronously via a detached
    /// thread, so it is ready when the next consumer calls tryAcquireReaderWork.
    void prefetchPendingWork();

    void prefetchReaderWork(const ColumnarReaderWorkPtr & reader_work);

    void replaceReaderWork(
        const ColumnarReaderWorkPtr & reader_work,
        std::vector<ColumnarReaderPlan> replanned_reader_plans);

    // Total number of ColumnarReaderWork items in this pool.
    size_t reader_count;

    // Number of concurrent consumers (ColumnarInputStream / ColumnarSourceOp)
    // that will pull work from this pool.
    size_t source_num;

    std::shared_ptr<ColumnarReaderSharedContext> shared_reader_context;

    mutable std::mutex pending_reader_works_mutex;

    // Queue of work items not yet claimed by any consumer.
    // Consumers call tryAcquireReaderWork() to pop from the front;
    // failed/replanned works are pushed back to the front for retry.
    std::deque<ColumnarReaderWorkPtr> pending_reader_works;
};

// Free function to create a columnar reader via FFI.
ColumnarReaderPtr createColumnarReader(
    const ColumnarReaderSharedContext & shared_context,
    const ColumnarReaderPlan & reader_plan);

#ifdef DBMS_PUBLIC_GTEST
using ColumnarPhysicalTableRanges = std::vector<std::tuple<TableID, pingcap::coprocessor::KeyRanges>>;
using BucketSplitUnit = std::pair<TableID, pingcap::coprocessor::KeyRange>;

struct BucketSplitResult
{
    bool has_bucket_split = false;
    std::vector<BucketSplitUnit> units;
};

struct ColumnarRegionReaderPlan
{
    RegionID region_id;
    pingcap::kv::RegionVerID region_ver_id;
    ColumnarPhysicalTableRanges physical_table_ranges;
    std::vector<BucketSplitUnit> bucket_units;
};

struct ColumnarRegionReaderPlansOutput
{
    size_t planned_reader_num = 0;
    size_t total_split_bucket_num = 0;
    std::vector<ColumnarRegionReaderPlan> region_reader_plans;
};

bool isBucketBoundaryInsideRange(const String & bucket_key, const pingcap::coprocessor::KeyRange & range);

BucketSplitResult splitRangesByBucketKeys(
    const ColumnarPhysicalTableRanges & physical_table_ranges,
    const std::vector<String> & bucket_keys);

std::vector<ColumnarReaderPlan> flattenColumnarRegionReaderPlans(
    const std::vector<ColumnarRegionReaderPlan> & region_reader_plans);

ColumnarRegionReaderPlansOutput buildColumnarRegionReaderPlans(
    const Context & context,
    const std::unordered_map<RegionID, ColumnarPhysicalTableRanges> & all_remote_regions_by_region,
    const std::unordered_map<RegionID, pingcap::kv::RegionVerID> & region_ver_ids,
    bool enable_bucket_parallel);
#endif

} // namespace DB
#endif
