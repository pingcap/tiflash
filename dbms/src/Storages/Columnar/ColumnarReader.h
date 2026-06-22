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

class ColumnarReadTask;
using ColumnarReadTaskPtr = std::shared_ptr<ColumnarReadTask>;
class ColumnarReadTask
    : public boost::noncopyable
    , public std::enable_shared_from_this<ColumnarReadTask>
{
public:
    using RemoteTableRange = std::pair<TableID, pingcap::coprocessor::KeyRanges>;

    static std::vector<ColumnarReadTaskPtr> buildColumnarReadTaskWithBackoff(
        const LoggerPtr & log,
        const Context & context,
        UInt64 start_ts,
        const TiDBTableScan & table_scan,
        const FilterConditions & filter_conditions,
        const std::vector<RemoteTableRange> & remote_table_ranges,
        unsigned num_streams);

    static std::vector<ColumnarReadTaskPtr> buildColumnarReadTask(
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

    std::optional<ColumnarReaderWorkPtr> tryAcquireReaderWork();

#ifdef DBMS_PUBLIC_GTEST
    void replaceReaderWorkForTest(
        const ColumnarReaderWorkPtr & reader_work,
        std::vector<ColumnarReaderPlan> replanned_reader_plans);
#endif

    size_t getReaderCount() const;

    size_t getSourceNum() const;

    const Context & getContext() const;

    const LoggerPtr & getLog() const;

    const DM::ColumnDefines & getColumnsToRead() const;

    int getExtraTableIDIndex() const;

    TableID getLogicalTableID() const;

    const String & getExecutorID() const;

    ColumnarReadTask(
        std::vector<ColumnarReaderPlan> reader_plans,
        size_t source_num,
        std::shared_ptr<ColumnarReaderSharedContext> shared_reader_context);

private:
    void prefetchPendingWork();

    void prefetchReaderWork(const ColumnarReaderWorkPtr & reader_work);

    void replaceReaderWork(
        const ColumnarReaderWorkPtr & reader_work,
        std::vector<ColumnarReaderPlan> replanned_reader_plans);

    size_t reader_count;
    size_t source_num;
    std::shared_ptr<ColumnarReaderSharedContext> shared_reader_context;
    mutable std::mutex pending_reader_works_mutex;
    std::deque<ColumnarReaderWorkPtr> pending_reader_works;
};

// Free function to create a columnar reader via FFI.
ColumnarReaderPtr createColumnarReader(
    const ColumnarReaderSharedContext & shared_context,
    const ColumnarReaderPlan & reader_plan);

size_t getColumnarSourceNum(size_t num_streams, size_t reader_count);

} // namespace DB
#endif
