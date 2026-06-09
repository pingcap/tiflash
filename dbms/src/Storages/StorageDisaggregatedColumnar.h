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

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Operators/Operator.h>
#include <Storages/IStorage.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <pingcap/kv/RegionCache.h>
#include <tipb/executor.pb.h>

#include <atomic>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <optional>
#include <string_view>
#pragma GCC diagnostic pop

namespace DB
{
class DAGContext;
class ThreadManager;

namespace DM
{
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;
} // namespace DM

struct RNProxyReaderSharedContext;

struct RNProxyReaderPlan
{
    RegionID region_id;
    RegionVersion region_ver;
    UInt64 region_conf_ver;
    std::vector<std::tuple<TableID, pingcap::coprocessor::KeyRanges>> physical_table_ranges;
};

enum class RNProxyReaderMaterializeState
{
    NotStarted,
    Creating,
    Ready,
    Failed,
    Consumed,
};

struct RNProxyReaderSlot
{
    ~RNProxyReaderSlot();

    std::mutex mutex;
    std::condition_variable cv;
    RNProxyReaderMaterializeState state = RNProxyReaderMaterializeState::NotStarted;
    std::optional<ColumnarReaderPtr> reader;
    std::exception_ptr exception;
};

class RNProxyReadTask;
using RNProxyReadTaskPtr = std::shared_ptr<RNProxyReadTask>;
class RNProxyReadTask
    : public boost::noncopyable
    , public std::enable_shared_from_this<RNProxyReadTask>
{
public:
    using RemoteTableRange = std::pair<TableID, pingcap::coprocessor::KeyRanges>;

    static std::vector<RNProxyReadTaskPtr> buildProxyReadTaskWithBackoff(
        const LoggerPtr & log,
        const Context & context,
        UInt64 start_ts,
        const TiDBTableScan & table_scan,
        const FilterConditions & filter_conditions,
        const std::vector<RemoteTableRange> & remote_table_ranges,
        unsigned num_streams);

    static std::vector<RNProxyReadTaskPtr> buildProxyReadTask(
        const LoggerPtr & log,
        const Context & context,
        UInt64 start_ts,
        const TiDBTableScan & table_scan,
        const FilterConditions & filter_conditions,
        const std::vector<RemoteTableRange> & remote_table_ranges,
        unsigned num_streams);

    BlockInputStreams getInputStreams();

    BlockInputStreamPtr createSharedInputStream();

    BlockInputStreamPtr createInputStream(size_t reader_index);

    ColumnarReaderPtr createColumnarReaderWithBackoff(size_t reader_index) const;

    ColumnarReaderPtr getOrCreateReader(size_t reader_index);

    void prefetchReader(size_t reader_index);

    std::optional<size_t> tryAcquireReaderIndex();

    size_t getReaderCount() const;

    size_t getSourceNum() const;

    const Context & getContext() const;

    const LoggerPtr & getLog() const;

    const DM::ColumnDefines & getColumnsToRead() const;

    int getExtraTableIDIndex() const;

    TableID getLogicalTableID() const;

    const String & getExecutorID() const;

    RNProxyReadTask(
        std::vector<RNProxyReaderPlan> reader_plans,
        size_t source_num,
        std::shared_ptr<RNProxyReaderSharedContext> shared_reader_context);

private:
    std::vector<RNProxyReaderPlan> reader_plans;
    size_t source_num;
    std::shared_ptr<RNProxyReaderSharedContext> shared_reader_context;
    std::vector<std::shared_ptr<RNProxyReaderSlot>> reader_slots;
    std::atomic_size_t next_reader_index = 0;
    std::once_flag prefetch_thread_manager_once;
    std::shared_ptr<ThreadManager> prefetch_thread_manager;
};

class RNProxyInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "RNProxy";

public:
    ~RNProxyInputStream();

    String getName() const { return NAME; }
    Block getHeader() const { return header; }
    void setHeader(const Block & header) { this->header = header; }
    Block read(FilterPtr & res_filter, bool return_filter);

protected:
    Block readImpl();
    Block readImpl(FilterPtr & res_filter, bool return_filter);

public:
    struct Options
    {
        const Context & context;
        LoggerPtr log;
        RNProxyReadTaskPtr task;
        std::optional<size_t> reader_index;
        const DM::ColumnDefines & columns_to_read;
        int extra_table_id_index;
        TableID table_id;
        const String & executor_id;
    };

    explicit RNProxyInputStream(const Options & options)
        : context(options.context)
        , log(options.log)
        , task(options.task)
        , fixed_reader_index(options.reader_index)
        , action(options.columns_to_read, options.extra_table_id_index)
        , table_id(options.table_id)
        , executor_id(options.executor_id)
    {
        // Keep header aligned with genNamesAndTypesForTableScan when TiDB requests _tidb_tid on partition scans.
        setHeader(action.getHeader());
    }

    static BlockInputStreamPtr create(const Options & options) { return std::make_shared<RNProxyInputStream>(options); }

private:
    bool ensureReader();
    void releaseReader();

    const Context & context;
    const LoggerPtr log;
    RNProxyReadTaskPtr task;
    const std::optional<size_t> fixed_reader_index;
    std::optional<size_t> current_reader_index;
    std::optional<ColumnarReaderPtr> reader;
    AddExtraTableIDColumnTransformAction action;
    TableID table_id;
    const String & executor_id;
    Block header;

    bool done = false;

    double duration_deserialize_sec = 0;
    double duration_read_sec = 0;
    UInt64 batch_size = 10240;
    UInt64 total_bytes = 0;
};

class RNProxySourceOp : public SourceOp
{
    static constexpr auto NAME = "RNProxy";

public:
    struct Options
    {
        PipelineExecutorContext & exec_context;
        RNProxyReadTaskPtr task;
    };

    explicit RNProxySourceOp(const Options & options)
        : SourceOp(options.exec_context, options.task->getLog()->identifier())
        , context(options.task->getContext())
        , log(options.task->getLog())
        , task(options.task)
    {
        setHeader(AddExtraTableIDColumnTransformAction::buildHeader(
            options.task->getColumnsToRead(),
            options.task->getExtraTableIDIndex()));
    }

    static SourceOpPtr create(const Options & options) { return std::make_unique<RNProxySourceOp>(options); }

    String getName() const override { return NAME; }

    IOProfileInfoPtr getIOProfileInfo() const override { return IOProfileInfo::createForLocal(profile_info_ptr); }

protected:
    void operateSuffixImpl() override;

    void operatePrefixImpl() override;

    OperatorStatus readImpl(Block & block) override;

    OperatorStatus awaitImpl() override;

    OperatorStatus executeIOImpl() override;

private:
    const Context & context;
    const LoggerPtr log;
    RNProxyReadTaskPtr task;
    UInt64 total_bytes = 0;
    size_t total_rows = 0;
    size_t total_streams = 0;

    std::optional<size_t> current_reader_idx;
    BlockInputStreamPtr current_input_stream;

    // Temporarily store the block read from current_seg_task->stream and pass it to downstream operators in readImpl.
    std::optional<Block> t_block = std::nullopt;

    bool done = false;
    // Count the time spent waiting for segment tasks to be ready.
    //double duration_wait_ready_task_sec = 0;
    Stopwatch total_cost_watch{CLOCK_MONOTONIC_COARSE};

    // Count the time consumed by reading blocks in the stream of segment tasks.
    double duration_read_sec = 0;
};
} // namespace DB
#endif
