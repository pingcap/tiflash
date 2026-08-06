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
#include <Common/ThreadManager.h>
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
#include <deque>
#include <exception>
#include <mutex>
#include <optional>
#include <string_view>
#pragma GCC diagnostic pop

namespace DB
{
class DAGContext;

namespace DM
{
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;
} // namespace DM

enum class RNColumnarReaderMaterializeState
{
    NotStarted,
    Creating,
    Ready,
    Failed,
    Consumed,
};

struct RNColumnarReaderSharedContext;

struct RNColumnarReaderPlan
{
    RegionID region_id;
    RegionVersion region_ver;
    UInt64 region_conf_ver;
    std::vector<std::tuple<TableID, pingcap::coprocessor::KeyRanges>> physical_table_ranges;
};

struct RNColumnarReaderWork
{
    explicit RNColumnarReaderWork(RNColumnarReaderPlan plan_)
        : plan(std::move(plan_))
    {}

    ~RNColumnarReaderWork();

    RNColumnarReaderPlan plan;
    std::mutex mutex;
    std::condition_variable cv;
    RNColumnarReaderMaterializeState state = RNColumnarReaderMaterializeState::NotStarted;
    std::optional<ColumnarReaderPtr> reader;
    std::exception_ptr exception;
};

using RNColumnarReaderWorkPtr = std::shared_ptr<RNColumnarReaderWork>;

class RNColumnarReadTask;
using RNColumnarReadTaskPtr = std::shared_ptr<RNColumnarReadTask>;
class RNColumnarReadTask
    : public boost::noncopyable
    , public std::enable_shared_from_this<RNColumnarReadTask>
{
public:
    using RemoteTableRange = std::pair<TableID, pingcap::coprocessor::KeyRanges>;

    static std::vector<RNColumnarReadTaskPtr> buildColumnarReadTaskWithBackoff(
        const LoggerPtr & log,
        const Context & context,
        UInt64 start_ts,
        const TiDBTableScan & table_scan,
        const FilterConditions & filter_conditions,
        const std::vector<RemoteTableRange> & remote_table_ranges,
        unsigned num_streams);

    static std::vector<RNColumnarReadTaskPtr> buildColumnarReadTask(
        const LoggerPtr & log,
        const Context & context,
        UInt64 start_ts,
        const TiDBTableScan & table_scan,
        const FilterConditions & filter_conditions,
        const std::vector<RemoteTableRange> & remote_table_ranges,
        unsigned num_streams);

    BlockInputStreams getInputStreams();

    BlockInputStreamPtr createSharedInputStream();

    BlockInputStreamPtr createInputStream(const RNColumnarReaderWorkPtr & reader_work);

    ColumnarReaderPtr createColumnarReaderWithBackoff(const RNColumnarReaderWorkPtr & reader_work);

    ColumnarReaderPtr getOrCreateReader(const RNColumnarReaderWorkPtr & reader_work);

    std::optional<RNColumnarReaderWorkPtr> tryAcquireReaderWork();

#ifdef DBMS_PUBLIC_GTEST
    void replaceReaderWorkForTest(
        const RNColumnarReaderWorkPtr & reader_work,
        std::vector<RNColumnarReaderPlan> replanned_reader_plans);
#endif

    size_t getReaderCount() const;

    size_t getSourceNum() const;

    const Context & getContext() const;

    const LoggerPtr & getLog() const;

    const DM::ColumnDefines & getColumnsToRead() const;

    int getExtraTableIDIndex() const;

    TableID getLogicalTableID() const;

    const String & getExecutorID() const;

    RNColumnarReadTask(
        std::vector<RNColumnarReaderPlan> reader_plans,
        size_t source_num,
        std::shared_ptr<RNColumnarReaderSharedContext> shared_reader_context);

private:
    void prefetchPendingWork();

    void prefetchReaderWork(const RNColumnarReaderWorkPtr & reader_work);

    void replaceReaderWork(
        const RNColumnarReaderWorkPtr & reader_work,
        std::vector<RNColumnarReaderPlan> replanned_reader_plans);

    size_t reader_count;
    size_t source_num;
    std::shared_ptr<RNColumnarReaderSharedContext> shared_reader_context;
    mutable std::mutex pending_reader_works_mutex;
    std::deque<RNColumnarReaderWorkPtr> pending_reader_works;
};

class RNColumnarInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "RNProxy";

public:
    ~RNColumnarInputStream();

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
        RNColumnarReadTaskPtr task;
        RNColumnarReaderWorkPtr reader_work;
        const DM::ColumnDefines & columns_to_read;
        int extra_table_id_index;
        TableID table_id;
        const String & executor_id;
    };

    explicit RNColumnarInputStream(const Options & options)
        : context(options.context)
        , log(options.log)
        , task(options.task)
        , fixed_reader_work(options.reader_work)
        , action(options.columns_to_read, options.extra_table_id_index)
        , table_id(options.table_id)
        , executor_id(options.executor_id)
    {
        // Keep header aligned with genNamesAndTypesForTableScan when TiDB requests _tidb_tid on partition scans.
        setHeader(action.getHeader());
    }

    static BlockInputStreamPtr create(const Options & options)
    {
        return std::make_shared<RNColumnarInputStream>(options);
    }

private:
    bool ensureReader();
    void mergeReaderStats();
    void releaseReader();

    const Context & context;
    const LoggerPtr log;
    RNColumnarReadTaskPtr task;
    const RNColumnarReaderWorkPtr fixed_reader_work;
    RNColumnarReaderWorkPtr current_reader_work;
    std::optional<ColumnarReaderPtr> reader;
    AddExtraTableIDColumnTransformAction action;
    TableID table_id;
    const String executor_id;
    Block header;

    bool done = false;

    double duration_deserialize_sec = 0;
    double duration_read_sec = 0;
    UInt64 batch_size = 10240;
    UInt64 total_bytes = 0;
};

class RNColumnarSourceOp : public SourceOp
{
    static constexpr auto NAME = "RNProxy";

public:
    struct Options
    {
        PipelineExecutorContext & exec_context;
        RNColumnarReadTaskPtr task;
    };

    explicit RNColumnarSourceOp(const Options & options)
        : SourceOp(options.exec_context, options.task->getLog()->identifier())
        , context(options.task->getContext())
        , log(options.task->getLog())
        , task(options.task)
    {
        setHeader(AddExtraTableIDColumnTransformAction::buildHeader(
            options.task->getColumnsToRead(),
            options.task->getExtraTableIDIndex()));
    }

    static SourceOpPtr create(const Options & options) { return std::make_unique<RNColumnarSourceOp>(options); }

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
    RNColumnarReadTaskPtr task;
    UInt64 total_bytes = 0;
    size_t total_rows = 0;
    size_t total_streams = 0;

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
