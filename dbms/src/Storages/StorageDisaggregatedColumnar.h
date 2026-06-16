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
#include <Common/ThreadManager.h>
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/IStorage.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <pingcap/kv/RegionCache.h>
#include <tipb/executor.pb.h>

#include <condition_variable>
#include <deque>
#include <exception>
#include <mutex>
#include <optional>
#pragma GCC diagnostic pop

namespace DB
{
class DAGContext;
class PipelineExecGroupBuilder;
class PipelineExecutorContext;

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

/// NotifyFuture adapter so pipeline tasks can wait on reader materialize without blocking an IO thread.
/// Delegates task registration and wakeup to an internal PipeConditionVariable.
struct RNColumnarReaderNotifyFuture : public NotifyFuture
{
    void registerTask(TaskPtr && task) override
    {
        task->setNotifyType(NotifyType::WAIT_ON_TABLE_SCAN_READ);
        pipe_cv.registerTask(std::move(task));
    }

    void notifyAll() { pipe_cv.notifyAll(); }

    PipeConditionVariable pipe_cv;
};

struct RNColumnarReaderWork
{
    explicit RNColumnarReaderWork(RNColumnarReaderPlan plan_)
        : plan(std::move(plan_))
    {}

    ~RNColumnarReaderWork();

    RNColumnarReaderPlan plan;
    std::mutex mutex;
    // cv is kept for the stream path (RNColumnarInputStream) which still uses blocking wait.
    std::condition_variable cv;
    // notify_future is kept for experimental one-shot/latch implementations; the current pipeline path
    // does not wait on reader materialize with WAIT_FOR_NOTIFY.
    RNColumnarReaderNotifyFuture notify_future;
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

    /// Start asynchronous materialize for a given work on a detached thread.
    /// Atomically transitions NotStarted → Creating and kicks off columnar reader
    /// creation with backoff. No-op if the work is already in a later state.
    /// Used by the pipeline path so the IO thread is never blocked on materialize.
    void startAsyncMaterializeReader(const RNColumnarReaderWorkPtr & reader_work);

    /// Non-blocking read attempt for the pipeline path.
    /// Returns the reader if state is Ready (and transitions to Consumed).
    /// Returns std::nullopt if state is NotStarted or Creating.
    /// Throws if state is Failed or Consumed (error state).
    std::optional<ColumnarReaderPtr> tryGetReadyReader(const RNColumnarReaderWorkPtr & reader_work);

    std::optional<RNColumnarReaderWorkPtr> tryAcquireReaderWork(bool enable_prefetch = true);

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

    /// Set by the pipeline path before any source ops are created so
    /// async prefetch tasks can be submitted to the scheduler.
    void setPipelineExecutorContext(PipelineExecutorContext * ctx) { exec_context = ctx; }

private:
    void prefetchPendingWork();

    void prefetchReaderWork(const RNColumnarReaderWorkPtr & reader_work);

    void replaceReaderWork(
        const RNColumnarReaderWorkPtr & reader_work,
        std::vector<RNColumnarReaderPlan> replanned_reader_plans);

    size_t reader_count;
    size_t source_num;
    std::shared_ptr<RNColumnarReaderSharedContext> shared_reader_context;
    PipelineExecutorContext * exec_context = nullptr;
    mutable std::mutex pending_reader_works_mutex;
    std::deque<RNColumnarReaderWorkPtr> pending_reader_works;
};

class RNColumnarInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "RNProxy";

public:
    ~RNColumnarInputStream() override;

    String getName() const override { return NAME; }
    Block getHeader() const override { return header; }
    void setHeader(const Block & header) { this->header = header; }
    Block read(FilterPtr & res_filter, bool return_filter) override;

protected:
    Block readImpl() override;
    Block readImpl(FilterPtr & res_filter, bool return_filter) override;

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

    explicit RNColumnarInputStream(const Options & options);

    static BlockInputStreamPtr create(const Options & options)
    {
        return std::make_shared<RNColumnarInputStream>(options);
    }

    /// Create an input stream that already owns a materialized reader, bypassing ensureReader().
    static BlockInputStreamPtr createWithReader(const Options & options, ColumnarReaderPtr reader);

private:
    bool ensureReader();
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

#ifdef DBMS_PUBLIC_GTEST
void addColumnarNullSourceForTest(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    const TiDBTableScan & table_scan,
    const LoggerPtr & log);

void addColumnarTableScanProfileInfosForTest(
    const Context & context,
    PipelineExecGroupBuilder & group_builder,
    const TiDBTableScan & table_scan);
#endif

} // namespace DB
#endif
