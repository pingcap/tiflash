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

class RNProxyReader;
using RNProxyReaderPtr = std::shared_ptr<RNProxyReader>;
class RNProxyReader : boost::noncopyable
{
public:
    static RNProxyReaderPtr createProxyReader(
        const LoggerPtr & log,
        const Context & context,
        RegionID region_id,
        RegionVersion region_ver,
        UInt64 region_conf_ver,
        const std::vector<std::tuple<TableID, pingcap::coprocessor::KeyRanges>> & partition_table_ranges,
        UInt64 start_ts,
        const TiDBTableScan & table_scan,
        const FilterConditions & filter_conditions,
        std::mutex & output_lock);

    BlockInputStreamPtr getInputStream() const
    {
        RUNTIME_CHECK(input_stream != nullptr);
        return input_stream;
    }

    RNProxyReader(BlockInputStreamPtr input_stream)
        : input_stream(input_stream)
    {}

private:
    BlockInputStreamPtr input_stream;
};

class RNProxyReadTask;
using RNProxyReadTaskPtr = std::shared_ptr<RNProxyReadTask>;
class RNProxyReadTask : boost::noncopyable
{
public:
    using RemoteTableRange = std::pair<TableID, pingcap::coprocessor::KeyRanges>;
    const std::vector<RNProxyReaderPtr> proxy_readers;

    static RNProxyReadTaskPtr create(const std::vector<RNProxyReaderPtr> & proxy_readers)
    {
        return std::shared_ptr<RNProxyReadTask>(new RNProxyReadTask(proxy_readers));
    }

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

    BlockInputStreams getInputStreams() const;

    std::vector<RNProxyReaderPtr> getProxyReaders() { return proxy_readers; }

    RNProxyReadTask(const std::vector<RNProxyReaderPtr> & proxy_readers)
        : proxy_readers(proxy_readers)
    {}
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
        std::string_view debug_tag;
        const DM::ColumnDefines & columns_to_read;
        ColumnarReaderPtr reader;
        int extra_table_id_index;
        TableID table_id;
        const String & executor_id;
    };

    explicit RNProxyInputStream(const Options & options)
        : context(options.context)
        , log(Logger::get(options.debug_tag))
        , reader(options.reader)
        , action(options.columns_to_read, options.extra_table_id_index)
        , table_id(options.table_id)
        , executor_id(options.executor_id)
    {
        setHeader(toEmptyBlock(options.columns_to_read));
    }

    static BlockInputStreamPtr create(const Options & options) { return std::make_shared<RNProxyInputStream>(options); }

private:
    const Context & context;
    const LoggerPtr log;
    ColumnarReaderPtr reader;
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
        const Context & context;
        std::string_view debug_tag;
        PipelineExecutorContext & exec_context;
        const DM::ColumnDefines & columns_to_read;
        RNProxyReadTaskPtr task;
        int extra_table_id_index;
    };

    explicit RNProxySourceOp(const Options & options)
        : SourceOp(options.exec_context, String(options.debug_tag))
        , context(options.context)
        , log(Logger::get(options.debug_tag))
        , task(options.task)
        , action(options.columns_to_read, options.extra_table_id_index)
    {
        setHeader(toEmptyBlock(options.columns_to_read));
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
    AddExtraTableIDColumnTransformAction action;
    size_t total_rows = 0;

    Int32 current_reader_idx = -1;

    // Temporarily store the block read from current_seg_task->stream and pass it to downstream operators in readImpl.
    std::optional<Block> t_block = std::nullopt;

    bool done = false;
    // Count the time spent waiting for segment tasks to be ready.
    //double duration_wait_ready_task_sec = 0;
    Stopwatch wait_stop_watch{CLOCK_MONOTONIC_COARSE};

    // Count the time consumed by reading blocks in the stream of segment tasks.
    double duration_read_sec = 0;
};
} // namespace DB
#endif
