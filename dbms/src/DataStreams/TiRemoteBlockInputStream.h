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

#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

#include <chrono>
#include <mutex>
#include <thread>
#include <utility>

namespace DB
{
// TiRemoteBlockInputStream is a block input stream that read/receive data from remote.
template <typename RemoteReader>
class TiRemoteBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr bool is_streaming_reader = RemoteReader::is_streaming_reader;

    std::shared_ptr<RemoteReader> remote_reader;
    size_t source_num;
    std::vector<ConnectionProfileInfo> connection_profile_infos;

    Block sample_block;

    std::queue<Block> block_queue;

    String name;

    /// this atomic variable is kind of a lock for the struct of execution_summaries:
    /// if execution_summaries_inited[index] = true, the map execution_summaries[index]
    /// itself will not be modified, so DAGResponseWriter can read it safely, otherwise,
    /// DAGResponseWriter will just skip execution_summaries[index]
    std::vector<std::atomic<bool>> execution_summaries_inited;
    std::vector<std::unordered_map<String, ExecutionSummary>> execution_summaries;

    const LoggerPtr log;

    uint64_t total_rows;

    void initRemoteExecutionSummaries(tipb::SelectResponse & resp, size_t index)
    {
        for (auto & execution_summary : resp.execution_summaries())
        {
            if (execution_summary.has_executor_id())
            {
                auto & executor_id = execution_summary.executor_id();
                execution_summaries[index][executor_id].time_processed_ns = execution_summary.time_processed_ns();
                execution_summaries[index][executor_id].num_produced_rows = execution_summary.num_produced_rows();
                execution_summaries[index][executor_id].num_iterations = execution_summary.num_iterations();
                execution_summaries[index][executor_id].concurrency = execution_summary.concurrency();
            }
        }
        execution_summaries_inited[index].store(true);
    }

    void addRemoteExecutionSummaries(tipb::SelectResponse & resp, size_t index, bool is_streaming_call)
    {
        if (resp.execution_summaries_size() == 0)
            return;
        if (!execution_summaries_inited[index].load())
        {
            initRemoteExecutionSummaries(resp, index);
            return;
        }
        auto & execution_summaries_map = execution_summaries[index];
        for (auto & execution_summary : resp.execution_summaries())
        {
            if (execution_summary.has_executor_id())
            {
                auto & executor_id = execution_summary.executor_id();
                if (unlikely(execution_summaries_map.find(executor_id) == execution_summaries_map.end()))
                {
                    LOG_FMT_WARNING(log, "execution {} not found in execution_summaries, this should not happen", executor_id);
                    continue;
                }
                auto & current_execution_summary = execution_summaries_map[executor_id];
                if (is_streaming_call)
                {
                    current_execution_summary.time_processed_ns
                        = std::max(current_execution_summary.time_processed_ns, execution_summary.time_processed_ns());
                    current_execution_summary.num_produced_rows
                        = std::max(current_execution_summary.num_produced_rows, execution_summary.num_produced_rows());
                    current_execution_summary.num_iterations
                        = std::max(current_execution_summary.num_iterations, execution_summary.num_iterations());
                    current_execution_summary.concurrency
                        = std::max(current_execution_summary.concurrency, execution_summary.concurrency());
                }
                else
                {
                    current_execution_summary.time_processed_ns
                        = std::max(current_execution_summary.time_processed_ns, execution_summary.time_processed_ns());
                    current_execution_summary.num_produced_rows += execution_summary.num_produced_rows();
                    current_execution_summary.num_iterations += execution_summary.num_iterations();
                    current_execution_summary.concurrency += execution_summary.concurrency();
                }
            }
        }
    }

    bool fetchRemoteResult()
    {
        auto result = remote_reader->nextResult(block_queue, sample_block);
        if (result.meet_error)
        {
            LOG_FMT_WARNING(log, "remote reader meets error: {}", result.error_msg);
            throw Exception(result.error_msg);
        }
        if (result.eof)
            return false;
        if (result.resp != nullptr && result.resp->has_error())
        {
            LOG_FMT_WARNING(log, "remote reader meets error: {}", result.resp->error().DebugString());
            throw Exception(result.resp->error().DebugString());
        }
        /// only the last response contains execution summaries
        if (result.resp != nullptr)
        {
            if constexpr (is_streaming_reader)
            {
                addRemoteExecutionSummaries(*result.resp, result.call_index, true);
            }
            else
            {
                addRemoteExecutionSummaries(*result.resp, 0, false);
            }
        }

        const auto & decode_detail = result.decode_detail;

        size_t index = 0;
        if constexpr (is_streaming_reader)
            index = result.call_index;

        ++connection_profile_infos[index].packets;
        connection_profile_infos[index].bytes += decode_detail.packet_bytes;

        total_rows += decode_detail.rows;
        LOG_FMT_TRACE(
            log,
            "recv {} rows from remote for {}, total recv row num: {}",
            decode_detail.rows,
            result.req_info,
            total_rows);
        if (decode_detail.rows == 0)
            return fetchRemoteResult();
        return true;
    }

public:
    TiRemoteBlockInputStream(std::shared_ptr<RemoteReader> remote_reader_, const String & req_id, const String & executor_id)
        : remote_reader(remote_reader_)
        , source_num(remote_reader->getSourceNum())
        , name(fmt::format("TiRemoteBlockInputStream({})", RemoteReader::name))
        , execution_summaries_inited(source_num)
        , log(Logger::get(name, req_id, executor_id))
        , total_rows(0)
    {
        // generate sample block
        ColumnsWithTypeAndName columns;
        for (auto & dag_col : remote_reader->getOutputSchema())
        {
            auto tp = getDataTypeByColumnInfoForComputingLayer(dag_col.second);
            ColumnWithTypeAndName col(tp, dag_col.first);
            columns.emplace_back(col);
        }
        for (size_t i = 0; i < source_num; i++)
        {
            execution_summaries_inited[i].store(false);
        }
        execution_summaries.resize(source_num);
        connection_profile_infos.resize(source_num);
        sample_block = Block(columns);
    }

    Block getHeader() const override { return sample_block; }

    String getName() const override { return name; }

    void cancel(bool kill) override
    {
        if (kill)
            remote_reader->cancel();
    }
    Block readImpl() override
    {
        if (block_queue.empty())
        {
            if (!fetchRemoteResult())
                return {};
        }
        // todo should merge some blocks to make sure the output block is big enough
        Block block = block_queue.front();
        block_queue.pop();
        return block;
    }

    const std::unordered_map<String, ExecutionSummary> * getRemoteExecutionSummaries(size_t index)
    {
        return execution_summaries_inited[index].load() ? &execution_summaries[index] : nullptr;
    }

    size_t getSourceNum() const { return source_num; }
    bool isStreamingCall() const { return is_streaming_reader; }
    const std::vector<ConnectionProfileInfo> & getConnectionProfileInfos() const { return connection_profile_infos; }

    virtual void collectNewThreadCountOfThisLevel(int & cnt) override
    {
        remote_reader->collectNewThreadCount(cnt);
    }

    virtual void resetNewThreadCountCompute() override
    {
        if (collected)
        {
            collected = false;
            remote_reader->resetNewThreadCountCompute();
        }
    }

protected:
    virtual void readSuffixImpl() override
    {
        LOG_FMT_DEBUG(log, "finish read {} rows from remote", total_rows);
        remote_reader->close();
    }
};

using ExchangeReceiverInputStream = TiRemoteBlockInputStream<ExchangeReceiver>;
using CoprocessorBlockInputStream = TiRemoteBlockInputStream<CoprocessorReader>;
} // namespace DB
