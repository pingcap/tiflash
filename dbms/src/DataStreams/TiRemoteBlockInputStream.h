#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Flash/Mpp/ExchangeReceiver.h>
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
template <typename RemoteReaderPtr, bool is_streaming_reader>
class TiRemoteBlockInputStream : public IProfilingBlockInputStream
{
    RemoteReaderPtr remote_reader;

    Block sample_block;

    std::queue<Block> block_queue;

    String name;

    std::unordered_map<String, std::vector<ExecutionSummary>> execution_summaries;

    Logger * log;

    void addRemoteExecutionSummaries(tipb::SelectResponse & resp, size_t index, size_t concurrency, bool is_streaming_call)
    {
        for (auto & execution_summary : resp.execution_summaries())
        {
            if (execution_summary.has_executor_id())
            {
                auto & executor_id = execution_summary.executor_id();
                if (execution_summaries.find(executor_id) == execution_summaries.end())
                {
                    execution_summaries[executor_id].resize(concurrency);
                }
                auto & current_execution_summary = execution_summaries[executor_id][index];
                if (is_streaming_call)
                {
                    current_execution_summary.time_processed_ns += execution_summary.time_processed_ns();
                }
                else
                {
                    current_execution_summary.time_processed_ns
                        = std::max(current_execution_summary.time_processed_ns, execution_summary.time_processed_ns());
                }
                current_execution_summary.num_produced_rows += execution_summary.num_produced_rows();
                current_execution_summary.num_iterations += execution_summary.num_iterations();
                current_execution_summary.concurrency += execution_summary.concurrency();
            }
        }
    }

    bool fetchRemoteResult()
    {
        auto result = remote_reader->nextResult();
        if (result.meet_error)
        {
            LOG_WARNING(log, "remote reader meets error: " << result.error_msg);
            throw Exception(result.error_msg);
        }
        if (result.eof)
            return false;

        if constexpr (is_streaming_reader)
        {
            addRemoteExecutionSummaries(*result.resp, result.call_index, remote_reader->getSourceNum(), true);
        }
        else
        {
            addRemoteExecutionSummaries(*result.resp, 0, 1, false);
        }

        int chunk_size = result.resp->chunks_size();
        if (chunk_size == 0)
            return fetchRemoteResult();

        for (int i = 0; i < chunk_size; i++)
        {
            Block block;
            const tipb::Chunk & chunk = result.resp->chunks(i);
            switch (result.resp->encode_type())
            {
                case tipb::EncodeType::TypeCHBlock:
                    block = CHBlockChunkCodec().decode(chunk, remote_reader->getOutputSchema());
                    break;
                case tipb::EncodeType::TypeChunk:
                    block = ArrowChunkCodec().decode(chunk, remote_reader->getOutputSchema());
                    break;
                case tipb::EncodeType::TypeDefault:
                    block = DefaultChunkCodec().decode(chunk, remote_reader->getOutputSchema());
                    break;
                default:
                    throw Exception("Unsupported encode type", ErrorCodes::LOGICAL_ERROR);
            }
            LOG_DEBUG(log, "decode packet " << std::to_string(block.rows()) + " for " + result.req_info);
            if (unlikely(block.rows() == 0))
                continue;
            block_queue.push(std::move(block));
        }
        return true;
    }

public:
    explicit TiRemoteBlockInputStream(RemoteReaderPtr remote_reader_)
        : remote_reader(remote_reader_), name("TiRemoteBlockInputStream(" + remote_reader->getName() + ")"), log(&Logger::get(name))
    {
        // generate sample block
        ColumnsWithTypeAndName columns;
        for (auto & dag_col : remote_reader->getOutputSchema())
        {
            auto tp = getDataTypeByColumnInfo(dag_col.second);
            ColumnWithTypeAndName col(tp, dag_col.first);
            columns.emplace_back(col);
        }
        sample_block = Block(columns);
    }

    Block getHeader() const override { return sample_block; }

    String getName() const override { return name; }

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

    std::unordered_map<String, std::vector<ExecutionSummary>> & getRemoteExecutionSummaries() { return execution_summaries; }
};

using ExchangeReceiverInputStream = TiRemoteBlockInputStream<std::shared_ptr<ExchangeReceiver>, true>;
using CoprocessorBlockInputStream = TiRemoteBlockInputStream<std::shared_ptr<CoprocessorReader>, false>;
} // namespace DB
