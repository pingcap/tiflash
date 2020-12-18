#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
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
    DAGContext & dag_context;

    RemoteReaderPtr remote_reader;

    Block sample_block;

    std::queue<Block> block_queue;

    String name;

    Logger * log;

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
            dag_context.addRemoteExecutionSummariesForStreamingCall(*result.resp, result.call_index, remote_reader->getSourceNum());
        }
        else
        {
            dag_context.addRemoteExecutionSummariesForUnaryCall(*result.resp);
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
            LOG_TRACE(log, "decode packet" << std::to_string(block.rows()));
            block_queue.push(std::move(block));
        }
        return true;
    }

public:
    TiRemoteBlockInputStream(DAGContext & dag_context_, RemoteReaderPtr remote_reader_)
        : dag_context(dag_context_),
          remote_reader(remote_reader_),
          name("TiRemoteBlockInputStream(" + remote_reader->getName() + ")"),
          log(&Logger::get(name))
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
        Block block = block_queue.front();
        block_queue.pop();
        return block;
    }
};

using ExchangeReceiverInputStream = TiRemoteBlockInputStream<std::shared_ptr<ExchangeReceiver>, true>;
using CoprocessorBlockInputStream = TiRemoteBlockInputStream<std::shared_ptr<CoprocessorReader>, false>;
} // namespace DB
