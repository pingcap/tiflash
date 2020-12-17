#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
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

// ExchangeReceiver is in charge of receiving data from exchangeSender located in upstream tasks.
class ExchangeReceiverInputStream : public IProfilingBlockInputStream
{
    DAGContext & dag_context;

    std::shared_ptr<ExchangeReceiver> receiver;

    Block sample_block;

    std::queue<Block> block_queue;

    Logger * log;

    bool fetchExchangeRecieverResult()
    {
        auto result = receiver->nextResult();
        if (result.meet_error)
        {
            LOG_WARNING(log, "ExchangeReceiver meets error: " << result.error_msg);
            throw Exception(result.error_msg);
        }
        if (result.eof)
            return false;

        dag_context.addRemoteExecutionSummariesForStreamingCall(*result.resp, result.call_index, receiver->getSourceNum());
        int chunk_size = result.resp->chunks_size();
        if (chunk_size == 0)
            return fetchExchangeRecieverResult();

        for (int i = 0; i < chunk_size; i++)
        {
            Block block;
            const tipb::Chunk & chunk = result.resp->chunks(i);
            switch (result.resp->encode_type())
            {
                case tipb::EncodeType::TypeCHBlock:
                    block = CHBlockChunkCodec().decode(chunk, receiver->getOutputSchema());
                    break;
                case tipb::EncodeType::TypeChunk:
                    block = ArrowChunkCodec().decode(chunk, receiver->getOutputSchema());
                    break;
                case tipb::EncodeType::TypeDefault:
                    block = DefaultChunkCodec().decode(chunk, receiver->getOutputSchema());
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
    ExchangeReceiverInputStream(DAGContext & dag_context_, std::shared_ptr<ExchangeReceiver> receiver_)
        : dag_context(dag_context_), receiver(std::move(receiver_)), log(&Logger::get("ExchangeReceiverInputStream"))
    {
        // generate sample block
        ColumnsWithTypeAndName columns;
        for (auto & dag_col : receiver->getOutputSchema())
        {
            auto tp = getDataTypeByColumnInfo(dag_col.second);
            ColumnWithTypeAndName col(tp, dag_col.first);
            columns.emplace_back(col);
        }
        sample_block = Block(columns);
    }

    Block getHeader() const override { return sample_block; }

    String getName() const override { return "ExchangeReceiver"; }

    Block readImpl() override
    {
        if (block_queue.empty())
        {
            if (!fetchExchangeRecieverResult())
                return {};
        }
        Block block = block_queue.front();
        block_queue.pop();
        return block;
    }
};
} // namespace DB
