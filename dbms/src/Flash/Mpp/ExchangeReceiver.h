#pragma once

#include <Common/RecyclableBuffer.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Flash/Mpp/getMPPTaskLog.h>
#include <Interpreters/Context.h>
#include <kvproto/mpp.pb.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#include <mutex>
#include <thread>

namespace DB
{
struct ReceivedMessage
{
    ReceivedMessage()
    {
        packet = std::make_shared<mpp::MPPDataPacket>();
    }
    std::shared_ptr<mpp::MPPDataPacket> packet;
    size_t source_index = 0;
    String req_info;
};

struct ExchangeReceiverResult
{
    std::shared_ptr<tipb::SelectResponse> resp;
    size_t call_index;
    String req_info;
    bool meet_error;
    String error_msg;
    bool eof;
    std::shared_ptr<ReceivedMessage> recv_msg;

    ExchangeReceiverResult(
        std::shared_ptr<tipb::SelectResponse> resp_,
        size_t call_index_,
        const String & req_info_ = "",
        bool meet_error_ = false,
        const String & error_msg_ = "",
        bool eof_ = false,
        std::shared_ptr<ReceivedMessage> msg = nullptr)
        : resp(resp_)
        , call_index(call_index_)
        , req_info(req_info_)
        , meet_error(meet_error_)
        , error_msg(error_msg_)
        , eof(eof_)
        , recv_msg(msg)
    {}

    ExchangeReceiverResult()
        : ExchangeReceiverResult(nullptr, 0)
    {}

    Int64 decodeChunks(std::queue<Block> & block_queue, const DAGSchema & schema, const DataTypes & expected_types)
    {
        assert(recv_msg != nullptr);
        Int64 rows = 0;
        int chunk_size = recv_msg->packet->chunks_size();
        if (chunk_size == 0)
            return rows;

        for (int i = 0; i < chunk_size; i++)
        {
            Block block = CHBlockChunkCodec().decode(recv_msg->packet->chunks(i), schema);
            rows += block.rows();
            if (unlikely(block.rows() == 0))
                continue;
            assertBlockSchema(expected_types, block, "decode chunks in exchange receiver");
            block_queue.push(std::move(block));
        }
        return rows;
    }
};

enum class ExchangeReceiverState
{
    NORMAL,
    ERROR,
    CANCELED,
    CLOSED,
};


template <typename RPCContext>
class ExchangeReceiverBase
{
public:
    static constexpr bool is_streaming_reader = true;

public:
    ExchangeReceiverBase(
        std::shared_ptr<RPCContext> rpc_context_,
        const ::tipb::ExchangeReceiver & exc,
        const ::mpp::TaskMeta & meta,
        size_t max_streams_,
        const LogWithPrefixPtr & log_);

    ~ExchangeReceiverBase();

    void cancel();

    const DAGSchema & getOutputSchema() const { return schema; }

    ExchangeReceiverResult nextResult();

    void returnEmptyMsg(ExchangeReceiverResult const * res);

    size_t getSourceNum() { return source_num; }
    String getName() { return "ExchangeReceiver"; }

private:
    void setUpConnection();

    void readLoop(size_t source_index);

    std::shared_ptr<RPCContext> rpc_context;

    const tipb::ExchangeReceiver pb_exchange_receiver;
    const size_t source_num;
    const ::mpp::TaskMeta task_meta;
    const size_t max_streams;
    const size_t max_buffer_size;

    std::vector<std::thread> workers;
    DAGSchema schema;

    std::mutex mu;
    std::condition_variable cv;
    /// should lock `mu` when visit these members
    RecyclableBuffer<ReceivedMessage> res_buffer;
    Int32 live_connections;
    ExchangeReceiverState state;
    String err_msg;

    LogWithPrefixPtr log;
};

class ExchangeReceiver : public ExchangeReceiverBase<GRPCReceiverContext>
{
public:
    using Base = ExchangeReceiverBase<GRPCReceiverContext>;
    using Base::Base;
};

} // namespace DB
