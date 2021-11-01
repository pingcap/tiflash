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
    Int64 rows;

    ExchangeReceiverResult(
        std::shared_ptr<tipb::SelectResponse> resp_,
        size_t call_index_,
        const String & req_info_ = "",
        bool meet_error_ = false,
        const String & error_msg_ = "",
        bool eof_ = false)
        : resp(resp_)
        , call_index(call_index_)
        , req_info(req_info_)
        , meet_error(meet_error_)
        , error_msg(error_msg_)
        , eof(eof_)
        , rows(0)
    {}

    ExchangeReceiverResult()
        : ExchangeReceiverResult(nullptr, 0)
    {}
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
        const LogWithPrefixPtr & log_,
        bool enable_local_tunnel_ = false);

    ~ExchangeReceiverBase();

    void cancel();

    const DAGSchema & getOutputSchema() const { return schema; }

    ExchangeReceiverResult nextResult(std::queue<Block> & block_queue, const DataTypes & expected_types);

    void returnEmptyMsg(std::shared_ptr<ReceivedMessage> & recv_msg);

    Int64 decodeChunks(std::shared_ptr<ReceivedMessage> & recv_msg, std::queue<Block> & block_queue, const DataTypes & expected_types);

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
    bool enable_local_tunnel;
};

class ExchangeReceiver : public ExchangeReceiverBase<GRPCReceiverContext>
{
public:
    using Base = ExchangeReceiverBase<GRPCReceiverContext>;
    using Base::Base;
};

} // namespace DB
