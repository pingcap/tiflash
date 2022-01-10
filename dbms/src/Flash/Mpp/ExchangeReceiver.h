#pragma once

#include <Common/MPMCQueue.h>
#include <Common/ThreadManager.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/DecodeDetail.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Flash/Mpp/getMPPTaskLog.h>
#include <Interpreters/Context.h>
#include <kvproto/mpp.pb.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#include <future>
#include <mutex>
#include <thread>

namespace DB
{
struct ReceivedMessage
{
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
    DecodeDetail decode_detail;

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
    static constexpr auto name = "ExchangeReceiver";

public:
    ExchangeReceiverBase(
        std::shared_ptr<RPCContext> rpc_context_,
        size_t source_num_,
        size_t max_streams_,
        const LogWithPrefixPtr & log_);

    ~ExchangeReceiverBase();

    void cancel();

    const DAGSchema & getOutputSchema() const { return schema; }

    ExchangeReceiverResult nextResult(
        std::queue<Block> & block_queue,
        const Block & header);

    size_t getSourceNum() const { return source_num; }

private:
    using Request = typename RPCContext::Request;

    void setUpConnection();
    void readLoop(const Request & req);

    void setState(ExchangeReceiverState new_state);
    ExchangeReceiverState getState();

    DecodeDetail decodeChunks(
        const std::shared_ptr<ReceivedMessage> & recv_msg,
        std::queue<Block> & block_queue,
        const Block & header);

    void connectionDone(
        bool meet_error,
        const String & local_err_msg,
        const LogWithPrefixPtr & log);

    std::shared_ptr<RPCContext> rpc_context;

    const tipb::ExchangeReceiver pb_exchange_receiver;
    const size_t source_num;
    const ::mpp::TaskMeta task_meta;
    const size_t max_streams;
    const size_t max_buffer_size;

    std::shared_ptr<ThreadManager> thread_manager;
    DAGSchema schema;

    MPMCQueue<std::shared_ptr<ReceivedMessage>> msg_channel;

    std::mutex mu;
    /// should lock `mu` when visit these members
    Int32 live_connections;
    ExchangeReceiverState state;
    String err_msg;

    LogWithPrefixPtr exc_log;
};

class ExchangeReceiver : public ExchangeReceiverBase<GRPCReceiverContext>
{
public:
    using Base = ExchangeReceiverBase<GRPCReceiverContext>;
    using Base::Base;
};

} // namespace DB
