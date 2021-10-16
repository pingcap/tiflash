#pragma once

#include <Common/FiberPool.hpp>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Flash/Mpp/getMPPTaskLog.h>
#include <Interpreters/Context.h>
#include <kvproto/mpp.pb.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

namespace DB
{
struct ExchangeReceiverResult
{
    std::shared_ptr<tipb::SelectResponse> resp;
    size_t call_index;
    String req_info;
    bool meet_error;
    String error_msg;
    bool eof;

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

struct ReceivedPacket
{
    ReceivedPacket()
    {
        packet = std::make_shared<mpp::MPPDataPacket>();
    }
    std::shared_ptr<mpp::MPPDataPacket> packet;
    size_t source_index = 0;
    String req_info;
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

    std::vector<boost::fibers::future<void>> workers_done;
    DAGSchema schema;

    using Channel = boost::fibers::buffered_channel<std::shared_ptr<ReceivedPacket>>;
    std::vector<std::unique_ptr<Channel>> channels;

    boost::fibers::mutex mu;
    /// should lock `mu` when visit these members
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
