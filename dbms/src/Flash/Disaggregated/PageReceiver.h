#pragma once

#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/ThreadManager.h>
#include <Flash/Disaggregated/GRPCPageReceiverContext.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <kvproto/mpp.pb.h>

namespace DB
{
namespace DM
{
class RemoteSegmentReadTask;
using RemoteSegmentReadTaskPtr = std::shared_ptr<RemoteSegmentReadTask>;
} // namespace DM

struct PageReceivedMessage
{
    String req_info;
    DM::RemoteSegmentReadTaskPtr seg_task;
    const TrackedPageDataPacketPtr packet;
    const mpp::Error * error_ptr;

    bool empty() const { return packet->pages_size() == 0 && packet->chunks_size() == 0; }
    // The serialized pages to be parsed as Page
    const auto & pages() const { return packet->pages(); }
    // The chunks to be parsed as Block
    const auto & chunks() const { return packet->chunks(); }

    PageReceivedMessage(
        const String & req_info_,
        const DM::RemoteSegmentReadTaskPtr & seg_task_,
        const TrackedPageDataPacketPtr & packet_,
        const mpp::Error * error_ptr_)
        : req_info(req_info_)
        , seg_task(seg_task_)
        , packet(packet_)
        , error_ptr(error_ptr_)
    {
    }
};
using PageReceivedMessagePtr = std::shared_ptr<PageReceivedMessage>;


/// Detail of the packet that decoding in PageReceiverBase.decodeChunks
struct PageDecodeDetail
{
    // Responding packets count, usually be 1, be 0 when flush data before eof
    Int64 packets = 1;

    // The row number of all blocks of the original packet
    Int64 rows = 0;

    // Total byte size of the origin packet
    Int64 packet_bytes = 0;

    // The pages of the original packet
    Int64 pages = 0;
};
struct PageReceiverResult
{
    enum class Type
    {
        Ok,
        Eof,
        Error,
    };

    Type type;
    String req_info;
    String error_msg;
    // details to collect execution summary
    PageDecodeDetail decode_detail;

    static PageReceiverResult newOk(const String & req_info_)
    {
        return PageReceiverResult{Type::Ok, req_info_, /*error_msg*/ ""};
    }

    static PageReceiverResult newEOF(const String & req_info_)
    {
        return PageReceiverResult{Type::Eof, req_info_, /*error_msg*/ ""};
    }

    static PageReceiverResult newError(const String & req_info, const String & error_msg)
    {
        return PageReceiverResult{Type::Error, req_info, error_msg};
    }

    bool ok() const { return type == Type::Ok; }
    bool eof() const { return type == Type::Eof; }

private:
    explicit PageReceiverResult(
        Type type_,
        const String & req_info_ = "",
        const String & error_msg_ = "")
        : type(type_)
        , req_info(req_info_)
        , error_msg(error_msg_)
    {}
};

template <typename RPCContext>
class PageReceiverBase
{
public:
    static constexpr auto name = "PageReceiver";

public:
    PageReceiverBase(
        std::unique_ptr<RPCContext> rpc_context_,
        size_t source_num_,
        size_t max_streams_,
        const String & req_id,
        const String & executor_id);

    ~PageReceivedMessage();

    void cancel();

    void close();

    PageReceiverResult nextResult(
        size_t stream_id,
        std::unique_ptr<CHBlockChunkCodec> & decoder_ptr);

private:
    using Request = typename RPCContext::Request;

    void setUpConnection();
    void readLoop();
    std::tuple<bool, String> taskReadLoop(const Request & req);

    bool setEndState(ExchangeReceiverState new_state);
    String getStatusString();

    void connectionDone(
        bool meet_error,
        const String & local_err_msg,
        const LoggerPtr & log);

    // TODO the persist logic may belong to another class
    void setUpPersist();
    void persistLoop(size_t idx);
    bool consumeOneResult(const LoggerPtr & log);

    void finishAllMsgChannels();
    void cancelAllMsgChannels();

    PageReceiverResult toDecodeResult(
        const std::shared_ptr<PageReceivedMessage> & recv_msg,
        std::unique_ptr<CHBlockChunkCodec> & decoder_ptr);

    PageDecodeDetail decodeChunks(
        const std::shared_ptr<PageReceivedMessage> & recv_msg,
        std::unique_ptr<CHBlockChunkCodec> & decoder_ptr);

private:
    std::unique_ptr<RPCContext> rpc_context;
    const size_t source_num;
    const size_t max_buffer_size;
    const size_t persist_threads_num;

    std::shared_ptr<ThreadManager> thread_manager;
    std::vector<std::unique_ptr<MPMCQueue<PageReceivedMessagePtr>>> msg_channels;

    std::mutex mu;
    /// should lock `mu` when visit these members
    Int32 live_connections;
    ExchangeReceiverState state;
    String err_msg;

    bool collected;
    int thread_count;

    LoggerPtr exc_log;

    // below members are for persist threads
    std::atomic<size_t> total_rows;
    std::atomic<size_t> total_pages;
};

class PageReceiver : public PageReceiverBase<GRPCPagesReceiverContext>
{
public:
    using Base = PageReceiverBase<GRPCPagesReceiverContext>;
    using Base::Base;
};

} // namespace DB
