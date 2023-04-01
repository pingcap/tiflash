// Copyright 2023 PingCAP, Ltd.
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

#include <Common/ThreadManager.h>
#include <Flash/Coprocessor/ChunkDecodeAndSquash.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/GRPCReceiverContext.h>

#include <future>
#include <mutex>
#include <thread>

namespace DB
{
constexpr Int32 batch_packet_count = 16;

struct ExchangeReceiverResult
{
    std::shared_ptr<tipb::SelectResponse> resp;
    size_t call_index;
    String req_info;
    bool meet_error;
    String error_msg;
    bool eof;
    DecodeDetail decode_detail;

    ExchangeReceiverResult()
        : ExchangeReceiverResult(nullptr, 0)
    {}

    static ExchangeReceiverResult newOk(std::shared_ptr<tipb::SelectResponse> resp_, size_t call_index_, const String & req_info_)
    {
        return {resp_, call_index_, req_info_, /*meet_error*/ false, /*error_msg*/ "", /*eof*/ false};
    }

    static ExchangeReceiverResult newEOF(const String & req_info_)
    {
        return {/*resp*/ nullptr, 0, req_info_, /*meet_error*/ false, /*error_msg*/ "", /*eof*/ true};
    }

    static ExchangeReceiverResult newError(size_t call_index, const String & req_info, const String & error_msg)
    {
        return {/*resp*/ nullptr, call_index, req_info, /*meet_error*/ true, error_msg, /*eof*/ false};
    }

private:
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
};

enum class ExchangeReceiverState
{
    NORMAL,
    ERROR,
    CANCELED,
    CLOSED,
};

enum class ReceiveStatus
{
    empty,
    ok,
    eof,
};

struct ReceiveResult
{
    ReceiveStatus recv_status;
    std::shared_ptr<ReceivedMessage> recv_msg;
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
        const String & req_id,
        const String & executor_id,
        uint64_t fine_grained_shuffle_stream_count,
        Int32 local_tunnel_version_,
        const std::vector<RequestAndRegionIDs> & disaggregated_dispatch_reqs_ = {});

    ~ExchangeReceiverBase();

    void cancel();
    void close();

    ReceiveResult receive(size_t stream_id);
    ReceiveResult nonBlockingReceive(size_t stream_id);

    ExchangeReceiverResult toExchangeReceiveResult(
        ReceiveResult & recv_result,
        std::queue<Block> & block_queue,
        const Block & header,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

    ExchangeReceiverResult nextResult(
        std::queue<Block> & block_queue,
        const Block & header,
        size_t stream_id,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

    const DAGSchema & getOutputSchema() const { return schema; }
    size_t getSourceNum() const { return source_num; }
    uint64_t getFineGrainedShuffleStreamCount() const { return enable_fine_grained_shuffle_flag ? output_stream_count : 0; }
    int getExternalThreadCnt() const { return thread_count; }
    std::vector<MsgChannelPtr> & getMsgChannels() { return msg_channels; }
    MemoryTracker * getMemoryTracker() const { return mem_tracker.get(); }
    std::atomic<Int64> * getDataSizeInQueue() { return &data_size_in_queue; }

private:
    std::shared_ptr<MemoryTracker> mem_tracker;
    using Request = typename RPCContext::Request;

    // Template argument enable_fine_grained_shuffle will be setup properly in setUpConnection().
    template <bool enable_fine_grained_shuffle>
    void readLoop(const Request & req);
    template <bool enable_fine_grained_shuffle>
    void reactor(const std::vector<Request> & async_requests);
    void setUpConnection();

    bool setEndState(ExchangeReceiverState new_state);
    String getStatusString();

    ExchangeReceiverResult handleUnnormalChannel(
        std::queue<Block> & block_queue,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

    DecodeDetail decodeChunks(
        const std::shared_ptr<ReceivedMessage> & recv_msg,
        std::queue<Block> & block_queue,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

    void connectionDone(
        bool meet_error,
        const String & local_err_msg,
        const LoggerPtr & log);

    void waitAllConnectionDone();
    void waitLocalConnectionDone(std::unique_lock<std::mutex> & lock);

    void finishAllMsgChannels();
    void cancelAllMsgChannels();

    ExchangeReceiverResult toDecodeResult(
        std::queue<Block> & block_queue,
        const Block & header,
        const std::shared_ptr<ReceivedMessage> & recv_msg,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

    ReceiveResult receive(
        size_t stream_id,
        std::function<MPMCQueueResult(size_t, std::shared_ptr<ReceivedMessage> &)> recv_func);

private:
    void prepareMsgChannels();
    void addLocalConnectionNum();
    void connectionLocalDone();
    void handleConnectionAfterException();

    void setUpConnectionWithReadLoop(Request && req);
    void setUpLocalConnections(std::vector<Request> & requests, bool has_remote_conn);

    bool isReceiverForTiFlashStorage()
    {
        // If not empty, need to send MPPTask to tiflash_storage.
        return !disaggregated_dispatch_reqs.empty();
    }

    std::shared_ptr<RPCContext> rpc_context;

    const tipb::ExchangeReceiver pb_exchange_receiver;
    const size_t source_num;
    const ::mpp::TaskMeta task_meta;
    const bool enable_fine_grained_shuffle_flag;
    const size_t output_stream_count;
    const size_t max_buffer_size;
    Int32 connection_uncreated_num;

    std::shared_ptr<ThreadManager> thread_manager;
    DAGSchema schema;

    std::vector<MsgChannelPtr> msg_channels;

    std::mutex mu;
    std::condition_variable cv;
    /// should lock `mu` when visit these members
    Int32 live_local_connections;
    Int32 live_connections;
    ExchangeReceiverState state;
    String err_msg;

    LoggerPtr exc_log;

    bool collected = false;
    int thread_count = 0;
    Int32 local_tunnel_version;

    std::atomic<Int64> data_size_in_queue;

    // For tiflash_compute node, need to send MPPTask to tiflash_storage node.
    std::vector<RequestAndRegionIDs> disaggregated_dispatch_reqs;
};

class ExchangeReceiver : public ExchangeReceiverBase<GRPCReceiverContext>
{
public:
    using Base = ExchangeReceiverBase<GRPCReceiverContext>;
    using Base::Base;
};

} // namespace DB
