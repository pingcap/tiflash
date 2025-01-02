// Copyright 2023 PingCAP, Inc.
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
#include <Flash/Mpp/AsyncRequestHandler.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Flash/Statistics/ConnectionProfileInfo.h>

#include <memory>
#include <mutex>

namespace DB
{
constexpr Int32 batch_packet_count_v1 = 16;
struct Settings;

struct ExchangeReceiverResult
{
    std::shared_ptr<tipb::SelectResponse> resp;
    size_t call_index;
    String req_info;
    bool meet_error;
    bool eof;
    String error_msg;
    DecodeDetail decode_detail;

    ExchangeReceiverResult()
        : ExchangeReceiverResult(nullptr, 0)
    {}

    static ExchangeReceiverResult newOk(
        std::shared_ptr<tipb::SelectResponse> resp_,
        size_t call_index_,
        const String & req_info_)
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
        , eof(eof_)
        , error_msg(error_msg_)
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
        const Settings & settings);

    ~ExchangeReceiverBase();

    void cancel();
    void close();

    ReceiveStatus receive(size_t stream_id, ReceivedMessagePtr & recv_msg);
    ReceiveStatus tryReceive(size_t stream_id, ReceivedMessagePtr & recv_msg);

    ExchangeReceiverResult toExchangeReceiveResult(
        size_t stream_id,
        ReceiveStatus receive_status,
        ReceivedMessagePtr & recv_msg,
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
    uint64_t getFineGrainedShuffleStreamCount() const
    {
        return enable_fine_grained_shuffle_flag ? output_stream_count : 0;
    }
    int getExternalThreadCnt() const { return thread_count; }
    MemoryTracker * getMemoryTracker() const { return mem_tracker.get(); }
    std::atomic<Int64> * getDataSizeInQueue() { return &data_size_in_queue; }

    void verifyStreamId(size_t stream_id) const;
    const ConnectionProfileInfo::ConnTypeVec & getConnTypeVec() const;

private:
    std::shared_ptr<MemoryTracker> mem_tracker;
    using Request = typename RPCContext::Request;

    void readLoop(const Request & req);

    void reactor(const std::vector<Request> & async_requests);

    void setUpConnection();
    bool setEndState(ExchangeReceiverState new_state);
    String getStatusString();

    ExchangeReceiverResult handleUnnormalChannel(
        std::queue<Block> & block_queue,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

    DecodeDetail decodeChunks(
        size_t stream_id,
        const ReceivedMessagePtr & recv_msg,
        std::queue<Block> & block_queue,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

    void connectionDone(bool meet_error, const String & local_err_msg, const LoggerPtr & log);

    void waitAllConnectionDone();
    void waitLocalConnectionDone(std::unique_lock<std::mutex> & lock);
    void waitAsyncConnectionDone();

    void finishReceivedQueue();
    void cancelReceivedQueue();

    ExchangeReceiverResult toDecodeResult(
        size_t stream_id,
        std::queue<Block> & block_queue,
        const Block & header,
        const ReceivedMessagePtr & recv_msg,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

    void addLocalConnectionNum();
    void createAsyncRequestHandler(Request && request);

    void setUpLocalConnection(Request && req);
    void setUpSyncConnection(Request && req);
    void setUpAsyncConnection(std::vector<Request> && async_requests);

    void connectionLocalDone();
    void handleConnectionAfterException();

    void setUpConnectionWithReadLoop(Request && req);
    void setUpLocalConnections(std::vector<Request> & requests, bool has_remote_conn);

private:
    LoggerPtr exc_log;

    std::shared_ptr<RPCContext> rpc_context;

    const size_t source_num;
    const bool enable_fine_grained_shuffle_flag;
    const size_t output_stream_count;
    const size_t max_buffer_size;
    Int32 connection_uncreated_num;

    std::shared_ptr<ThreadManager> thread_manager;
    DAGSchema schema;

    ReceivedMessageQueue received_message_queue;

    std::vector<std::unique_ptr<AsyncRequestHandler<RPCContext>>> async_handler_ptrs;

    std::mutex mu;
    std::condition_variable cv;
    /// should lock `mu` when visit these members
    Int32 live_local_connections;
    Int32 live_connections;
    ExchangeReceiverState state;
    String err_msg;

    bool collected = false;
    int thread_count = 0;
    Int32 local_tunnel_version;
    Int32 async_recv_version;

    std::atomic<Int64> data_size_in_queue;
};

class ExchangeReceiver : public ExchangeReceiverBase<GRPCReceiverContext>
{
public:
    using Base = ExchangeReceiverBase<GRPCReceiverContext>;
    using Base::Base;
};

} // namespace DB
