// Copyright 2022 PingCAP, Ltd.
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

#include <Common/MPMCQueue.h>
#include <Common/ThreadManager.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/DecodeDetail.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
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
    size_t source_index;
    String req_info;
    // shared_ptr<const MPPDataPacket> is copied to make sure error_ptr, resp_ptr and chunks are valid.
    const std::shared_ptr<const MPPDataPacket> packet;
    const mpp::Error * error_ptr;
    const String * resp_ptr;
    std::vector<const String *> chunks;

    // Constructor that move chunks.
    ReceivedMessage(size_t source_index_,
                    const String & req_info_,
                    const std::shared_ptr<const MPPDataPacket> & packet_,
                    const mpp::Error * error_ptr_,
                    const String * resp_ptr_,
                    std::vector<const String *> && chunks_)
        : source_index(source_index_)
        , req_info(req_info_)
        , packet(packet_)
        , error_ptr(error_ptr_)
        , resp_ptr(resp_ptr_)
        , chunks(chunks_)
    {}
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

using MsgChannelPtr = std::unique_ptr<MPMCQueue<std::shared_ptr<ReceivedMessage>>>;

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
        uint64_t fine_grained_shuffle_stream_count);

    ~ExchangeReceiverBase();

    void cancel();

    void close();

    const DAGSchema & getOutputSchema() const { return schema; }

    ExchangeReceiverResult nextResult(
        std::queue<Block> & block_queue,
        const Block & header,
        size_t stream_id);

    size_t getSourceNum() const { return source_num; }
    uint64_t getFineGrainedShuffleStreamCount() const { return fine_grained_shuffle_stream_count; }

    int computeNewThreadCount() const { return thread_count; }

    void collectNewThreadCount(int & cnt)
    {
        if (!collected)
        {
            collected = true;
            cnt += computeNewThreadCount();
        }
    }

    void resetNewThreadCountCompute()
    {
        collected = false;
    }

private:
    using Request = typename RPCContext::Request;

    void setUpConnection();
    // Template argument enable_fine_grained_shuffle will be setup properly in setUpConnection().
    template <bool enable_fine_grained_shuffle>
    void readLoop(const Request & req);
    template <bool enable_fine_grained_shuffle>
    void reactor(const std::vector<Request> & async_requests);

    bool setEndState(ExchangeReceiverState new_state);
    ExchangeReceiverState getState();

    DecodeDetail decodeChunks(
        const std::shared_ptr<ReceivedMessage> & recv_msg,
        std::queue<Block> & block_queue,
        const Block & header);

    void connectionDone(
        bool meet_error,
        const String & local_err_msg,
        const LoggerPtr & log);

    void finishAllMsgChannels();
    void cancelAllMsgChannels();

    std::shared_ptr<RPCContext> rpc_context;

    const tipb::ExchangeReceiver pb_exchange_receiver;
    const size_t source_num;
    const ::mpp::TaskMeta task_meta;
    const size_t max_streams;
    const size_t max_buffer_size;

    std::shared_ptr<ThreadManager> thread_manager;
    DAGSchema schema;

    std::vector<MsgChannelPtr> msg_channels;

    std::mutex mu;
    /// should lock `mu` when visit these members
    Int32 live_connections;
    ExchangeReceiverState state;
    String err_msg;

    LoggerPtr exc_log;

    bool collected = false;
    int thread_count = 0;
    uint64_t fine_grained_shuffle_stream_count;
};

class ExchangeReceiver : public ExchangeReceiverBase<GRPCReceiverContext>
{
public:
    using Base = ExchangeReceiverBase<GRPCReceiverContext>;
    using Base::Base;
};

} // namespace DB
