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

#include <Flash/Mpp/ExchangeReceiverCommon.h>
#include <Common/ThreadManager.h>
#include <Flash/Coprocessor/ChunkDecodeAndSquash.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Interpreters/Context.h>
#include <future>
#include <mutex>
#include <thread>

namespace DB
{
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
        size_t stream_id,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

    size_t getSourceNum() const { return source_num; }
    uint64_t getFineGrainedShuffleStreamCount() const { return enable_fine_grained_shuffle_flag ? output_stream_count : 0; }

    int getExternalThreadCnt() const { return thread_count; }

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

    void finishAllMsgChannels();
    void cancelAllMsgChannels();

    ExchangeReceiverResult toDecodeResult(
        std::queue<Block> & block_queue,
        const Block & header,
        const std::shared_ptr<ReceivedMessage> & recv_msg,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

private:
    std::shared_ptr<RPCContext> rpc_context;

    const tipb::ExchangeReceiver pb_exchange_receiver;
    const size_t source_num;
    const ::mpp::TaskMeta task_meta;
    const bool enable_fine_grained_shuffle_flag;
    const size_t output_stream_count;
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
};

class ExchangeReceiver : public ExchangeReceiverBase<GRPCReceiverContext>
{
public:
    using Base = ExchangeReceiverBase<GRPCReceiverContext>;
    using Base::Base;
};

} // namespace DB
