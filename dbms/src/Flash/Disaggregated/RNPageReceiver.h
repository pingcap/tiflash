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

#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadManager.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Disaggregated/RNPageReceiverContext.h>
#include <kvproto/disaggregated.pb.h>

namespace DB
{
namespace DM
{
class RNRemoteSegmentReadTask;
using RNRemoteSegmentReadTaskPtr = std::shared_ptr<RNRemoteSegmentReadTask>;
} // namespace DM

enum class PageReceiverState
{
    NORMAL,
    ERROR,
    CANCELED,
    CLOSED,
};

struct PageReceivedMessage
{
    String req_info;
    DM::RNRemoteSegmentReadTaskPtr seg_task;
    const TrackedPageDataPacketPtr packet;
    const disaggregated::DisaggReadError * error_ptr;

    bool empty() const { return packet->pages_size() == 0 && packet->chunks_size() == 0; }
    // The serialized pages to be parsed as Page
    const auto & pages() const { return packet->pages(); }
    // The chunks to be parsed as Block
    const auto & chunks() const { return packet->chunks(); }

    PageReceivedMessage(
        const String & req_info_,
        const DM::RNRemoteSegmentReadTaskPtr & seg_task_,
        const TrackedPageDataPacketPtr & packet_,
        const disaggregated::DisaggReadError * error_ptr_)
        : req_info(req_info_)
        , seg_task(seg_task_)
        , packet(packet_)
        , error_ptr(error_ptr_)
    {
    }
};
using PageReceivedMessagePtr = std::shared_ptr<PageReceivedMessage>;


/// Detail of the packet that decoding in RNPageReceiverBase.decodeChunks
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

// `RNPageReceiver` starts background threads to keep
// - poping ** non-ready ** segment tasks from the task pool and fetch
//   the pages and mem-tables blocks through `FlashService::FetchDisaggPages`
//   from write nodes
// - receiving message from different write nodes and push them into msg_channel
template <typename RPCContext>
class RNPageReceiverBase
{
public:
    static constexpr auto name = "RNPageReceiver";

public:
    RNPageReceiverBase(
        std::unique_ptr<RPCContext> rpc_context_,
        size_t source_num_,
        size_t max_streams_,
        const String & req_id,
        const String & executor_id);

    ~RNPageReceiverBase();

    void cancel();

    void close();

    PageReceiverResult nextResult(std::unique_ptr<CHBlockChunkCodec> & decoder_ptr);

private:
    void setUpConnection();
    void readLoop();
    std::tuple<bool, String> taskReadLoop(const FetchPagesRequest & req);

    bool setEndState(PageReceiverState new_state);
    String getStatusString();

    void connectionDone(
        bool meet_error,
        const String & local_err_msg,
        const LoggerPtr & log);

    void finishAllMsgChannels();
    void cancelAllMsgChannels();

    PageReceiverResult toDecodeResult(
        const std::shared_ptr<PageReceivedMessage> & recv_msg,
        std::unique_ptr<CHBlockChunkCodec> & decoder_ptr);

    PageDecodeDetail decodeChunksAndPersistPages(
        const std::shared_ptr<PageReceivedMessage> & recv_msg,
        std::unique_ptr<CHBlockChunkCodec> & decoder_ptr);

private:
    std::unique_ptr<RPCContext> rpc_context;
    const size_t source_num;
    const size_t max_buffer_size;

    std::shared_ptr<ThreadManager> thread_manager;
    std::unique_ptr<MPMCQueue<PageReceivedMessagePtr>> msg_channel;

    std::mutex mu;
    /// should lock `mu` when visit these members
    Int32 live_connections;
    PageReceiverState state;
    String err_msg;

    bool collected;
    int thread_count;

    LoggerPtr exc_log;
};

class RNPageReceiver : public RNPageReceiverBase<GRPCPagesReceiverContext>
{
public:
    using Base = RNPageReceiverBase<GRPCPagesReceiverContext>;
    using Base::Base;
};

} // namespace DB
