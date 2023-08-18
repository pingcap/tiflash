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

#include <Common/CPUAffinityManager.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/ThreadManager.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Disaggregated/RNPageReceiver.h>
#include <Flash/Disaggregated/RNPageReceiverContext.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Remote/RNRemoteReadTask.h>
#include <common/logger_useful.h>
#include <kvproto/disaggregated.pb.h>

#include <magic_enum.hpp>
#include <memory>
#include <mutex>

namespace DB
{
namespace details
{
String constructStatusString(PageReceiverState state, const String & error_message)
{
    if (error_message.empty())
        return fmt::format("Receiver state: {}", magic_enum::enum_name(state));
    return fmt::format("Receiver state: {}, error message: {}", magic_enum::enum_name(state), error_message);
}
bool pushPacket(const DM::RNRemoteSegmentReadTaskPtr & seg_task,
                const String & req_info,
                const TrackedPageDataPacketPtr & tracked_packet,
                std::unique_ptr<MPMCQueue<PageReceivedMessagePtr>> & msg_channel,
                LoggerPtr & log)
{
    bool push_succeed = true;

    const disaggregated::DisaggReadError * error_ptr = nullptr;
    auto & packet = *tracked_packet;
    if (packet.has_error())
        error_ptr = &packet.error();

    if (!(error_ptr == nullptr // no error
          && packet.chunks_size() == 0 && packet.pages_size() == 0 // empty
          ))
    {
        auto recv_msg = std::make_shared<PageReceivedMessage>(
            req_info,
            seg_task,
            tracked_packet,
            error_ptr);

        // We record pending msg before pushing to channel, because the msg may be consumed immediately
        // after the push.
        seg_task->addPendingMsg();
        push_succeed = msg_channel->push(std::move(recv_msg)) == MPMCQueueResult::OK;
    }

    LOG_TRACE(log, "push recv_msg to msg_channels(size: {}) succeed:{}", 1, push_succeed);
    return push_succeed;
}
} // namespace details

template <typename RPCContext>
RNPageReceiverBase<RPCContext>::RNPageReceiverBase(
    std::unique_ptr<RPCContext> rpc_context_,
    size_t source_num_,
    size_t max_streams_,
    const String & req_id,
    const String & executor_id)
    : rpc_context(std::move(rpc_context_))
    , source_num(source_num_)
    , max_buffer_size(std::max<size_t>(16, max_streams_ * 2))
    , thread_manager(newThreadManager())
    , live_connections(source_num)
    , state(PageReceiverState::NORMAL)
    , collected(false)
    , thread_count(0)
    , exc_log(Logger::get(req_id, executor_id))
{
    try
    {
        msg_channel = std::make_unique<MPMCQueue<PageReceivedMessagePtr>>(max_buffer_size);
        // setup fetch threads to fetch pages/blocks from write nodes
        setUpConnection();
    }
    catch (...)
    {
        try
        {
            cancel();
            thread_manager->wait();
        }
        catch (...)
        {
            tryLogCurrentException(exc_log, __PRETTY_FUNCTION__);
        }
        throw;
    }
}

template <typename RPCContext>
RNPageReceiverBase<RPCContext>::~RNPageReceiverBase()
{
    try
    {
        close();
        thread_manager->wait();
    }
    catch (...)
    {
        tryLogCurrentException(exc_log, __PRETTY_FUNCTION__);
    }
}

template <typename RPCContext>
void RNPageReceiverBase<RPCContext>::cancel()
{
    if (setEndState(PageReceiverState::CANCELED))
    {
        rpc_context->cancelDisaggTaskOnTiFlashStorageNode(exc_log);
    }
    cancelAllMsgChannels();
}

template <typename RPCContext>
void RNPageReceiverBase<RPCContext>::close()
{
    setEndState(PageReceiverState::CLOSED);
    finishAllMsgChannels();
}

template <typename RPCContext>
void RNPageReceiverBase<RPCContext>::setUpConnection()
{
    // TODO: support async
    for (size_t index = 0; index < source_num; ++index)
    {
        thread_manager->schedule(true, "Receiver", [this] {
            readLoop();
        });
        ++thread_count;
    }
}

template <typename RPCContext>
PageReceiverResult RNPageReceiverBase<RPCContext>::nextResult(std::unique_ptr<CHBlockChunkCodec> & decoder_ptr)
{
    // Note that decode_ptr can not squash blocks, the blocks could comes
    // from different stores, tables, segments

    std::shared_ptr<PageReceivedMessage> recv_msg;
    auto pop_res = msg_channel->pop(recv_msg);
    if (pop_res != MPMCQueueResult::OK)
    {
        std::unique_lock lock(mu);
        if (state != PageReceiverState::NORMAL)
        {
            return PageReceiverResult::newError(RNPageReceiverBase<RPCContext>::name, details::constructStatusString(state, err_msg));
        }

        /// live_connections == 0, msg_channel is finished, and state is NORMAL,
        /// that is the end.
        return PageReceiverResult::newEOF(RNPageReceiverBase<RPCContext>::name);
    }

    assert(recv_msg != nullptr);
    if (unlikely(recv_msg->error_ptr != nullptr))
        return PageReceiverResult::newError(recv_msg->req_info, recv_msg->error_ptr->msg());

    // Decode the pages or blocks into recv_msg->seg_task
    return toDecodeResult(recv_msg, decoder_ptr);
}

template <typename RPCContext>
PageReceiverResult RNPageReceiverBase<RPCContext>::toDecodeResult(
    const std::shared_ptr<PageReceivedMessage> & recv_msg,
    std::unique_ptr<CHBlockChunkCodec> & decoder_ptr)
{
    assert(recv_msg != nullptr);
    /// the data packets (now we ignore execution summary)
    auto result = PageReceiverResult::newOk(recv_msg->req_info);
    result.decode_detail = decodeChunksAndPersistPages(recv_msg, decoder_ptr);
    auto has_pending_msg = recv_msg->seg_task->addConsumedMsg();
    LOG_DEBUG(exc_log,
              "seg: {} state: {} received: {} pending: {}",
              recv_msg->seg_task->segment_id,
              magic_enum::enum_name(recv_msg->seg_task->state),
              recv_msg->seg_task->num_msg_consumed,
              recv_msg->seg_task->num_msg_to_consume);
    if (recv_msg->seg_task->state == DM::SegmentReadTaskState::Receiving
        && !has_pending_msg)
    {
        // All pending message of current segment task are received,
        // mark the segment task is ready for reading.
        rpc_context->finishTaskReceive(recv_msg->seg_task);
    }
    return result;
}

template <typename RPCContext>
PageDecodeDetail RNPageReceiverBase<RPCContext>::decodeChunksAndPersistPages(
    const std::shared_ptr<PageReceivedMessage> & recv_msg,
    std::unique_ptr<CHBlockChunkCodec> & decoder_ptr)
{
    assert(recv_msg != nullptr);
    PageDecodeDetail detail;

    LOG_DEBUG(exc_log, "decoding msg with {} chunks, {} pages", recv_msg->chunks().size(), recv_msg->pages().size());

    if (recv_msg->empty())
        return detail;

    {
        // Record total packet size
        const auto & packet = *recv_msg->packet;
        detail.packet_bytes = packet.ByteSizeLong();
    }

    // Note: Currently in the 2nd response memtable data is not contained.
    for (const String & chunk : recv_msg->chunks())
    {
        auto block = decoder_ptr->decode(chunk);
        if (!block)
            continue;
        detail.rows += block.rows();
        if likely (block.rows() > 0)
        {
            recv_msg->seg_task->receiveMemTable(std::move(block));
        }
    }

    // Parse the remote pages and persist them into local cache
    for (const String & page : recv_msg->pages())
    {
        DM::RemotePb::RemotePage remote_page;
        bool parsed = remote_page.ParseFromString(page); // TODO: handle error
        RUNTIME_CHECK_MSG(parsed, "Can not parse remote page");
        recv_msg->seg_task->receivePage(std::move(remote_page));
        detail.pages += 1;
    }

    return detail;
}

constexpr Int32 max_retry_times = 10;

template <typename RPCContext>
void RNPageReceiverBase<RPCContext>::readLoop()
{
    // TODO: metrics

    CPUAffinityManager::getInstance().bindSelfQueryThread();
    bool meet_error = false;
    String local_err_msg;

    // Keep popping segment fetch pages request to get the task ready
    while (!meet_error)
    {
        auto req = rpc_context->nextFetchPagesRequest();
        if (!req.isValid())
            break;
        try
        {
            std::tie(meet_error, local_err_msg) = taskReadLoop(req);
        }
        catch (...)
        {
            meet_error = true;
            local_err_msg = getCurrentExceptionMessage(false);
        }
        rpc_context->finishTaskEstablish(req, meet_error);
    }
    connectionDone(meet_error, local_err_msg, exc_log);
}

template <typename RPCContext>
std::tuple<bool, String> RNPageReceiverBase<RPCContext>::taskReadLoop(const FetchPagesRequest & req)
{
    auto status = RPCContext::getStatusOK();
    bool meet_error = false;
    String local_err_msg;

    String req_info = fmt::format("tunnel{}", req.identifier());
    LoggerPtr log = exc_log->getChild(req_info);
    for (int i = 0; i < max_retry_times; ++i)
    {
        Stopwatch w_fetch_page;
        SCOPE_EXIT({
            GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_rpc_fetch_page).Observe(w_fetch_page.elapsedSeconds());
        });

        auto resp_reader = rpc_context->doRequest(req);
        bool has_data = false;
        // Keep reading packets and push the packet
        // into msg_channels
        for (;;)
        {
            LOG_TRACE(log, "begin next ");
            TrackedPageDataPacketPtr packet = std::make_shared<PageDataPacket>();
            if (bool success = resp_reader->read(packet); !success)
                break;
            has_data = true;
            if (packet->has_error())
            {
                meet_error = true;
                local_err_msg = fmt::format("Read error message from page packet: {}", packet->error().msg());
                break;
            }

            if (!details::pushPacket(
                    req.seg_task,
                    req_info,
                    packet,
                    msg_channel,
                    log))
            {
                meet_error = true;
                local_err_msg = fmt::format("Push page packet failed. {}", getStatusString());
                break;
            }
            // LOG_DEBUG(log,
            //           "push packet into channel, chunks: {} pages: {}, msg_size: {}",
            //           packet->chunks_size(),
            //           packet->pages_size(),
            //           msg_channel->size());
        }
        // if meet error, such as decode packet fails, it will not retry.
        if (meet_error)
        {
            resp_reader->cancel(local_err_msg);
            break;
        }
        status = resp_reader->finish();
        if (status.ok())
        {
            // LOG_DEBUG(log, "finish read: {}", req.debugString());
            break;
        }
        else
        {
            bool retryable = !has_data && i + 1 < max_retry_times;
            LOG_WARNING(
                log,
                "FetchDisaggregatedPagesRequest meets rpc fail. Err code = {}, err msg = {}, retryable = {}",
                status.error_code(),
                status.error_message(),
                retryable);
            // if we have received some data, we can not retry.
            if (has_data)
                break;

            using namespace std::chrono_literals;
            std::this_thread::sleep_for(1s);
        }
    }
    if (!status.ok())
    {
        meet_error = true;
        local_err_msg = status.error_message();
    }
    return {meet_error, local_err_msg};
}

template <typename RPCContext>
bool RNPageReceiverBase<RPCContext>::setEndState(PageReceiverState new_state)
{
    assert(new_state == PageReceiverState::CANCELED || new_state == PageReceiverState::CLOSED);
    std::unique_lock lock(mu);
    if (state == PageReceiverState::CANCELED || state == PageReceiverState::CLOSED)
    {
        return false;
    }
    state = new_state;
    return true;
}

template <typename RPCContext>
String RNPageReceiverBase<RPCContext>::getStatusString()
{
    std::unique_lock lock(mu);
    return details::constructStatusString(state, err_msg);
}

template <typename RPCContext>
void RNPageReceiverBase<RPCContext>::connectionDone(
    bool meet_error,
    const String & local_err_msg,
    const LoggerPtr & log)
{
    Int32 copy_live_conn = -1;
    {
        std::unique_lock lock(mu);
        if (meet_error)
        {
            if (state == PageReceiverState::NORMAL)
                state = PageReceiverState::ERROR;
            if (err_msg.empty())
                err_msg = local_err_msg;
        }
        copy_live_conn = --live_connections;
    }
    LOG_DEBUG(
        log,
        "connection end. meet error: {}{}, current alive connections: {}",
        meet_error,
        meet_error ? fmt::format(", err msg: {}", local_err_msg) : "",
        copy_live_conn);

    if (copy_live_conn == 0)
    {
        LOG_DEBUG(log, "All read threads end in RNPageReceiver");
    }
    else if (copy_live_conn < 0)
        throw Exception("live_connections should not be less than 0!");

    if (meet_error || copy_live_conn == 0)
        finishAllMsgChannels();
}

template <typename RPCContext>
void RNPageReceiverBase<RPCContext>::finishAllMsgChannels()
{
    msg_channel->finish();
}

template <typename RPCContext>
void RNPageReceiverBase<RPCContext>::cancelAllMsgChannels()
{
    msg_channel->cancel();
}

/// Explicit template instantiations
template class RNPageReceiverBase<GRPCPagesReceiverContext>;

} // namespace DB
