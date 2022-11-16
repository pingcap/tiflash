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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/EstablishCall.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/Utils.h>
#include <fmt/core.h>

namespace DB
{
namespace FailPoints
{
extern const char random_tunnel_wait_timeout_failpoint[];
} // namespace FailPoints

namespace
{
String tunnelSenderModeToString(TunnelSenderMode mode)
{
    switch (mode)
    {
    case TunnelSenderMode::ASYNC_GRPC:
        return "async";
    case TunnelSenderMode::SYNC_GRPC:
        return "sync";
    case TunnelSenderMode::LOCAL:
        return "local";
    default:
        return "unknown";
    }
}

// Update metric for tunnel's response bytes
inline void updateMetric(size_t pushed_data_size, TunnelSenderMode mode)
{
    switch (mode)
    {
    case TunnelSenderMode::LOCAL:
        GET_METRIC(tiflash_coprocessor_response_bytes, type_mpp_establish_conn_local).Increment(pushed_data_size);
        break;
    case TunnelSenderMode::ASYNC_GRPC:
    case TunnelSenderMode::SYNC_GRPC:
        GET_METRIC(tiflash_coprocessor_response_bytes, type_mpp_establish_conn).Increment(pushed_data_size);
        break;
    default:
        throw DB::Exception("Illegal TunnelSenderMode");
    }
}
} // namespace

MPPTunnel::MPPTunnel(
    const mpp::TaskMeta & receiver_meta_,
    const mpp::TaskMeta & sender_meta_,
    const std::chrono::seconds timeout_,
    int input_steams_num_,
    bool is_local_,
    bool is_async_,
    const String & req_id)
    : MPPTunnel(fmt::format("tunnel{}+{}", sender_meta_.task_id(), receiver_meta_.task_id()), timeout_, input_steams_num_, is_local_, is_async_, req_id)
{}

MPPTunnel::MPPTunnel(
    const String & tunnel_id_,
    const std::chrono::seconds timeout_,
    int input_steams_num_,
    bool is_local_,
    bool is_async_,
    const String & req_id)
    : status(TunnelStatus::Unconnected)
    , timeout(timeout_)
    , tunnel_id(tunnel_id_)
    , mem_tracker(current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr)
    , queue_size(std::max(5, input_steams_num_ * 5)) // MPMCQueue can benefit from a slightly larger queue size
    , log(Logger::get(req_id, tunnel_id))
{
    RUNTIME_ASSERT(!(is_local_ && is_async_), log, "is_local: {}, is_async: {}.", is_local_, is_async_);
    if (is_local_)
        mode = TunnelSenderMode::LOCAL;
    else if (is_async_)
        mode = TunnelSenderMode::ASYNC_GRPC;
    else
        mode = TunnelSenderMode::SYNC_GRPC;
    GET_METRIC(tiflash_object_count, type_count_of_mpptunnel).Increment();
}

MPPTunnel::~MPPTunnel()
{
    SCOPE_EXIT({
        GET_METRIC(tiflash_object_count, type_count_of_mpptunnel).Decrement();
    });
    try
    {
        close("", true);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in destructor function of MPPTunnel");
    }
    LOG_TRACE(log, "destructed tunnel obj!");
}

/// exit abnormally, such as being cancelled.
void MPPTunnel::close(const String & reason, bool wait_sender_finish)
{
    {
        std::unique_lock lk(mu);
        switch (status)
        {
        case TunnelStatus::Unconnected:
            status = TunnelStatus::Finished;
            cv_for_status_changed.notify_all();
            return;
        case TunnelStatus::Connected:
        case TunnelStatus::WaitingForSenderFinish:
        {
            if (!reason.empty())
            {
                tunnel_sender->cancelWith(reason);
            }
            else
            {
                tunnel_sender->finish();
            }
            break;
        }
        case TunnelStatus::Finished:
            return;
        default:
            RUNTIME_ASSERT(false, log, "Unsupported tunnel status: {}", static_cast<Int32>(status));
        }
    }
    if (wait_sender_finish)
        waitForSenderFinish(false);
}

void MPPTunnel::write(TrackedMppDataPacketPtr && data)
{
    LOG_TRACE(log, "ready to write");
    {
        std::unique_lock lk(mu);
        waitUntilConnectedOrFinished(lk);
        if (tunnel_sender == nullptr)
            throw Exception(fmt::format("write to tunnel which is already closed."));
    }

    auto pushed_data_size = data->getPacket().ByteSizeLong();
    if (tunnel_sender->push(std::move(data)))
    {
        updateMetric(pushed_data_size, mode);
        connection_profile_info.bytes += pushed_data_size;
        connection_profile_info.packets += 1;
        return;
    }
    throw Exception(fmt::format("write to tunnel which is already closed,{}", tunnel_sender->isConsumerFinished() ? tunnel_sender->getConsumerFinishMsg() : ""));
}

/// done normally and being called exactly once after writing all packets
void MPPTunnel::writeDone()
{
    LOG_TRACE(log, "ready to finish, is_local: {}", mode == TunnelSenderMode::LOCAL);
    {
        std::unique_lock lk(mu);
        /// make sure to finish the tunnel after it is connected
        waitUntilConnectedOrFinished(lk);
        if (tunnel_sender == nullptr)
            throw Exception(fmt::format("write to tunnel which is already closed."));
    }
    tunnel_sender->finish();
    waitForSenderFinish(/*allow_throw=*/true);
}

void MPPTunnel::connect(PacketWriter * writer)
{
    {
        std::unique_lock lk(mu);
        if (status != TunnelStatus::Unconnected)
            throw Exception(fmt::format("MPPTunnel has connected or finished: {}", statusToString()));

        LOG_TRACE(log, "ready to connect");
        switch (mode)
        {
        case TunnelSenderMode::LOCAL:
        {
            RUNTIME_ASSERT(writer == nullptr, log);
            local_tunnel_sender = std::make_shared<LocalTunnelSender>(queue_size, mem_tracker, log, tunnel_id);
            tunnel_sender = local_tunnel_sender;
            break;
        }
        case TunnelSenderMode::SYNC_GRPC:
        {
            RUNTIME_ASSERT(writer != nullptr, log, "Sync writer shouldn't be null");
            sync_tunnel_sender = std::make_shared<SyncTunnelSender>(queue_size, mem_tracker, log, tunnel_id);
            sync_tunnel_sender->startSendThread(writer);
            tunnel_sender = sync_tunnel_sender;
            break;
        }
        default:
            RUNTIME_ASSERT(false, log, "Unsupported TunnelSenderMode in connect: {}", static_cast<Int32>(mode));
        }
        status = TunnelStatus::Connected;
        cv_for_status_changed.notify_all();
    }
    LOG_DEBUG(log, "connected");
}

void MPPTunnel::connectAsync(IAsyncCallData * call_data)
{
    {
        std::unique_lock lk(mu);
        if (status != TunnelStatus::Unconnected)
            throw Exception(fmt::format("MPPTunnel has connected or finished: {}", statusToString()));

        LOG_TRACE(log, "ready to connect async");
        RUNTIME_ASSERT(mode == TunnelSenderMode::ASYNC_GRPC, log, "mode {} is not async grpc in connectAsync", magic_enum::enum_name(mode));
        RUNTIME_ASSERT(call_data != nullptr, log, "Async writer shouldn't be null");

        auto kick_func_for_test = call_data->getKickFuncForTest();
        if (unlikely(kick_func_for_test.has_value()))
        {
            async_tunnel_sender = std::make_shared<AsyncTunnelSender>(queue_size, mem_tracker, log, tunnel_id, kick_func_for_test.value());
        }
        else
        {
            async_tunnel_sender = std::make_shared<AsyncTunnelSender>(queue_size, mem_tracker, log, tunnel_id, call_data->grpcCall());
        }
        call_data->attachAsyncTunnelSender(async_tunnel_sender);
        tunnel_sender = async_tunnel_sender;

        status = TunnelStatus::Connected;
        cv_for_status_changed.notify_all();
    }
    LOG_DEBUG(log, "Tunnel connected in {} mode", tunnelSenderModeToString(mode));
}

void MPPTunnel::waitForFinish()
{
    waitForSenderFinish(/*allow_throw=*/true);
}

void MPPTunnel::waitForSenderFinish(bool allow_throw)
{
#ifndef NDEBUG
    {
        std::unique_lock lock(mu);
        assert(status != TunnelStatus::Unconnected);
    }
#endif
    LOG_TRACE(log, "start wait for consumer finish!");
    {
        std::unique_lock lock(mu);
        if (status == TunnelStatus::Finished)
        {
            return;
        }
        status = TunnelStatus::WaitingForSenderFinish;
    }
    String err_msg = tunnel_sender->getConsumerFinishMsg(); // may blocking
    {
        std::unique_lock lock(mu);
        status = TunnelStatus::Finished;
    }
    if (allow_throw && !err_msg.empty())
        throw Exception("Consumer exits unexpected, " + err_msg);
    LOG_TRACE(log, "end wait for consumer finish!");
}

void MPPTunnel::waitUntilConnectedOrFinished(std::unique_lock<std::mutex> & lk)
{
    auto not_unconnected = [&] {
        return (status != TunnelStatus::Unconnected);
    };
    if (timeout.count() > 0)
    {
        LOG_TRACE(log, "start waitUntilConnectedOrFinished");
        if (status == TunnelStatus::Unconnected)
        {
            fiu_do_on(FailPoints::random_tunnel_wait_timeout_failpoint, throw Exception(tunnel_id + " is timeout"););
        }
        auto res = cv_for_status_changed.wait_for(lk, timeout, not_unconnected);
        LOG_TRACE(log, "end waitUntilConnectedOrFinished");
        if (!res)
            throw Exception(tunnel_id + " is timeout");
    }
    else
    {
        LOG_TRACE(log, "start waitUntilConnectedOrFinished");
        cv_for_status_changed.wait(lk, not_unconnected);
        LOG_TRACE(log, "end waitUntilConnectedOrFinished");
    }
    if (status == TunnelStatus::Unconnected)
        throw Exception("MPPTunnel can not be connected because MPPTask is cancelled");
}

StringRef MPPTunnel::statusToString()
{
    switch (status)
    {
    case TunnelStatus::Unconnected:
        return "Unconnected";
    case TunnelStatus::Connected:
        return "Connected";
    case TunnelStatus::WaitingForSenderFinish:
        return "WaitingForSenderFinish";
    case TunnelStatus::Finished:
        return "Finished";
    default:
        RUNTIME_ASSERT(false, log, "Unknown TaskStatus {}", static_cast<Int32>(status));
    }
}

void TunnelSender::consumerFinish(const String & msg)
{
    LOG_TRACE(log, "calling consumer Finish");
    finish();
    consumer_state.setMsg(msg);
}

SyncTunnelSender::~SyncTunnelSender()
{
    LOG_TRACE(log, "waiting child thread finished!");
    thread_manager->wait();
}

void SyncTunnelSender::sendJob(PacketWriter * writer)
{
    GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Increment();
    GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Value(), GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Value()));
    String err_msg;
    try
    {
        TrackedMppDataPacketPtr res;
        while (send_queue.pop(res) == MPMCQueueResult::OK)
        {
            if (!writer->write(res->packet))
            {
                err_msg = "grpc writes failed.";
                break;
            }
        }
        /// write the last error packet if needed
        if (send_queue.getStatus() == MPMCQueueStatus::CANCELLED)
        {
            RUNTIME_ASSERT(!send_queue.getCancelReason().empty(), "Tunnel sender cancelled without reason");
            if (!writer->write(getPacketWithError(send_queue.getCancelReason())))
            {
                err_msg = "grpc writes failed.";
            }
        }
    }
    catch (...)
    {
        err_msg = getCurrentExceptionMessage(true);
    }
    if (!err_msg.empty())
    {
        err_msg = fmt::format("{} meet error: {}", tunnel_id, err_msg);
        LOG_ERROR(log, err_msg);
        trimStackTrace(err_msg);
    }
    consumerFinish(err_msg);
    GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Decrement();
}

void SyncTunnelSender::startSendThread(PacketWriter * writer)
{
    thread_manager = newThreadManager();
    thread_manager->schedule(true, "MPPTunnel", [this, writer] {
        sendJob(writer);
    });
}

std::shared_ptr<DB::TrackedMppDataPacket> LocalTunnelSender::readForLocal()
{
    TrackedMppDataPacketPtr res;
    auto result = send_queue.pop(res);
    if (result == MPMCQueueResult::OK)
    {
        // switch tunnel's memory tracker into receiver's
        res->switchMemTracker(current_memory_tracker);
        return res;
    }
    else if (result == MPMCQueueResult::CANCELLED)
    {
        RUNTIME_ASSERT(!send_queue.getCancelReason().empty(), "Tunnel sender cancelled without reason");
        if (!cancel_reason_sent)
        {
            cancel_reason_sent = true;
            res = std::make_shared<TrackedMppDataPacket>(getPacketWithError(send_queue.getCancelReason()), current_memory_tracker);
            return res;
        }
    }
    consumerFinish("");
    return nullptr;
}
} // namespace DB
