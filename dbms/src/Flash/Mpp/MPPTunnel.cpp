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
#include <Common/ThreadFactory.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/Utils.h>
#include <fmt/core.h>

namespace DB
{
namespace FailPoints
{
extern const char exception_during_mpp_close_tunnel[];
extern const char random_tunnel_wait_timeout_failpoint[];
} // namespace FailPoints

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
    , send_queue(std::make_shared<MPMCQueue<MPPDataPacketPtr>>(std::max(5, input_steams_num_ * 5))) // MPMCQueue can benefit from a slightly larger queue size
    , log(Logger::get("MPPTunnel", req_id, tunnel_id))
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
        {
            std::unique_lock lock(mu);
            if (status == TunnelStatus::Finished)
            {
                LOG_DEBUG(log, "already finished!");
                return;
            }

            /// make sure to finish the tunnel after it is connected
            waitUntilConnectedOrFinished(lock);
            finishSendQueue();
        }
        LOG_FMT_TRACE(log, "waiting consumer finish!");
        waitForSenderFinish(/*allow_throw=*/false);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in destructor function of MPPTunnel");
    }
    LOG_FMT_TRACE(log, "destructed tunnel obj!");
}

void MPPTunnel::finishSendQueue()
{
    bool flag = send_queue->finish();
    if (flag && mode == TunnelSenderMode::ASYNC_GRPC)
    {
        async_tunnel_sender->tryFlushOne();
    }
}

/// exit abnormally, such as being cancelled.
void MPPTunnel::close(const String & reason)
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
        {
            if (!reason.empty())
            {
                try
                {
                    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_close_tunnel);
                    send_queue->push(std::make_shared<mpp::MPPDataPacket>(getPacketWithError(reason)));
                    if (mode == TunnelSenderMode::ASYNC_GRPC)
                        async_tunnel_sender->tryFlushOne();
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Failed to close tunnel: " + tunnel_id);
                }
            }
            finishSendQueue();
            break;
        }
        case TunnelStatus::WaitingForSenderFinish:
            break;
        case TunnelStatus::Finished:
            return;
        default:
            RUNTIME_ASSERT(false, log, "Unsupported tunnel status: {}", status);
        }
    }
    waitForSenderFinish(/*allow_throw=*/false);
}

// TODO: consider to hold a buffer
void MPPTunnel::write(const mpp::MPPDataPacket & data, bool close_after_write)
{
    LOG_FMT_TRACE(log, "ready to write");
    {
        {
            std::unique_lock lk(mu);
            waitUntilConnectedOrFinished(lk);
            if (status == TunnelStatus::Finished)
                throw Exception(fmt::format("write to tunnel which is already closed,{}", tunnel_sender ? tunnel_sender->getConsumerFinishMsg() : ""));
        }

        if (send_queue->push(std::make_shared<mpp::MPPDataPacket>(data)))
        {
            connection_profile_info.bytes += data.ByteSizeLong();
            connection_profile_info.packets += 1;
            if (mode == TunnelSenderMode::ASYNC_GRPC)
                async_tunnel_sender->tryFlushOne();
            if (close_after_write)
            {
                finishSendQueue();
                LOG_FMT_TRACE(log, "finish write.");
            }
            return;
        }
    }
    // push failed, wait consumer for the final state
    waitForSenderFinish(/*allow_throw=*/true);
}

/// done normally and being called exactly once after writing all packets
void MPPTunnel::writeDone()
{
    LOG_FMT_TRACE(log, "ready to finish, is_local: {}", mode == TunnelSenderMode::LOCAL);
    {
        std::unique_lock lk(mu);
        if (status == TunnelStatus::Finished)
            throw Exception(fmt::format("write to tunnel which is already closed,{}", tunnel_sender ? tunnel_sender->getConsumerFinishMsg() : ""));
        /// make sure to finish the tunnel after it is connected
        waitUntilConnectedOrFinished(lk);
        finishSendQueue();
    }
    waitForSenderFinish(/*allow_throw=*/true);
}

void MPPTunnel::connect(PacketWriter * writer)
{
    {
        std::unique_lock lk(mu);
        if (status != TunnelStatus::Unconnected)
            throw Exception(fmt::format("MPPTunnel has connected or finished: {}", statusToString()));

        LOG_FMT_TRACE(log, "ready to connect");
        switch (mode)
        {
        case TunnelSenderMode::LOCAL:
            RUNTIME_ASSERT(writer == nullptr, log);
            local_tunnel_sender = std::make_shared<LocalTunnelSender>(mode, send_queue, nullptr, log, tunnel_id);
            tunnel_sender = local_tunnel_sender;
            break;
        case TunnelSenderMode::SYNC_GRPC:
            RUNTIME_ASSERT(writer != nullptr, log, "Sync writer shouldn't be null");
            sync_tunnel_sender = std::make_shared<SyncTunnelSender>(mode, send_queue, writer, log, tunnel_id);
            sync_tunnel_sender->startSendThread();
            tunnel_sender = sync_tunnel_sender;
            break;
        case TunnelSenderMode::ASYNC_GRPC:
            RUNTIME_ASSERT(writer != nullptr, log, "Async writer shouldn't be null");
            async_tunnel_sender = std::make_shared<AsyncTunnelSender>(mode, send_queue, writer, log, tunnel_id);
            tunnel_sender = async_tunnel_sender;
            writer->attachAsyncTunnelSender(async_tunnel_sender);
            break;
        default:
            RUNTIME_ASSERT(false, log, "Unsupported TunnelSenderMode: {}", mode);
        }
        status = TunnelStatus::Connected;
        cv_for_status_changed.notify_all();
    }
    LOG_DEBUG(log, "connected");
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
    LOG_FMT_TRACE(log, "start wait for consumer finish!");
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
    LOG_FMT_TRACE(log, "end wait for consumer finish!");
}

void MPPTunnel::waitUntilConnectedOrFinished(std::unique_lock<std::mutex> & lk)
{
    auto not_unconnected = [&] {
        return (status != TunnelStatus::Unconnected);
    };
    if (timeout.count() > 0)
    {
        LOG_FMT_TRACE(log, "start waitUntilConnectedOrFinished");
        auto res = cv_for_status_changed.wait_for(lk, timeout, not_unconnected);
        LOG_FMT_TRACE(log, "end waitUntilConnectedOrFinished");
        fiu_do_on(FailPoints::random_tunnel_wait_timeout_failpoint, res = false;);
        if (!res)
            throw Exception(tunnel_id + " is timeout");
    }
    else
    {
        LOG_FMT_TRACE(log, "start waitUntilConnectedOrFinished");
        cv_for_status_changed.wait(lk, not_unconnected);
        LOG_FMT_TRACE(log, "end waitUntilConnectedOrFinished");
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
        RUNTIME_ASSERT(false, log, "Unknown TaskStatus {}", status);
    }
}

void TunnelSender::consumerFinish(const String & msg)
{
    LOG_FMT_TRACE(log, "calling consumer Finish");
    send_queue->finish();
    consumer_state.setMsg(msg);
}

SyncTunnelSender::~SyncTunnelSender()
{
    LOG_FMT_TRACE(log, "waiting child thread finished!");
    thread_manager->wait();
}

void SyncTunnelSender::sendJob()
{
    GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Increment();
    GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Value(), GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Value()));
    String err_msg;
    try
    {
        MPPDataPacketPtr res;
        while (send_queue->pop(res))
        {
            if (!writer->write(*res))
            {
                err_msg = "grpc writes failed.";
                break;
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

void SyncTunnelSender::startSendThread()
{
    thread_manager = newThreadManager();
    thread_manager->schedule(true, "MPPTunnel", [this] {
        sendJob();
    });
}

void AsyncTunnelSender::tryFlushOne()
{
    // When consumer finished, sending work is done already, just return
    if (consumer_state.msgHasSet())
        return;
    writer->tryFlushOne();
}

void AsyncTunnelSender::sendOne()
{
    // When consumer finished, sending work is done already, just return
    if (consumer_state.msgHasSet())
        return;

    String err_msg;
    bool queue_empty_flag = false;
    try
    {
        MPPDataPacketPtr res;
        queue_empty_flag = !send_queue->pop(res);
        if (!queue_empty_flag)
        {
            if (!writer->write(*res))
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
    if (!err_msg.empty() || queue_empty_flag)
    {
        consumerFinish(err_msg);
        writer->writeDone(grpc::Status::OK);
    }
}

LocalTunnelSender::MPPDataPacketPtr LocalTunnelSender::readForLocal()
{
    MPPDataPacketPtr res;
    if (send_queue->pop(res))
        return res;
    consumerFinish("");
    return nullptr;
}
} // namespace DB
