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
} // namespace FailPoints

MPPTunnel::MPPTunnel(
    const mpp::TaskMeta & receiver_meta_,
    const mpp::TaskMeta & sender_meta_,
    const std::chrono::seconds timeout_,
    int input_steams_num_,
    bool is_local_,
    bool is_async_,
    const String & req_id)
    : MPPTunnel(fmt::format("tunnel{}+{}", sender_meta_.task_id(), receiver_meta_.task_id())
                , timeout_
                , input_steams_num_
                , is_local_
                , is_async_
                , req_id) {}

MPPTunnel::MPPTunnel(
    const String & tunnel_id_,
    const std::chrono::seconds timeout_,
    int input_steams_num_,
    bool is_local_,
    bool is_async_,
    const String & req_id)
    : connected(false)
    , finished(false)
    , timeout(timeout_)
    , tunnel_id(tunnel_id_)
    , send_queue(std::make_shared<MPMCQueue<MPPDataPacketPtr>>(std::max(5, input_steams_num_ * 5))) // MPMCQueue can benefit from a slightly larger queue size
    , log(Logger::get("MPPTunnel", req_id, tunnel_id))
{
    RUNTIME_ASSERT(!(is_local_ && is_async_), log, "is_local: {}, is_async: {}.", is_local_, is_async_);
    cv_for_status_changed_ptr = std::make_shared<std::condition_variable>();
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
            if (finished)
            {
                LOG_DEBUG(log, "already finished!");
                return;
            }

            /// make sure to finish the tunnel after it is connected
            waitUntilConnectedOrFinished(lock);
            finishSendQueue();
        }
        LOG_FMT_TRACE(log, "waiting consumer finish!");
        waitForConsumerFinish(/*allow_throw=*/false);
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
    if (flag && mode == TunnelSenderMode::ASYNC_GRPC) {
        async_tunnel_sender->tryFlushOne();
    }
}

/// exit abnormally, such as being cancelled.
void MPPTunnel::close(const String & reason)
{
    {
        std::unique_lock lk(mu);
        if (finished)
            return;
        if (connected)
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
        }
        else
        {
            finished = true;
            cv_for_status_changed_ptr->notify_all();
            return;
        }
    }
    waitForConsumerFinish(/*allow_throw=*/false);
}

// TODO: consider to hold a buffer
void MPPTunnel::write(const mpp::MPPDataPacket & data, bool close_after_write)
{
    LOG_FMT_TRACE(log, "ready to write");
    {
        {
            std::unique_lock lk(mu);
            waitUntilConnectedOrFinished(lk);
            if (finished)
                throw Exception("write to tunnel which is already closed," + tunnel_sender->getConsumerFinishMsg());
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
    waitForConsumerFinish(/*allow_throw=*/true);
}

/// done normally and being called exactly once after writing all packets
void MPPTunnel::writeDone()
{
    LOG_FMT_TRACE(log, "ready to finish, is_local: {}", mode == TunnelSenderMode::LOCAL);
    {
        std::unique_lock lk(mu);
        if (finished)
            throw Exception("write to tunnel which is already closed," + tunnel_sender->getConsumerFinishMsg());
        /// make sure to finish the tunnel after it is connected
        waitUntilConnectedOrFinished(lk);
        finishSendQueue();
    }
    waitForConsumerFinish(/*allow_throw=*/true);
}

void MPPTunnel::connect(PacketWriter * writer)
{
    {
        std::unique_lock lk(mu);
        if (connected)
            throw Exception("MPPTunnel has connected");
        if (finished)
            throw Exception("MPPTunnel has finished");

        LOG_FMT_TRACE(log, "ready to connect");
        switch(mode)
        {
            case TunnelSenderMode::LOCAL:
                RUNTIME_ASSERT(writer == nullptr, log);
                local_tunnel_sender = std::make_shared<LocalTunnelSender>(mode, send_queue, nullptr, cv_for_status_changed_ptr, log);
                tunnel_sender = local_tunnel_sender;
                break;
            case TunnelSenderMode::SYNC_GRPC:
                sync_tunnel_sender = std::make_shared<SyncTunnelSender>(mode, send_queue, writer, cv_for_status_changed_ptr, log);
                sync_tunnel_sender->startSendThread();
                tunnel_sender = sync_tunnel_sender;
                break;
            case TunnelSenderMode::ASYNC_GRPC:
                async_tunnel_sender = std::make_shared<AsyncTunnelSender>(mode, send_queue, writer, cv_for_status_changed_ptr, log);
                tunnel_sender = async_tunnel_sender;
                RUNTIME_ASSERT(writer != nullptr, log, "Async writer shouldn't be null");
                RUNTIME_ASSERT(tunnel_sender != nullptr, log, "Tunnel sender shouldn't be null");
                writer->attachAsyncTunnelSender(async_tunnel_sender);
                break;
            default:
                RUNTIME_ASSERT(false, log, "Unsupported TunnelSenderMode: {}", mode);
        }
        connected = true;
        cv_for_status_changed_ptr->notify_all();
    }
    LOG_DEBUG(log, "connected");
}

void MPPTunnel::waitForFinish()
{
    waitForConsumerFinish(/*allow_throw=*/true);
}

void MPPTunnel::waitForConsumerFinish(bool allow_throw)
{
#ifndef NDEBUG
    {
        std::unique_lock lock(mu);
        assert(connected);
    }
#endif
    LOG_FMT_TRACE(log, "start wait for consumer finish!");
    String err_msg = tunnel_sender->getConsumerFinishMsg(); // may blocking
    {
        std::unique_lock lock(mu);
        finished = true;
    }
    if (allow_throw && !err_msg.empty())
        throw Exception("Consumer exits unexpected, " + err_msg);
    LOG_FMT_TRACE(log, "end wait for consumer finish!");
}

void MPPTunnel::waitUntilConnectedOrFinished(std::unique_lock<std::mutex> & lk)
{
    auto connected_or_finished_or_consumer_finished = [&] {
        return connected || finished || (tunnel_sender && tunnel_sender->isConsumerFinished());
    };
    if (timeout.count() > 0)
    {
        LOG_FMT_TRACE(log, "start waitUntilConnectedOrFinished");
        auto res = cv_for_status_changed_ptr->wait_for(lk, timeout, connected_or_finished_or_consumer_finished);
        LOG_FMT_TRACE(log, "end waitUntilConnectedOrFinished");

        if (!res)
            throw Exception(tunnel_id + " is timeout");
    }
    else
    {
        LOG_FMT_TRACE(log, "start waitUntilConnectedOrFinished");
        cv_for_status_changed_ptr->wait(lk, connected_or_finished_or_consumer_finished);
        LOG_FMT_TRACE(log, "end waitUntilConnectedOrFinished");
    }
    if (!connected)
        throw Exception("MPPTunnel can not be connected because MPPTask is cancelled");
}

void TunnelSender::consumerFinish(const String & err_msg)
{
    LOG_FMT_TRACE(log, "calling consumer Finish");
    send_queue->finish();
    // it's safe to call it multiple times
    {
        std::unique_lock lk(state_mu);
        if (consumer_state.msgHasSet())
            return;
        consumer_state.setMsg(err_msg);
        cv_for_status_changed_ptr->notify_all();
    }
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
        LOG_ERROR(log, err_msg);
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
    writer->tryFlushOne();
}

void AsyncTunnelSender::sendOne()
{
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
    if (!err_msg.empty()) {
        LOG_ERROR(log, err_msg);
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
