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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/EstablishCall.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/PacketWriter.h>
#include <Flash/Mpp/Utils.h>
#include <fmt/core.h>

#include <magic_enum.hpp>

namespace DB
{
namespace FailPoints
{
extern const char random_tunnel_wait_timeout_failpoint[];
extern const char random_tunnel_write_failpoint[];
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
void updateMetric(std::atomic<Int64> & data_size_in_queue, size_t pushed_data_size, TunnelSenderMode mode)
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
    MPPTunnelMetric::addDataSizeMetric(data_size_in_queue, pushed_data_size);
}
} // namespace

MPPTunnel::MPPTunnel(
    const mpp::TaskMeta & receiver_meta_,
    const mpp::TaskMeta & sender_meta_,
    const std::chrono::seconds timeout_,
    const CapacityLimits & queue_limits_,
    bool is_local_,
    bool is_async_,
    const String & req_id)
    : MPPTunnel(
        fmt::format("tunnel{}+{}", sender_meta_.task_id(), receiver_meta_.task_id()),
        timeout_,
        queue_limits_,
        is_local_,
        is_async_,
        req_id)
{}

MPPTunnel::MPPTunnel(
    const String & tunnel_id_,
    const std::chrono::seconds timeout_,
    const CapacityLimits & queue_limits_,
    bool is_local_,
    bool is_async_,
    const String & req_id)
    : status(TunnelStatus::Unconnected)
    , timeout(timeout_)
    , timeout_nanoseconds(timeout_.count() * 1000000000ULL)
    , tunnel_id(tunnel_id_)
    , mem_tracker(current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr)
    , queue_limit(queue_limits_)
    , log(Logger::get(req_id, tunnel_id))
    , data_size_in_queue(0)
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
    SCOPE_EXIT({ GET_METRIC(tiflash_object_count, type_count_of_mpptunnel).Decrement(); });
    try
    {
        close("", true);
        MPPTunnelMetric::clearDataSizeMetric(data_size_in_queue);
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
        std::lock_guard lk(mu);
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
            RUNTIME_ASSERT(false, log, "Unsupported tunnel status: {}", magic_enum::enum_name(status));
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
        RUNTIME_CHECK_MSG(tunnel_sender != nullptr, "write to tunnel {} which is already closed.", tunnel_id);
    }

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_tunnel_write_failpoint);

    auto pushed_data_size = data->getPacket().ByteSizeLong();
    if (tunnel_sender->push(std::move(data)))
    {
        updateMetric(data_size_in_queue, pushed_data_size, mode);
        updateConnProfileInfo(pushed_data_size);
        return;
    }
    throw Exception(fmt::format(
        "write to tunnel {} which is already closed, {}",
        tunnel_id,
        tunnel_sender->isConsumerFinished() ? tunnel_sender->getConsumerFinishMsg() : ""));
}

void MPPTunnel::forceWrite(TrackedMppDataPacketPtr && data)
{
    LOG_TRACE(log, "start force writing");

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_tunnel_write_failpoint);

    auto pushed_data_size = data->getPacket().ByteSizeLong();
    if (tunnel_sender->forcePush(std::move(data)))
    {
        updateMetric(data_size_in_queue, pushed_data_size, mode);
        updateConnProfileInfo(pushed_data_size);
        return;
    }
    throw Exception(fmt::format(
        "write to tunnel {} which is already closed, {}",
        tunnel_id,
        tunnel_sender->isConsumerFinished() ? tunnel_sender->getConsumerFinishMsg() : ""));
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
            throw Exception(fmt::format("write to tunnel {} which is already closed.", tunnel_id));
    }
    tunnel_sender->finish();
    waitForSenderFinish(/*allow_throw=*/true);
}

void MPPTunnel::connectSync(PacketWriter * writer)
{
    {
        std::unique_lock lk(mu);
        RUNTIME_CHECK_MSG(
            status == TunnelStatus::Unconnected,
            "MPPTunnel has connected or finished: {}",
            statusToString());
        RUNTIME_CHECK_MSG(mode == TunnelSenderMode::SYNC_GRPC, "This should be a sync tunnel");
        RUNTIME_ASSERT(writer != nullptr, log, "Sync writer shouldn't be null");

        LOG_TRACE(log, "ready to connect sync");
        sync_tunnel_sender
            = std::make_shared<SyncTunnelSender>(queue_limit, mem_tracker, log, tunnel_id, &data_size_in_queue);
        sync_tunnel_sender->startSendThread(writer);
        tunnel_sender = sync_tunnel_sender;

        status = TunnelStatus::Connected;
        cv_for_status_changed.notify_all();
    }
    LOG_DEBUG(log, "Sync tunnel connected");
}

void MPPTunnel::connectLocalV2(size_t source_index, LocalRequestHandler & local_request_handler, bool has_remote_conn)
{
    {
        std::unique_lock lk(mu);
        RUNTIME_CHECK_MSG(
            status == TunnelStatus::Unconnected,
            "MPPTunnel {} has connected or finished: {}",
            tunnel_id,
            statusToString());
        RUNTIME_CHECK_MSG(mode == TunnelSenderMode::LOCAL, "{} should be a local tunnel", tunnel_id);

        LOG_TRACE(log, "ready to connect local tunnel version 2");
        if (has_remote_conn)
        {
            local_tunnel_v2 = std::make_shared<LocalTunnelSenderV2<false>>(
                source_index,
                local_request_handler,
                log,
                mem_tracker,
                tunnel_id);
            tunnel_sender = local_tunnel_v2;
        }
        else
        {
            local_tunnel_local_only_v2 = std::make_shared<LocalTunnelSenderV2<true>>(
                source_index,
                local_request_handler,
                log,
                mem_tracker,
                tunnel_id);
            tunnel_sender = local_tunnel_local_only_v2;
        }

        status = TunnelStatus::Connected;
        cv_for_status_changed.notify_all();
    }
    LOG_DEBUG(log, "Local tunnel version 2 is connected");
}

void MPPTunnel::connectAsync(IAsyncCallData * call_data)
{
    {
        std::unique_lock lk(mu);
        RUNTIME_CHECK_MSG(
            status == TunnelStatus::Unconnected,
            "MPPTunnel {} has connected or finished: {}",
            tunnel_id,
            statusToString());

        LOG_TRACE(log, "ready to connect async");
        RUNTIME_ASSERT(
            mode == TunnelSenderMode::ASYNC_GRPC,
            log,
            "mode {} is not async grpc in connectAsync",
            magic_enum::enum_name(mode));
        RUNTIME_ASSERT(call_data != nullptr, log, "Async writer shouldn't be null");

        auto kick_func_for_test = call_data->getGRPCKickFuncForTest();
        if (unlikely(kick_func_for_test.has_value()))
        {
            async_tunnel_sender = std::make_shared<AsyncTunnelSender>(
                queue_limit,
                mem_tracker,
                log,
                tunnel_id,
                &data_size_in_queue,
                std::move(kick_func_for_test.value()));
        }
        else
        {
            async_tunnel_sender
                = std::make_shared<AsyncTunnelSender>(queue_limit, mem_tracker, log, tunnel_id, &data_size_in_queue);
        }
        call_data->attachAsyncTunnelSender(async_tunnel_sender);
        tunnel_sender = async_tunnel_sender;

        status = TunnelStatus::Connected;
        cv_for_status_changed.notify_all();
    }
    LOG_DEBUG(log, "Async tunnel connected in {} mode", tunnelSenderModeToString(mode));
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
            return;
        status = TunnelStatus::WaitingForSenderFinish;
    }
    String err_msg = tunnel_sender->getConsumerFinishMsg(); // may blocking
    {
        std::unique_lock lock(mu);
        status = TunnelStatus::Finished;
    }
    if (allow_throw && !err_msg.empty())
        throw Exception(fmt::format("{}: consumer exits unexpected, error message: {} ", tunnel_id, err_msg));
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
        fiu_do_on(FailPoints::random_tunnel_wait_timeout_failpoint, {
            if (status == TunnelStatus::Unconnected)
                throw Exception(tunnel_id + " is timeout");
        });
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
        throw Exception(fmt::format("MPPTunnel {} can not be connected because MPPTask is cancelled", tunnel_id));
}

WaitResult MPPTunnel::waitForWritable() const
{
    std::unique_lock lk(mu);
    switch (status)
    {
    case TunnelStatus::Unconnected:
    {
        if (timeout.count() > 0)
        {
            fiu_do_on(FailPoints::random_tunnel_wait_timeout_failpoint,
                      throw Exception(fmt::format("{} is timeout", tunnel_id)););
            if (unlikely(!timeout_stopwatch))
                timeout_stopwatch.emplace(CLOCK_MONOTONIC_COARSE);
            if (unlikely(timeout_stopwatch->elapsed() > timeout_nanoseconds))
                throw Exception(fmt::format("{} is timeout", tunnel_id));
        }
        return WaitResult::WaitForPolling;
    }
    case TunnelStatus::Connected:
    case TunnelStatus::WaitingForSenderFinish:
        RUNTIME_CHECK_MSG(tunnel_sender != nullptr, "write to tunnel {} which is already closed.", tunnel_id);
        if (!tunnel_sender->isWritable())
        {
            setNotifyFuture(tunnel_sender);
            return WaitResult::WaitForNotify;
        }
        return WaitResult::Ready;
    case TunnelStatus::Finished:
        RUNTIME_CHECK_MSG(tunnel_sender != nullptr, "write to tunnel {} which is already closed.", tunnel_id);
        throw Exception(fmt::format(
            "write to tunnel {} which is already closed, {}",
            tunnel_id,
            tunnel_sender->isConsumerFinished() ? tunnel_sender->getConsumerFinishMsg() : ""));
    default:
        RUNTIME_ASSERT(false, log, "Unsupported tunnel status: {}", magic_enum::enum_name(status));
    }
}

std::string_view MPPTunnel::statusToString()
{
    return magic_enum::enum_name(status);
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
    GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp)
        .Set(std::max(
            GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Value(),
            GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Value()));
    String err_msg;
    try
    {
        TrackedMppDataPacketPtr res;
        while (send_queue.pop(res) == MPMCQueueResult::OK)
        {
            MPPTunnelMetric::subDataSizeMetric(*data_size_in_queue, res->getPacket().ByteSizeLong());
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
    thread_manager->schedule(true, "MPPTunnel", [this, writer] { sendJob(writer); });
}

// TODO remove it in the future
void MPPTunnel::connectLocalV1(PacketWriter * writer)
{
    {
        std::unique_lock lk(mu);
        if (status != TunnelStatus::Unconnected)
            throw Exception(fmt::format("MPPTunnel {} has connected or finished: {}", tunnel_id, statusToString()));

        LOG_TRACE(log, "ready to connect local tunnel version 1");

        RUNTIME_ASSERT(writer == nullptr, log);
        local_tunnel_sender_v1
            = std::make_shared<LocalTunnelSenderV1>(queue_limit, mem_tracker, log, tunnel_id, &data_size_in_queue);
        tunnel_sender = local_tunnel_sender_v1;

        status = TunnelStatus::Connected;
        cv_for_status_changed.notify_all();
    }
    LOG_DEBUG(log, "Local tunnel version 1 is connected");
}

std::shared_ptr<DB::TrackedMppDataPacket> LocalTunnelSenderV1::readForLocal()
{
    TrackedMppDataPacketPtr res;
    auto result = send_queue.pop(res);
    if (result == MPMCQueueResult::OK)
    {
        MPPTunnelMetric::subDataSizeMetric(*data_size_in_queue, res->getPacket().ByteSizeLong());
        return res;
    }
    else if (result == MPMCQueueResult::CANCELLED)
    {
        RUNTIME_ASSERT(!send_queue.getCancelReason().empty(), "Tunnel sender cancelled without reason");
        if (!cancel_reason_sent)
        {
            cancel_reason_sent = true;
            res = std::make_shared<TrackedMppDataPacket>(
                getPacketWithError(send_queue.getCancelReason()),
                current_memory_tracker);
            return res;
        }
    }
    consumerFinish("");
    return nullptr;
}
} // namespace DB
