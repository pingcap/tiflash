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

template <typename Writer>
MPPTunnelBase<Writer>::MPPTunnelBase(
    const mpp::TaskMeta & receiver_meta_,
    const mpp::TaskMeta & sender_meta_,
    const std::chrono::seconds timeout_,
    int input_steams_num_,
    bool is_local_,
    bool is_async_,
    const String & req_id)
    : connected(false)
    , finished(false)
    , is_local(is_local_)
    , is_async(is_async_)
    , timeout(timeout_)
    , tunnel_id(fmt::format("tunnel{}+{}", sender_meta_.task_id(), receiver_meta_.task_id()))
    , input_streams_num(input_steams_num_)
    , send_queue(std::max(5, input_steams_num_ * 5)) // MPMCQueue can benefit from a slightly larger queue size
    , thread_manager(newThreadManager())
    , log(Logger::get("MPPTunnel", req_id, tunnel_id))
{
    RUNTIME_ASSERT(!(is_local && is_async), log, "is_local: {}, is_async: {}.", is_local, is_async);
    GET_METRIC(tiflash_object_count, type_count_of_mpptunnel).Increment();
}

template <typename Writer>
MPPTunnelBase<Writer>::MPPTunnelBase(
    const String & tunnel_id_,
    const std::chrono::seconds timeout_,
    int input_steams_num_,
    bool is_local_,
    bool is_async_,
    const String & req_id)
    : connected(false)
    , finished(false)
    , is_local(is_local_)
    , is_async(is_async_)
    , timeout(timeout_)
    , tunnel_id(tunnel_id_)
    , input_streams_num(input_steams_num_)
    , send_queue(std::max(5, input_steams_num_ * 5)) // MPMCQueue can benefit from a slightly larger queue size
    , thread_manager(newThreadManager())
    , log(Logger::get("MPPTunnel", req_id, tunnel_id))
{
    RUNTIME_ASSERT(!(is_local && is_async), log, "is_local: {}, is_async: {}.", is_local, is_async);
}

template <typename Writer>
MPPTunnelBase<Writer>::~MPPTunnelBase()
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
                LOG_FMT_TRACE(log, "already finished!");
                return;
            }

            /// make sure to finish the tunnel after it is connected
            waitUntilConnectedOrFinished(lock);
            finishSendQueue();
        }
        LOG_FMT_TRACE(log, "waiting consumer finish!");
        waitForConsumerFinish(/*allow_throw=*/false);
        LOG_FMT_TRACE(log, "waiting child thread finished!");
        thread_manager->wait();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in destructor function of MPPTunnel");
    }
    LOG_FMT_TRACE(log, "destructed tunnel obj!");
}

template <typename Writer>
void MPPTunnelBase<Writer>::finishSendQueue()
{
    bool flag = send_queue.finish();
    if (flag && !is_local && is_async)
        writer->tryFlushOne();
}

/// exit abnormally, such as being cancelled.
template <typename Writer>
void MPPTunnelBase<Writer>::close(const String & reason)
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
                    send_queue.push(std::make_shared<mpp::MPPDataPacket>(getPacketWithError(reason)));
                    if (!is_local && is_async)
                        writer->tryFlushOne();
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
            cv_for_connected_or_finished.notify_all();
            return;
        }
    }
    waitForConsumerFinish(/*allow_throw=*/false);
}

// TODO: consider to hold a buffer
template <typename Writer>
void MPPTunnelBase<Writer>::write(const mpp::MPPDataPacket & data, bool close_after_write)
{
    LOG_FMT_TRACE(log, "ready to write");
    {
        std::unique_lock lk(mu);
        waitUntilConnectedOrFinished(lk);
        if (finished)
            throw Exception("write to tunnel which is already closed," + consumer_state.getError());

        if (send_queue.push(std::make_shared<mpp::MPPDataPacket>(data)))
        {
            connection_profile_info.bytes += data.ByteSizeLong();
            connection_profile_info.packets += 1;
            if (!is_local && is_async)
                writer->tryFlushOne();
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

template <typename Writer>
void MPPTunnelBase<Writer>::sendJob(bool need_lock)
{
    RUNTIME_ASSERT(!is_local, log, "should not reach sendJob for local tunnels");
    if (!is_async)
    {
        GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Increment();
        GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Value(), GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Value()));
    }
    String err_msg;
    try
    {
        /// TODO(fzh) reuse it later
        MPPDataPacketPtr res;
        while (send_queue.pop(res))
        {
            if (!writer->write(*res))
            {
                err_msg = "grpc writes failed.";
                break;
            }
            else
            {
                if (is_async)
                    return;
            }
        }
    }
    catch (Exception & e)
    {
        err_msg = e.message();
    }
    catch (std::exception & e)
    {
        err_msg = e.what();
    }
    catch (...)
    {
        err_msg = "fatal error in sendJob()";
    }
    if (!err_msg.empty())
        LOG_ERROR(log, err_msg);
    consumerFinish(err_msg, need_lock);
    if (is_async)
        writer->writeDone(grpc::Status::OK);
    else
    {
        GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Decrement();
    }
}


/// done normally and being called exactly once after writing all packets
template <typename Writer>
void MPPTunnelBase<Writer>::writeDone()
{
    LOG_FMT_TRACE(log, "ready to finish, is_local: {}", is_local);
    {
        std::unique_lock lk(mu);
        if (finished)
            throw Exception("write to tunnel which is already closed," + consumer_state.getError());
        /// make sure to finish the tunnel after it is connected
        waitUntilConnectedOrFinished(lk);
        finishSendQueue();
    }
    waitForConsumerFinish(/*allow_throw=*/true);
}

template <typename Writer>
std::shared_ptr<mpp::MPPDataPacket> MPPTunnelBase<Writer>::readForLocal()
{
    RUNTIME_ASSERT(is_local, log, "should not reach readForLocal for remote tunnels");
    MPPDataPacketPtr res;
    if (send_queue.pop(res))
        return res;
    consumerFinish("");
    return nullptr;
}

template <typename Writer>
void MPPTunnelBase<Writer>::connect(Writer * writer_)
{
    {
        std::unique_lock lk(mu);
        if (connected)
            throw Exception("MPPTunnel has connected");
        if (finished)
            throw Exception("MPPTunnel has finished");

        LOG_FMT_TRACE(log, "ready to connect");
        if (is_local)
            RUNTIME_ASSERT(writer_ == nullptr, log);
        else
        {
            writer = writer_;
            if (!is_async)
            {
                // communicate send_thread through `consumer_state`
                // NOTE: if the thread creation failed, `connected` will still be `false`.
                thread_manager->schedule(true, "MPPTunnel", [this] {
                    sendJob();
                });
            }
        }
        connected = true;
        cv_for_connected_or_finished.notify_all();
    }
    LOG_DEBUG(log, "connected");
}

template <typename Writer>
void MPPTunnelBase<Writer>::waitForFinish()
{
    waitForConsumerFinish(/*allow_throw=*/true);
}

template <typename Writer>
void MPPTunnelBase<Writer>::waitForConsumerFinish(bool allow_throw)
{
#ifndef NDEBUG
    {
        std::unique_lock lock(mu);
        assert(connected);
    }
#endif
    LOG_FMT_TRACE(log, "start wait for consumer finish!");
    String err_msg = consumer_state.getError(); // may blocking
    if (allow_throw && !err_msg.empty())
        throw Exception("Consumer exits unexpected, " + err_msg);
    LOG_FMT_TRACE(log, "end wait for consumer finish!");
}

template <typename Writer>
void MPPTunnelBase<Writer>::waitUntilConnectedOrFinished(std::unique_lock<std::mutex> & lk)
{
    auto connected_or_finished = [&] {
        return connected || finished;
    };
    if (timeout.count() > 0)
    {
        LOG_FMT_TRACE(log, "start waitUntilConnectedOrFinished");
        auto res = cv_for_connected_or_finished.wait_for(lk, timeout, connected_or_finished);
        LOG_FMT_TRACE(log, "end waitUntilConnectedOrFinished");

        if (!res)
            throw Exception(tunnel_id + " is timeout");
    }
    else
    {
        LOG_FMT_TRACE(log, "start waitUntilConnectedOrFinished");
        cv_for_connected_or_finished.wait(lk, connected_or_finished);
        LOG_FMT_TRACE(log, "end waitUntilConnectedOrFinished");
    }
    if (!connected)
        throw Exception("MPPTunnel can not be connected because MPPTask is cancelled");
}

template <typename Writer>
void MPPTunnelBase<Writer>::consumerFinish(const String & err_msg, bool need_lock)
{
    // must finish send_queue outside of the critical area to avoid deadlock with write.
    LOG_FMT_TRACE(log, "calling consumer Finish");
    send_queue.finish();
    auto rest_work = [this, &err_msg] {
        // it's safe to call it multiple times
        if (finished && consumer_state.errHasSet())
            return;
        finished = true;
        // must call setError in the critical area to keep consistent with `finished` from outside.
        consumer_state.setError(err_msg);
        cv_for_connected_or_finished.notify_all();
    };
    if (need_lock)
    {
        std::unique_lock lk(mu);
        rest_work();
    }
    else
        rest_work();
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class MPPTunnelBase<PacketWriter>;

} // namespace DB
