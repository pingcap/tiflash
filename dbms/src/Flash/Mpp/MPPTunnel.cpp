#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ThreadFactory.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/Utils.h>
#include <Flash/Mpp/getMPPTaskLog.h>
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
    const LogWithPrefixPtr & log_)
    : connected(false)
    , finished(false)
    , is_local(is_local_)
    , timeout(timeout_)
    , tunnel_id(fmt::format("tunnel{}+{}", sender_meta_.task_id(), receiver_meta_.task_id()))
    , input_streams_num(input_steams_num_)
    , send_queue(std::max(5, input_steams_num_ * 5)) // MPMCQueue can benefit from a slightly larger queue size
    , thd_manager(newThreadManager())
    , log(getMPPTaskLog(log_, tunnel_id))
{
}

template <typename Writer>
MPPTunnelBase<Writer>::~MPPTunnelBase()
{
    try
    {
        {
            std::unique_lock lock(mu);
            if (finished)
                return;
            /// make sure to finish the tunnel after it is connected
            waitUntilConnectedOrFinished(lock);
            finishSendQueue();
        }
        waitForConsumerFinish(/*allow_throw=*/false);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in destructor function of MPPTunnel");
    }
    thd_manager->wait();
}

template <typename Writer>
void MPPTunnelBase<Writer>::finishSendQueue()
{
    bool fg = send_queue.finish();
    if (fg && !is_local && is_async)
        writer->TryWrite();
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
                        writer->TryWrite();
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
    LOG_TRACE(log, "ready to write");
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
                writer->TryWrite();
            if (close_after_write)
            {
                finishSendQueue();
                LOG_TRACE(log, "finish write.");
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
    assert(!is_local);
    bool async = is_async.load();
    if (!async)
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
            if (!writer->Write(*res))
            {
                err_msg = "grpc writes failed.";
                break;
            }
            else
            {
                if (async)
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
    if (async)
        writer->WriteDone(grpc::Status::OK);
    else
    {
        GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Decrement();
    }
}


/// done normally and being called exactly once after writing all packets
template <typename Writer>
void MPPTunnelBase<Writer>::writeDone()
{
    LOG_TRACE(log, "ready to finish, is_local: " << is_local);
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
    assert(is_local);
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
            throw Exception("has connected");

        LOG_TRACE(log, "ready to connect");
        if (is_local)
            assert(writer_ == nullptr);
        else
        {
            writer = writer_;
            if (!is_async)
            {
                // communicate send_thread through `consumer_state`
                // NOTE: if the thread creation failed, `connected` will still be `false`.
                thd_manager->schedule(true, "MPPTunnel", [this] {
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
    String err_msg = consumer_state.getError(); // may blocking
    if (allow_throw && !err_msg.empty())
        throw Exception("Consumer exits unexpected, " + err_msg);
}

template <typename Writer>
void MPPTunnelBase<Writer>::waitUntilConnectedOrFinished(std::unique_lock<std::mutex> & lk)
{
    auto connected_or_finished = [&] {
        return connected || finished;
    };
    if (timeout.count() > 0)
    {
        LOG_TRACE(log, "start waitUntilConnectedOrFinished");
        auto res = cv_for_connected_or_finished.wait_for(lk, timeout, connected_or_finished);
        LOG_TRACE(log, "end waitUntilConnectedOrFinished");

        if (!res)
            throw Exception(tunnel_id + " is timeout");
    }
    else
    {
        LOG_TRACE(log, "start waitUntilConnectedOrFinished");
        cv_for_connected_or_finished.wait(lk, connected_or_finished);
        LOG_TRACE(log, "end waitUntilConnectedOrFinished");
    }
    if (!connected)
        throw Exception("MPPTunnel can not be connected because MPPTask is cancelled");
}

template <typename Writer>
void MPPTunnelBase<Writer>::consumerFinish(const String & err_msg, bool need_lock)
{
    // must finish send_queue outside of the critical area to avoid deadlock with write.
    send_queue.finish();
    auto rest_work = [this, &err_msg] {
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
