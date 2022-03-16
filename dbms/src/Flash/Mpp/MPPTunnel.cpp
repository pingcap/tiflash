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
            send_queue.finish();
        }
        waitForConsumerFinish(/*allow_throw=*/false);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in destructor function of MPPTunnel");
    }
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
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Failed to close tunnel: " + tunnel_id);
                }
            }
            send_queue.finish();
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

            if (close_after_write)
            {
                send_queue.finish();
                LOG_FMT_TRACE(log, "finish write.");
            }
            return;
        }
    }
    // push failed, wait consumer for the final state
    waitForConsumerFinish(/*allow_throw=*/true);
}

template <typename Writer>
void MPPTunnelBase<Writer>::sendLoop()
{
    assert(!is_local);
    UPDATE_CUR_AND_MAX_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp, type_max_threads_of_establish_mpp);
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
        err_msg = "fatal error in sendLoop()";
    }
    if (!err_msg.empty())
        LOG_ERROR(log, err_msg);
    consumerFinish(err_msg);
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

        send_queue.finish();
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

        LOG_FMT_TRACE(log, "ready to connect");
        if (is_local)
            assert(writer_ == nullptr);
        else
        {
            writer = writer_;
            // communicate send_thread through `consumer_state`
            // NOTE: if the thread creation failed, `connected` will still be `false`.
            newThreadManager()->scheduleThenDetach(true, "MPPTunnel", [this] {
                sendLoop();
            });
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
void MPPTunnelBase<Writer>::consumerFinish(const String & err_msg)
{
    // must finish send_queue outside of the critical area to avoid deadlock with write.
    send_queue.finish();

    std::unique_lock lk(mu);
    finished = true;
    // must call setError in the critical area to keep consistent with `finished` from outside.
    consumer_state.setError(err_msg);
    cv_for_connected_or_finished.notify_all();
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class MPPTunnelBase<::grpc::ServerWriter<::mpp::MPPDataPacket>>;

} // namespace DB
