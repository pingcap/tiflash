#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ThreadFactory.h>
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
    TaskCancelledCallback callback,
    int input_steams_num_)
    : connected(false)
    , finished(false)
    , timeout(timeout_)
    , task_cancelled_callback(std::move(callback))
    , tunnel_id(fmt::format("tunnel{}+{}", sender_meta_.task_id(), receiver_meta_.task_id()))
    , send_loop_msg("")
    , input_streams_num(input_steams_num_)
    , send_queue(input_steams_num_ * 5) /// TODO(fzh) set a reasonable parameter
    , log(&Poco::Logger::get(tunnel_id))
{
}

template <typename Writer>
MPPTunnelBase<Writer>::~MPPTunnelBase()
{
    try
    {
        send_queue.cancel();
        if (!finished)
            writeDone();
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
    send_queue.cancel();
    std::unique_lock<std::mutex> lk(mu);
    if (finished)
        return;
    stopSendLoop(false);
    if (connected && !reason.empty())
    {
        try
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_close_tunnel);
            if (!writer->Write(getPacketWithError(reason)))
            {
                auto msg = " grpc writes failed.";
                LOG_ERROR(log, msg);
                throw Exception(tunnel_id + msg);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to close tunnel: " + tunnel_id);
        }
    }
    finishWithLock();
}

template <typename Writer>
bool MPPTunnelBase<Writer>::isTaskCancelled()
{
    return task_cancelled_callback();
}

// TODO: consider to hold a buffer
template <typename Writer>
void MPPTunnelBase<Writer>::write(const mpp::MPPDataPacket & data, bool close_after_write)
{
    auto packet = std::make_shared<mpp::MPPDataPacket>(data);
    LOG_TRACE(log, "ready to write");
    {
        std::unique_lock<std::mutex> lk(mu);
        waitUntilConnectedOrCancelled(lk);
        if (finished)
            throw Exception("write to tunnel which is already closed," + send_loop_msg);

        bool res = send_queue.push(std::move(packet));
        if (res)
        {
            if (close_after_write)
            {
                send_queue.finish();
                stopSendLoop(true);
                finishWithLock();
            }
        }
        else
            stopSendLoop(true);
    }
}

/// to avoid being blocked when pop(), we should send nullptr into send_queue in all cases
template <typename Writer>
void MPPTunnelBase<Writer>::sendLoop()
{
    try
    {
        while (true)
        {
            auto res = send_queue.pop();
            if (!res.has_value())
                break;
            else
            {
                if (!writer->Write(*res.value()))
                {
                    auto msg = " grpc writes failed.";
                    LOG_ERROR(log, msg);
                    send_loop_msg = tunnel_id + msg;
                    break;
                }
            }
        }
    }
    catch (Exception & e)
    {
        send_loop_msg = e.message();
    }
    catch (std::exception & e)
    {
        send_loop_msg = e.what();
    }
    catch (...)
    {
        send_loop_msg = "fatal error in sendLoop()";
    }
    if (!send_loop_msg.empty())
        send_queue.cancel();
}

/// done normally and being called exactly once after writing all packets
template <typename Writer>
void MPPTunnelBase<Writer>::writeDone()
{
    send_queue.finish();
    {
        std::unique_lock<std::mutex> lk(mu);
        if (finished)
            throw Exception("has finished, " + send_loop_msg);
        /// make sure to finish the tunnel after it is connected
        waitUntilConnectedOrCancelled(lk);
        if (finished)
            throw Exception("has finished, " + send_loop_msg);
        stopSendLoop(true);
        finishWithLock();
    }
}

template <typename Writer>
void MPPTunnelBase<Writer>::connect(Writer * writer_)
{
    std::lock_guard<std::mutex> lk(mu);
    if (connected)
        throw Exception("has connected");

    LOG_DEBUG(log, "ready to connect");
    writer = writer_;
    send_thread = std::make_unique<std::thread>(ThreadFactory(true, "MPPTunnel").newThread([this] { sendLoop(); }));

    connected = true;
    cv_for_connected.notify_all();
}

template <typename Writer>
void MPPTunnelBase<Writer>::waitForFinish()
{
    std::unique_lock<std::mutex> lk(mu);

    cv_for_finished.wait(lk, [&]() { return finished.load(); });
}

template <typename Writer>
void MPPTunnelBase<Writer>::waitUntilConnectedOrCancelled(std::unique_lock<std::mutex> & lk)
{
    auto connected_or_cancelled = [&] {
        return connected || isTaskCancelled();
    };
    if (timeout.count() > 0)
    {
        if (!cv_for_connected.wait_for(lk, timeout, connected_or_cancelled))
            throw Exception(tunnel_id + " is timeout");
    }
    else
    {
        cv_for_connected.wait(lk, connected_or_cancelled);
    }
    if (!connected)
        throw Exception("MPPTunnel can not be connected because MPPTask is cancelled");
}

template <typename Writer>
void MPPTunnelBase<Writer>::finishWithLock()
{
    finished = true;
    cv_for_finished.notify_all();
}

template <typename Writer>
void MPPTunnelBase<Writer>::stopSendLoop(bool check_loop_error)
{
    if (send_thread && send_thread->joinable())
    {
        send_thread->join();
        send_thread.reset();
    }
    if (check_loop_error && !send_loop_msg.empty())
        throw Exception("sendLoop() exits unexpected, " + send_loop_msg);
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class MPPTunnelBase<::grpc::ServerWriter<::mpp::MPPDataPacket>>;

} // namespace DB
