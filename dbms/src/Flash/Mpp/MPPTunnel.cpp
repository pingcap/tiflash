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
    int input_steams_num_,
    bool is_local_)
    : connected(false)
    , finished(false)
    , is_local(is_local_)
    , timeout(timeout_)
    , task_cancelled_callback(std::move(callback))
    , tunnel_id(fmt::format("tunnel{}+{}", sender_meta_.task_id(), receiver_meta_.task_id()))
    , send_loop_msg("")
    , input_streams_num(input_steams_num_)
    , send_thread(nullptr)
    , send_queue(std::max(5, input_steams_num_ * 5)) /// the queue should not be too small to push the last nullptr or error msg. TODO(fzh) set a reasonable parameter
    , log(&Poco::Logger::get(tunnel_id))
{
}

template <typename Writer>
MPPTunnelBase<Writer>::~MPPTunnelBase()
{
    try
    {
        if (!finished)
            writeDone();
        if (nullptr != send_thread && send_thread->joinable())
        {
            send_thread->join();
        }
        clearSendQueue();
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
    std::unique_lock<std::mutex> lk(mu);
    if (finished)
        return;
    if (connected && !reason.empty())
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
    if (connected)
    {
        send_queue.push(nullptr);
        /// should wait the errors being sent in abnormal cases.
        cv_for_finished.wait(lk, [&]() { return finished.load(); });
    }
    else
    {
        finished = true;
    }
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
    LOG_TRACE(log, "ready to write");
    {
        {
            std::unique_lock<std::mutex> lk(mu);
            waitUntilConnectedOrCancelled(lk);
            if (finished)
                throw Exception("write to tunnel which is already closed," + send_loop_msg);
        }

        send_queue.push(std::make_shared<mpp::MPPDataPacket>(data));
        if (close_after_write)
        {
            std::unique_lock<std::mutex> lk(mu);
            if (!finished)
            {
                /// in abnormal cases, finished can be set in advance and pushing nullptr is also necessary
                send_queue.push(nullptr);
                LOG_TRACE(log, "sending a nullptr to finish write.");
            }
        }
    }
}

/// in abnormal cases, popping all packets out of send_queue to avoid blocking any thread pushes packets into it.
template <typename Writer>
void MPPTunnelBase<Writer>::clearSendQueue()
{
    MPPDataPacketPtr res;
    while (send_queue.size() > 0)
    {
        send_queue.pop(res);
    }
}

/// to avoid being blocked when pop(), we should send nullptr into send_queue in all cases
template <typename Writer>
void MPPTunnelBase<Writer>::sendLoop()
{
    if (is_local)
        return; // sendLoop is useless in local environment
    try
    {
        /// TODO(fzh) reuse it later
        MPPDataPacketPtr res;
        while (!finished)
        {
            send_queue.pop(res);
            if (nullptr == res)
            {
                finishWithLock();
                return;
            }
            else
            {
                if (!writer->Write(*res))
                {
                    finishWithLock();
                    auto msg = " grpc writes failed.";
                    LOG_ERROR(log, msg);
                    throw Exception(tunnel_id + msg);
                }
            }
        }
    }
    catch (Exception & e)
    {
        std::unique_lock<std::mutex> lk(mu);
        send_loop_msg = e.message();
    }
    catch (std::exception & e)
    {
        std::unique_lock<std::mutex> lk(mu);
        send_loop_msg = e.what();
    }
    catch (...)
    {
        std::unique_lock<std::mutex> lk(mu);
        send_loop_msg = "fatal error in sendLoop()";
    }
    if (!finished)
    {
        finishWithLock();
    }
}

/// done normally and being called exactly once after writing all packets
template <typename Writer>
void MPPTunnelBase<Writer>::writeDone()
{
    LOG_TRACE(log, "ready to finish");
    std::unique_lock<std::mutex> lk(mu);
    if (finished)
        throw Exception("has finished, " + send_loop_msg);
    /// make sure to finish the tunnel after it is connected
    waitUntilConnectedOrCancelled(lk);
    lk.unlock();
    /// in normal cases, send nullptr to notify finish
    send_queue.push(nullptr);
    waitForFinish();
    LOG_TRACE(log, "done to finish");
}

template <typename Writer>
std::shared_ptr<mpp::MPPDataPacket> MPPTunnelBase<Writer>::readForLocal()
{
    if (is_local)
    {
        MPPDataPacketPtr res;
        if (!finished)
        {
            send_queue.pop(res);
            if (nullptr == res)
            {
                finishWithLock();
            }

            return res;
        }
    }
    return nullptr;
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

    /// check whether sendLoop() normally or abnormally exited
    if (!send_loop_msg.empty())
        throw Exception("sendLoop() exits unexpected, " + send_loop_msg);
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
    clearSendQueue();
    std::unique_lock<std::mutex> lk(mu);
    finished = true;
    cv_for_finished.notify_all();
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class MPPTunnelBase<::grpc::ServerWriter<::mpp::MPPDataPacket>>;

} // namespace DB
