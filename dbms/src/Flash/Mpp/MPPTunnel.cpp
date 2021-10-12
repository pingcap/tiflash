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
    , send_queue(input_steams_num_ * 5) /// TODO(fzh) set a reasonable parameter
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
        /// in abnormal cases, popping all packets out of send_queue to avoid blocking any thread pushes packets into it.
        while (send_queue.size() > 0)
        {
            std::optional<MPPDataPacketPtr> pop_item = send_queue.pop();
            if (!pop_item.has_value())
            {
                LOG_ERROR(log, "Failed to pop packet from send_queue in destructor");
                break;
            }
        }
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
            bool success = send_queue.push(std::make_shared<mpp::MPPDataPacket>(getPacketWithError(reason)));
            if (!success)
            {
                LOG_ERROR(log, "Failed to push packet into send_queue, tunnel: " << tunnel_id);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to close tunnel: " + tunnel_id);
        }
    }
    if (connected)
    {
        if (!send_queue.push(nullptr))
        {
            LOG_ERROR(log, "Failed to push end-singal(nullptr) into send_queue, tunnel: " << tunnel_id);
        }
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

        if (!send_queue.push(std::make_shared<mpp::MPPDataPacket>(data)))
        {
            LOG_DEBUG(log, "Failed to push packet into send_queue when writing");
        }
        if (close_after_write)
        {
            std::unique_lock<std::mutex> lk(mu);
            if (!finished)
            {
                /// in abnormal cases, finished can be set in advance and pushing nullptr is also necessary
                if (!send_queue.push(nullptr))
                {
                    LOG_DEBUG(log, "Failed to push end-signal(nullptr) into send_queue when writing");
                }
                LOG_TRACE(log, "sending a nullptr to finish write.");
            }
        }
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
            std::optional<MPPDataPacketPtr> pop_item = send_queue.pop();
            if (pop_item.has_value())
            {
                res = pop_item.value();
            }
            else
            {
                LOG_ERROR(log, "Failed to pop packet from send_queue, tunnel:" << tunnel_id);
            }
            if (nullptr == res || !pop_item.has_value())
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
    if (!send_queue.push(nullptr))
    {
        LOG_ERROR(log, "Failed push end-signal(nullptr) into end_queue [MPPTunnelBase<Writer>::writeDone()]");
    }
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
            std::optional<MPPDataPacketPtr> pop_item = send_queue.pop();
            if (pop_item.has_value())
            {
                res = pop_item.value();
            }
            if (nullptr == res || !pop_item.has_value())
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
    std::unique_lock<std::mutex> lk(mu);
    finished = true;
    cv_for_finished.notify_all();
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class MPPTunnelBase<::grpc::ServerWriter<::mpp::MPPDataPacket>>;

} // namespace DB
