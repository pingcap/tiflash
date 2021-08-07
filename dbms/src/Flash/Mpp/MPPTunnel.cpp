#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/Utils.h>
#include <Flash/Mpp/TaskStatus.h>
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
    const std::shared_ptr<MPPTask> & current_task_)
    : connected(false),
      finished(false),
      timeout(timeout_),
      current_task(current_task_),
      tunnel_id(fmt::format("tunnel{}+{}", sender_meta_.task_id(), receiver_meta_.task_id())),
      log(&Logger::get(tunnel_id))
{
}

MPPTunnel::~MPPTunnel()
{
    try
    {
        if (!finished)
            writeDone();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in destructor function of MPPTunnel");
    }
}

void MPPTunnel::close(const String & reason)
{
    std::unique_lock<std::mutex> lk(mu);
    if (finished)
        return;
    if (connected && !reason.empty())
    {
        try
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_close_tunnel);
            if (!writer->Write(getPacketWithError(reason)))
                throw Exception("Failed to write err");
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to close tunnel: " + tunnel_id);
        }
    }
    finishWithLock();
}

bool MPPTunnel::isTaskCancelled()
{
    auto sp = current_task.lock();
    return sp != nullptr && sp->getStatus() == CANCELLED;
}

// TODO: consider to hold a buffer
void MPPTunnel::write(const mpp::MPPDataPacket & data, bool close_after_write)
{

    LOG_TRACE(log, "ready to write");
    {
        std::unique_lock<std::mutex> lk(mu);

        waitUntilConnectedOrCancelled(lk);
        if (finished)
            throw Exception("write to tunnel which is already closed.");
        if (!writer->Write(data))
            throw Exception("Failed to write data");
        if (close_after_write)
            finishWithLock();
    }
    if (close_after_write)
        LOG_TRACE(log, "finish write and close the tunnel");
    else
        LOG_TRACE(log, "finish write");
}

void MPPTunnel::writeDone()
{
    LOG_TRACE(log, "ready to finish");
    {
        std::unique_lock<std::mutex> lk(mu);
        if (finished)
            throw Exception("has finished");
        /// make sure to finish the tunnel after it is connected
        waitUntilConnectedOrCancelled(lk);
        finishWithLock();
    }
    LOG_TRACE(log, "done to finish");
}

void MPPTunnel::connect(::grpc::ServerWriter<::mpp::MPPDataPacket> * writer_)
{
    std::lock_guard<std::mutex> lk(mu);
    if (connected)
    {
        throw Exception("has connected");
    }
    LOG_DEBUG(log, "ready to connect");
    connected = true;
    writer = writer_;

    cv_for_connected.notify_all();
}

void MPPTunnel::waitForFinish()
{
    std::unique_lock<std::mutex> lk(mu);

    cv_for_finished.wait(lk, [&]() { return finished; });
}

void MPPTunnel::waitUntilConnectedOrCancelled(std::unique_lock<std::mutex> & lk)
{
    auto connected_or_cancelled = [&]() { return connected || isTaskCancelled(); };
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

void MPPTunnel::finishWithLock()
{
    finished = true;
    cv_for_finished.notify_all();
}

} // namespace DB

