#include <Flash/EstablishCall.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/Utils.h>

namespace DB
{
EstablishCallData::EstablishCallData(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq)
    : ctx(std::make_shared<::grpc::ServerContext>())
    , watch_dog(new EstablishCallData(this, ctx))
    , service(service)
    , cq(cq)
    , notify_cq(notify_cq)
    , responder(ctx.get())
    , state(NEW_REQUEST)
{
    // As part of the initial CREATE state, we *request* that the system
    // start processing requests. In this request, "this" acts are
    // the tag uniquely identifying the request.
    ctx->AsyncNotifyWhenDone(watch_dog);
    service->RequestEstablishMPPConnection(ctx.get(), &request, &responder, cq, notify_cq, this);
}

void EstablishSharedEnv::cancelLoop()
{
    while (!end_syn)
    {
        {
            struct timespec ts;
            clock_gettime(CLOCK_MONOTONIC, &ts);
            long now_ts = ts.tv_sec;
            while (true)
            {
                std::unique_lock<std::mutex> lock(mu);
                if (!wait_deadline_queue.empty())
                {
                    std::pair<long, EstablishCallData *> top_item = wait_deadline_queue.top();
                    if (now_ts >= top_item.first)
                    { // deadline is met
                        wait_deadline_queue.pop();
                        lock.unlock();
                        top_item.second->cancel();
                    }
                    else
                        break;
                }
                else
                    break;
            }
        }
        usleep(1000000); // sleep 1s
    }
}

void EstablishSharedEnv::submitCancelTask(EstablishCallData * calldata)
{
    const int wait_interval = 60; // 60s
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    long deadline_ts = ts.tv_sec + wait_interval; // timestamp of deadline, unit: second
    std::unique_lock<std::mutex> lock(mu);
    wait_deadline_queue.push(std::make_pair(deadline_ts, calldata));
}

std::unique_ptr<EstablishSharedEnv> EstablishSharedEnv::global_instance;

EstablishCallData * EstablishCallData::spawn(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq)
{
    return new EstablishCallData(service, cq, notify_cq);
}

//called by MPPTunnel
void EstablishCallData::tryFlushOne()
{
    std::unique_lock lock(mu);
    if (canceled)
        return;
    // check whether there is a valid msg to write
    {
        if (ready && mpp_tunnel->isSendQueueNextPopNonBlocking()) //not ready or no packet
            ready = false;
        else
            return;
    }
    // there is a valid msg, do single write operation
    mpp_tunnel->sendJob(false);
}

void EstablishCallData::initRpc()
{
    std::exception_ptr eptr = nullptr;
    try
    {
        service->EstablishMPPConnectionSyncOrAsync(ctx.get(), &request, nullptr, this);
    }
    catch (...)
    {
        eptr = std::current_exception();
    }
    if (eptr)
    {
        grpc::Status status(static_cast<grpc::StatusCode>(GRPC_STATUS_UNKNOWN), getExceptionMessage(eptr, false));
        writeDone(status);
    }
}

bool EstablishCallData::write(const mpp::MPPDataPacket & packet)
{
    if (canceled)
        return false;
    responder.Write(packet, this);
    return true;
}

void EstablishCallData::writeErr(const mpp::MPPDataPacket & packet)
{
    state = ERR_HANDLE;
    if (write(packet))
        err_status = grpc::Status::OK;
    else
        err_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Write error message failed for unknown reason.");
}

void EstablishCallData::writeDone(const ::grpc::Status & status)
{
    if (canceled || state == FINISH)
        return;
    state = FINISH;
    if (stopwatch)
    {
        LOG_FMT_INFO(mpp_tunnel->getLogger(), "connection for {} cost {} ms.", mpp_tunnel->id(), stopwatch->elapsedMilliseconds());
    }
    responder.Finish(status, this);
}

void EstablishCallData::notifyReady()
{
    ready = true;
}

//called by background cancel worker.
void EstablishCallData::cancel()
{
    std::unique_lock lock(mu);
    if (mpp_tunnel)
    {
        canceled = true;
        mpp_tunnel->consumerFinish("grpc writes failed.", true); //trigger mpp tunnel finish work
        delete this;
    }
}

//called by hanldeRpcs
void EstablishCallData::proceed()
{
    if (calldata_watched) // handle cancel case, which is triggered by AsyncNotifyWhenDone. calldata_watched is the EstablishCallData to be canceled. "this" is the monitor.
    {
        // watchdog role
        if (ctx->IsCancelled())
        {
            std::unique_lock lock(calldata_watched->mu);
            calldata_watched->canceled = true;
            // if state == FINISH for watchdog, it means watched calldata had been finished and released.
            // So just cancel case of "state != FINISH"
            if (state != FINISH)
                EstablishSharedEnv::global_instance->submitCancelTask(calldata_watched);
        }
        // delete watchdog itself
        delete this;
        return;
    }
    // calldata role
    std::unique_lock lock(mu);
    if (canceled)
        return;
    if (state == NEW_REQUEST)
    {
        state = PROCESSING;

        spawn(service, cq, notify_cq);
        notifyReady();
        initRpc();
    }
    else if (state == PROCESSING)
    {
        if (mpp_tunnel->isSendQueueNextPopNonBlocking())
        {
            ready = false;
            mpp_tunnel->sendJob(true);
        }
        else
            ready = true;
    }
    else if (state == ERR_HANDLE)
    {
        writeDone(err_status);
    }
    else
    {
        assert(state == FINISH);
        watch_dog->state = FINISH; // update state of watchdog, so that it can known if watched calldata had beed released.
        // Once in the FINISH state, deallocate ourselves (EstablishCallData).
        // That't the way GRPC official examples do. link: https://github.com/grpc/grpc/blob/master/examples/cpp/helloworld/greeter_async_server.cc
        delete this;
        return;
    }
}

void EstablishCallData::attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpp_tunnel_)
{
    stopwatch = std::make_shared<Stopwatch>();
    this->mpp_tunnel = mpp_tunnel_;
    {
        if (canceled)
        {
            EstablishSharedEnv::global_instance->submitCancelTask(this);
        }
    }
}

} // namespace DB
