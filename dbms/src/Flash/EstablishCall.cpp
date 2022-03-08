#include <Flash/EstablishCall.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/Utils.h>


namespace DB
{

EstablishCallData::EstablishCallData(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq)
    : calldata_watched(nullptr)
    , watch_dog(new EstablishCallData(this))
    , p_ctx(&(watch_dog->ctx)) //  Uses context from watch_dog to avoid context early released. Since watch_dog will check context, even when watched calldata is released.
    , service(service)
    , cq(cq)
    , notify_cq(notify_cq)
    , responder(p_ctx)
    , state(NEW_REQUEST)

{
    // As part of the initial CREATE state, we *request* that the system
    // start processing requests. In this request, "this" acts are
    // the tag uniquely identifying the request.
    p_ctx->AsyncNotifyWhenDone(watch_dog);
    service->RequestEstablishMPPConnection(p_ctx, &request, &responder, cq, notify_cq, this);
}

EstablishCallData * EstablishCallData::spawn(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq)
{
    return new EstablishCallData(service, cq, notify_cq);
}

void EstablishCallData::tryFlushOne()
{
    if (canceled)
        return;
    // check whether there is a valid msg to write
    {
        std::unique_lock lk(mu);
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
        service->EstablishMPPConnectionSyncOrAsync(p_ctx, &request, nullptr, this);
    }
    catch (...)
    {
        eptr = std::current_exception();
    }
    if (eptr)
    {
        state = FINISH;
        grpc::Status status(static_cast<grpc::StatusCode>(GRPC_STATUS_UNKNOWN), getExceptionMessage(eptr, false));
        responder.Finish(status, this);
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
    if (canceled)
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
    std::unique_lock lk(mu);
    ready = true;
}

void EstablishCallData::cancel()
{
    canceled = true;
    if (mpp_tunnel)
        mpp_tunnel->consumerFinish("grpc canceled.", true); //trigger mpp tunnel finish work
}

void EstablishCallData::proceed()
{
    if (calldata_watched) // handle cancel case, which is triggered by AsyncNotifyWhenDone. calldata_watched is the EstablishCallData to be canceled. "this" is the monitor.
    {
        if (ctx.IsCancelled() && state != FINISH)
        {
            calldata_watched->cancel();
            delete calldata_watched;
        }
        if (calldata_watched == nullptr)
        {
            std::cerr << "calldata_watched == nullptr & != nullptr!!" << std::endl;
        }
        delete this;
        return;
    }
    if (state == NEW_REQUEST)
    {
        state = PROCESSING;

        spawn(service, cq, notify_cq);
        notifyReady();
        initRpc();
    }
    else if (state == PROCESSING)
    {
        std::unique_lock lk(mu);
        if (mpp_tunnel->isSendQueueNextPopNonBlocking())
        {
            ready = false;
            lk.unlock();
            mpp_tunnel->sendJob(true);
        }
        else
            ready = true;
    }
    else if (state == ERR_HANDLE)
    {
        state = FINISH;
        writeDone(err_status);
    }
    else
    {
        assert(state == FINISH);
        // Once in the FINISH state, deallocate ourselves (EstablishCallData).
        // That't the way GRPC official examples do. link: https://github.com/grpc/grpc/blob/master/examples/cpp/helloworld/greeter_async_server.cc
        if (watch_dog)
            watch_dog->state = FINISH;
        delete this;
        return;
    }
}

void EstablishCallData::attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpp_tunnel_)
{
    stopwatch = std::make_shared<Stopwatch>();
    this->mpp_tunnel = mpp_tunnel_;
}
} // namespace DB
