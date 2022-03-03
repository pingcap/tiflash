#include <Flash/EstablishCall.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/Utils.h>

namespace DB
{
EstablishCallData::EstablishCallData(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq)
    : service(service)
    , cq(cq)
    , notify_cq(notify_cq)
    , responder(&ctx)
    , state(PROCESS)
{
    // As part of the initial CREATE state, we *request* that the system
    // start processing requests. In this request, "this" acts are
    // the tag uniquely identifying the request.
    service->RequestEstablishMPPConnection(&ctx, &request, &responder, cq, notify_cq, this);
}

bool EstablishCallData::TryWrite()
{
    // check whether there is a valid msg to write
    {
        std::unique_lock lk(mu);
        if (mpptunnel && ready && mpptunnel->isSendQueueNextPopNonBlocking()) //not ready or no packet
            ready = false;
        else
            return false;
    }
    // there is a valid msg, do single write operation
    mpptunnel->sendJob(false);
    return true;
}

void EstablishCallData::initRpc()
{
    std::exception_ptr eptr = nullptr;
    try
    {
        service->EstablishMPPConnectionSyncOrAsync(&ctx, &request, nullptr, this);
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

bool EstablishCallData::Write(const mpp::MPPDataPacket & packet)
{
    try
    {
        responder.Write(packet, this);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

void EstablishCallData::WriteErr(const mpp::MPPDataPacket & packet)
{
    state = ERR_HANDLE;
    if (Write(packet))
        err_status = grpc::Status::OK;
    else
        err_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Write error message failed for unknown reason.");
}

void EstablishCallData::WriteDone(const ::grpc::Status & status)
{
    state = FINISH;
    if (stopwatch)
    {
        LOG_FMT_INFO(mpptunnel->getLogger(), "connection for {} cost {} ms.", mpptunnel->id(), stopwatch->elapsedMilliseconds());
    }
    responder.Finish(status, this);
}

void EstablishCallData::notifyReady()
{
    std::unique_lock lk(mu);
    ready = true;
}

void EstablishCallData::Proceed()
{
    if (state == PROCESS)
    {
        state = JOIN;
        // Spawn a new EstablishCallData instance to serve new clients while we process the one for this EstablishCallData.
        // The instance will deallocate itself as part of its FINISH state.
        // EstablishCallData will handle its lifecycle by itself.
        new EstablishCallData(service, cq, notify_cq);
        notifyReady();
        initRpc();
    }
    else if (state == JOIN)
    {
        if (mpptunnel && mpptunnel->isSendQueueNextPopNonBlocking())
        {
            {
                std::unique_lock lk(mu);
                ready = false;
            }
            mpptunnel->sendJob(true);
        }
        else
            notifyReady();
    }
    else if (state == ERR_HANDLE)
    {
        state = FINISH;
        WriteDone(err_status);
    }
    else
    {
        assert(state == FINISH);
        // Once in the FINISH state, deallocate ourselves (EstablishCallData).
        // That't the way GRPC official examples do. link: https://github.com/grpc/grpc/blob/master/examples/cpp/helloworld/greeter_async_server.cc
        delete this;
        return;
    }
}

void EstablishCallData::attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpptunnel)
{
    stopwatch = std::make_shared<Stopwatch>();
    this->mpptunnel = mpptunnel;
}
} // namespace DB
