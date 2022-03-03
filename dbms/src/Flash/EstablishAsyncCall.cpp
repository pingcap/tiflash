#include "EstablishAsyncCall.h"

#include <Flash/FlashService.h>
#include <Flash/Mpp/Utils.h>

namespace DB
{
EstablishCallData::EstablishCallData(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq)
    : service_(service)
    , cq_(cq)
    , notify_cq_(notify_cq)
    , responder_(&ctx_)
    , state_(PROCESS)
    , send_queue_(nullptr)
{
    // As part of the initial CREATE state, we *request* that the system
    // start processing requests. In this request, "this" acts are
    // the tag uniquely identifying the request.
    service_->RequestEstablishMPPConnection(&ctx_, &request_, &responder_, cq_, notify_cq_, this);
}

bool EstablishCallData::TryWrite()
{
    // check whether there is a valid msg to write
    {
        std::unique_lock lk(mu);
        if (mpptunnel_ && send_queue_ && ready && send_queue_->isNextPopNonBlocking()) //not ready or no packet
            ready = false;
        else
            return false;
    }
    // there is a valid msg, do single write operation
    mpptunnel_->sendJob(false);
    return true;
}

void EstablishCallData::rpcInitOp()
{
    std::exception_ptr eptr = nullptr;
    try
    {
        service_->EstablishMPPConnection0(&ctx_, &request_, nullptr, this);
    }
    catch (...)
    {
        eptr = std::current_exception();
    }
    if (eptr)
    {
        state_ = FINISH;
        grpc::Status status(static_cast<grpc::StatusCode>(GRPC_STATUS_UNKNOWN), getExceptionMessage(eptr, false));
        responder_.Finish(status, this);
        return;
    }
}

bool EstablishCallData::Write(const mpp::MPPDataPacket & packet)
{
    try
    {
        responder_.Write(packet, this);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

void EstablishCallData::WriteErr(const mpp::MPPDataPacket & packet)
{
    state_ = ERR_HANDLE;
    if (Write(packet))
        status4err = grpc::Status::OK;
    else
        status4err = grpc::Status(grpc::StatusCode::UNKNOWN, "Write error message failed for unknown reason.");
}

void EstablishCallData::WriteDone(const ::grpc::Status & status)
{
    state_ = FINISH;
    if (stopwatch)
    {
        LOG_FMT_INFO(mpptunnel_->getLogger(), "connection for {} cost {} ms.", mpptunnel_->id(), stopwatch->elapsedMilliseconds());
    }
    responder_.Finish(status, this);
}

void EstablishCallData::notifyReady()
{
    std::unique_lock lk(mu);
    ready = true;
}

void EstablishCallData::Proceed()
{
    if (state_ == PROCESS)
    {
        state_ = JOIN;
        // Spawn a new EstablishCallData instance to serve new clients while we process the one for this EstablishCallData.
        // The instance will deallocate itself as part of its FINISH state.
        new EstablishCallData(service_, cq_, notify_cq_);
        notifyReady();
        rpcInitOp();
    }
    else if (state_ == JOIN)
    {
        if (send_queue_ && mpptunnel_ && send_queue_->isNextPopNonBlocking())
        {
            {
                std::unique_lock lk(mu);
                ready = false;
            }
            mpptunnel_->sendJob(true);
        }
        else
            notifyReady();
    }
    else if (state_ == ERR_HANDLE)
    {
        state_ = FINISH;
        WriteDone(status4err);
    }
    else
    {
        assert(state_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (EstablishCallData).
        delete this;
        return;
    }
}

void EstablishCallData::attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpptunnel)
{
    stopwatch = std::make_shared<Stopwatch>();
    this->mpptunnel_ = mpptunnel;
    this->send_queue_ = mpptunnel->getSendQueue();
}
} // namespace DB
