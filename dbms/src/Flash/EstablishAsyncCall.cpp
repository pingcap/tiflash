#include "EstablishAsyncCall.h"

#include <Flash/FlashService.h>
#include <Flash/Mpp/Utils.h>

namespace DB
{
std::string DeriveErrWhat(std::exception_ptr eptr) // passing by value is ok
{
    try
    {
        if (eptr)
        {
            std::rethrow_exception(eptr);
        }
    }
    catch (const std::exception & e)
    {
        return std::string(e.what());
    }
    return "";
}

EstablishCallData::EstablishCallData(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq)
    : service_(service)
    , cq_(cq)
    , notify_cq_(notify_cq)
    , responder_(&ctx_)
    , state_(CREATE)
    , send_queue_(nullptr)
{
    // Invoke the serving logic right away.
    Proceed();
}

void EstablishCallData::ContinueFromPending(const std::shared_ptr<MPPTunnel> & tunnel, std::string & err_msg)
{
    if (tunnel == nullptr)
        WriteErr(getPacketWithError(err_msg));
    else
    {
        attachTunnel(tunnel);
        state_ = EstablishCallData::CallStatus::JOIN;
        mpptunnel_->is_async = true;
        try
        {
            mpptunnel_->connect(this);
        }
        catch (...)
        {
            grpc::Status status(static_cast<grpc::StatusCode>(GRPC_STATUS_UNKNOWN), "has connected");
            WriteDone(status);
        }
    }
}

bool EstablishCallData::TryWrite()
{
    //check whether there is a valid msg to write
    {
        std::unique_lock lk(mu);
        if (ready && (send_queue_ && (send_queue_->size() || send_queue_->isFinished()) && mpptunnel_)) //not ready or no packet
            ready = false;
        else
            return false;
    }
    //there is a valid msg, submit write task
    //    exec_pool->SubmitTunnelSendOp(mpptunnel_);
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
        grpc::Status status(static_cast<grpc::StatusCode>(GRPC_STATUS_UNKNOWN), DeriveErrWhat(eptr));
        responder_.Finish(status, this);
        return;
    }
}

void EstablishCallData::Pending()
{
    state_ = PENDING;
}

bool EstablishCallData::Write(const mpp::MPPDataPacket & packet)
{
    // The actual processing.
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
    bool ret = Write(packet);
    if (ret)
        status4err = grpc::Status::OK;
    else
        status4err = grpc::Status(grpc::StatusCode::UNKNOWN, "Write error message failed for unknown reason.");
}

void EstablishCallData::WriteDone(const ::grpc::Status & status)
{
    // And we are done! Let the gRPC runtime know we've finished, using the
    // memory address of this instance as the uniquely identifying tag for
    // the event.
    state_ = FINISH;
    responder_.Finish(status, this);
}

void EstablishCallData::notifyReady()
{
    ready = true;
    cv.notify_one();
}

void EstablishCallData::Proceed()
{
    if (state_ == CREATE)
    {
        // Make this instance progress to the PROCESS state.
        state_ = PROCESS;

        // As part of the initial CREATE state, we *request* that the system
        // start processing requests. In this request, "this" acts are
        // the tag uniquely identifying the request (so that different EstablishCallData
        // instances can serve different requests concurrently), in this case
        // the memory address of this EstablishCallData instance.
        service_->RequestEstablishMPPConnection(&ctx_, &request_, &responder_, cq_, notify_cq_, this);
    }
    else if (state_ == PROCESS)
    {
        state_ = JOIN;
        // Spawn a new EstablishCallData instance to serve new clients while we process
        // the one for this EstablishCallData. The instance will deallocate itself as
        // part of its FINISH state.
        new EstablishCallData(service_, cq_, notify_cq_);
        {
            std::unique_lock lk(mu);
            notifyReady();
        }
        rpcInitOp();
    }
    else if (state_ == PENDING)
    {
    }
    else if (state_ == JOIN)
    {
        {
            std::unique_lock lk(mu);
            if (send_queue_ && (send_queue_->size() || send_queue_->isFinished()) && mpptunnel_)
            {
                ready = false;
                lk.unlock();
                mpptunnel_->sendJob(true);
                //                exec_pool->SubmitTunnelSendOp(mpptunnel_);
            }
            else
            {
                notifyReady();
            }
        }
    }
    else if (state_ == ERR_HANDLE)
    {
        state_ = FINISH;
        WriteDone(status4err);
    }
    else
    {
        GPR_ASSERT(state_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (EstablishCallData).
        delete this;
        return;
    }
}

void EstablishCallData::attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpptunnel)
{
    this->mpptunnel_ = mpptunnel;
    this->send_queue_ = mpptunnel->getSendQueue();
}

} // namespace DB