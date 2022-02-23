#include "EstablishAsyncCall.h"

#include <Flash/FlashService.h>
#include <Flash/Mpp/Utils.h>


namespace DB
{

const int tunnel_sender_cap = 40;
const int rpc_exe_cap = 200;
const int rpc_reg_cap = 10;

CallExecPool::~CallExecPool()
{
    for (auto & q : tunnel_send_op_queues)
        q->finish();
    for (auto & q : calldata_proc_queues)
        q->finish();
    calldata_to_reg_queue.finish();
    thd_manager->wait();
}

CallExecPool::CallExecPool()
    : calldata_to_reg_queue(1000)
{
    thd_manager = newThreadManager();
    for (int i = 0; i < rpc_reg_cap; i++)
    {
        thd_manager->schedule(false, "calldata_reg", [this] {
            CallDataReg reg;
            while (calldata_to_reg_queue.pop(reg))
            {
                new EstablishCallData(reg.service, reg.cq, reg.notify_cq);
            }
        });
    }
    for (int i = 0; i < rpc_exe_cap; i++)
    {
        calldata_proc_queues.emplace_back(std::make_shared<MPMCQueue<EstablishCallData *>>(100));
        auto queue = calldata_proc_queues.back();
        thd_manager->schedule(false, "rpc_exec", [queue] {
            EstablishCallData * calldata;
            while (queue->pop(calldata))
            {
                calldata->Proceed0();
            }
        });
    }
    for (int i = 0; i < tunnel_sender_cap; i++)
    {
        tunnel_send_op_queues.emplace_back(std::make_shared<MPMCQueue<std::shared_ptr<DB::MPPTunnel>>>(100));
        auto queue = tunnel_send_op_queues[i];
        thd_manager->schedule(false, "tunnel_sender", [queue] {
            std::shared_ptr<DB::MPPTunnel> obj;
            while (queue->pop(obj))
            {
                obj->sendJob();
            }
        });
    }
}

void CallExecPool::SubmitTunnelSendOp(const std::shared_ptr<DB::MPPTunnel> & mpptunnel)
{
    int idx = tunnel_send_idx++;
    idx = idx % tunnel_sender_cap;
    if (idx < 0)
        idx += tunnel_sender_cap;
    tunnel_send_op_queues[idx]->push(mpptunnel);
}

void CallExecPool::SubmitCalldataProcTask(EstablishCallData * cd)
{
    int idx = rpc_exe_idx++;
    idx = idx % rpc_exe_cap;
    if (idx < 0)
        idx += rpc_exe_cap;
    calldata_proc_queues[idx]->push(cd);
}

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

EstablishCallData::EstablishCallData(FlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq)
    : service_(service)
    , exec_pool(service->exec_pool.get())
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
            WriteDone(status, false);
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
    exec_pool->SubmitTunnelSendOp(mpptunnel_);
    return true;
}

void EstablishCallData::AsyncRpcInitOp()
{
    std::exception_ptr eptr = nullptr;
    try
    {
        service_->EstablishMPPConnection4Async(&ctx_, &request_, this);
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

bool EstablishCallData::Write(const mpp::MPPDataPacket & packet, bool need_wait)
{
    // The actual processing.
    try
    {
        if (need_wait)
        {
            std::unique_lock lk(mu);
            cv.wait(lk, [&] { return ready.load(); });
            ready = false;
        }
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

void EstablishCallData::WriteDone(const ::grpc::Status & status, bool need_wait)
{
    // And we are done! Let the gRPC runtime know we've finished, using the
    // memory address of this instance as the uniquely identifying tag for
    // the event.
    if (need_wait)
    {
        std::unique_lock lk(mu);
        cv.wait(lk, [&] { return ready.load(); });
        ready = false;
    }
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
    else
    {
        exec_pool->SubmitCalldataProcTask(this);
    }
}

void EstablishCallData::Proceed0()
{
    if (state_ == PROCESS)
    {
        state_ = JOIN;
        // Spawn a new EstablishCallData instance to serve new clients while we process
        // the one for this EstablishCallData. The instance will deallocate itself as
        // part of its FINISH state.
        exec_pool->calldata_to_reg_queue.push(CallDataReg{service_, cq_, notify_cq_});

        {
            std::unique_lock lk(mu);
            notifyReady();
        }
        AsyncRpcInitOp();
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
                exec_pool->SubmitTunnelSendOp(mpptunnel_);
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
        WriteDone(status4err, false);
    }
    else
    {
        GPR_ASSERT(state_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (EstablishCallData).
        delete this;
        return;
    }
}

void EstablishCallData::attachQueue(MPMCQueue<std::shared_ptr<mpp::MPPDataPacket>> * send_queue)
{
    this->send_queue_ = send_queue;
}

void EstablishCallData::attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpptunnel)
{
    this->mpptunnel_ = mpptunnel;
}

} // namespace DB