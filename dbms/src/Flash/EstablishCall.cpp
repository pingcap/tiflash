// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    , state(NEW_REQUEST)
{
    // As part of the initial CREATE state, we *request* that the system
    // start processing requests. In this request, "this" acts are
    // the tag uniquely identifying the request.
    service->RequestEstablishMPPConnection(&ctx, &request, &responder, cq, notify_cq, this);
}

EstablishCallData * EstablishCallData::spawn(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq)
{
    return new EstablishCallData(service, cq, notify_cq);
}

void EstablishCallData::tryFlushOne()
{
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

bool EstablishCallData::write(const mpp::MPPDataPacket & packet)
{
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
    if (state == NEW_REQUEST || state == FINISH) // state == NEW_REQUEST means the server is shutdown and no new rpc has come.
    {
        delete this;
        return;
    }
    if (mpp_tunnel)
        mpp_tunnel->consumerFinish("grpc writes failed.", true); //trigger mpp tunnel finish work
    grpc::Status status(static_cast<grpc::StatusCode>(GRPC_STATUS_UNKNOWN), "Consumer exits unexpected, grpc writes failed.");
    responder.Finish(status, this);
}

void EstablishCallData::proceed()
{
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
