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

#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/VariantOp.h>
#include <Flash/EstablishCall.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/GRPCSendQueue.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/Utils.h>

namespace DB
{
namespace FailPoints
{
extern const char random_tunnel_init_rpc_failure_failpoint[];
} // namespace FailPoints

EstablishCallData::EstablishCallData(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq, const std::shared_ptr<std::atomic<bool>> & is_shutdown)
    : service(service)
    , cq(cq)
    , notify_cq(notify_cq)
    , is_shutdown(is_shutdown)
    , responder(&ctx)
    , state(NEW_REQUEST)
{
    GET_METRIC(tiflash_object_count, type_count_of_establish_calldata).Increment();
    // As part of the initial CREATE state, we *request* that the system
    // start processing requests. In this request, "this" acts are
    // the tag uniquely identifying the request.
    service->RequestEstablishMPPConnection(&ctx, &request, &responder, cq, notify_cq, this);
}

EstablishCallData::~EstablishCallData()
{
    GET_METRIC(tiflash_object_count, type_count_of_establish_calldata).Decrement();
    if (stopwatch)
    {
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_mpp_establish_conn).Observe(stopwatch->elapsedSeconds());
    }
}

void EstablishCallData::proceed(bool ok)
{
    if (unlikely(!ok))
    {
        /// state == NEW_REQUEST means the server is shutdown and no new rpc has come.
        if (state == NEW_REQUEST || state == FINISH)
        {
            delete this;
            return;
        }
        unexpectedWriteDone();
        return;
    }

    if (state == NEW_REQUEST)
    {
        spawn(service, cq, notify_cq, is_shutdown);
        initRpc();
    }
    else if (state == PROCESSING)
    {
        if (unlikely(is_shutdown->load(std::memory_order_relaxed)))
        {
            unexpectedWriteDone();
            return;
        }

        trySendOneMsg();
    }
    else if (state == ERR_HANDLE)
    {
        writeDone("state is ERR_HANDLE", grpc::Status::OK);
    }
    else
    {
        assert(state == FINISH);
        // Once in the FINISH state, deallocate ourselves (EstablishCallData).
        // That's the way GRPC official examples do. link: https://github.com/grpc/grpc/blob/master/examples/cpp/helloworld/greeter_async_server.cc
        delete this;
        return;
    }
}

grpc_call * EstablishCallData::grpcCall()
{
    return ctx.c_call();
}

void EstablishCallData::attachAsyncTunnelSender(const std::shared_ptr<DB::AsyncTunnelSender> & async_tunnel_sender_)
{
    assert(stopwatch != nullptr);
    async_tunnel_sender = async_tunnel_sender_;
    waiting_task_time_ms = stopwatch->elapsedMilliseconds();
}

void EstablishCallData::startEstablishConnection()
{
    stopwatch = std::make_unique<Stopwatch>();
}


EstablishCallData * EstablishCallData::spawn(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq, const std::shared_ptr<std::atomic<bool>> & is_shutdown)
{
    return new EstablishCallData(service, cq, notify_cq, is_shutdown);
}

void EstablishCallData::initRpc()
{
    std::exception_ptr eptr = nullptr;
    try
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_tunnel_init_rpc_failure_failpoint);

        auto res = service->establishMPPConnectionSyncOrAsync(&ctx, &request, nullptr, this);

        bool success = true;
        std::visit(variant_op::overloaded{
                       [&, this](grpc::Status & status) {
                           if (!status.ok())
                           {
                               writeDone("initRpc called with no-ok status", status);
                               success = false;
                           }
                       },
                       [&, this](std::string & err_msg) {
                           writeErr(getPacketWithError(err_msg));
                           success = false;
                       }},
                   res);
        if (!success)
        {
            // If success is false, return immediately due to calling `write` or `writeErr`.
            return;
        }
    }
    catch (...)
    {
        eptr = std::current_exception();
    }
    if (eptr)
    {
        grpc::Status status(static_cast<grpc::StatusCode>(GRPC_STATUS_UNKNOWN), getExceptionMessage(eptr, false));
        writeDone("initRpc called with exception", status);
        return;
    }

    // Initialization is successful.
    state = PROCESSING;

    // Try to send one message.
    // If there is no message, the pointer of this class will be saved in `async_tunnel_sender`.
    trySendOneMsg();
}

void EstablishCallData::write(const mpp::MPPDataPacket & packet)
{
    responder.Write(packet, this);
}

void EstablishCallData::writeErr(const mpp::MPPDataPacket & packet)
{
    state = ERR_HANDLE;
    write(packet);
}

void EstablishCallData::writeDone(String msg, const grpc::Status & status)
{
    state = FINISH;

    if (async_tunnel_sender)
    {
<<<<<<< HEAD
        if (stopwatch)
        {
            LOG_FMT_INFO(async_tunnel_sender->getLogger(), "connection for {} cost {} ms.", async_tunnel_sender->getTunnelId(), stopwatch->elapsedMilliseconds());
        }
=======
        LOG_INFO(async_tunnel_sender->getLogger(), "connection for {} cost {} ms, including {} ms to waiting task.", async_tunnel_sender->getTunnelId(), stopwatch->elapsedMilliseconds(), waiting_task_time_ms);
>>>>>>> e57a6063da (fix metrics for establish connection (#6203))

        RUNTIME_ASSERT(!async_tunnel_sender->isConsumerFinished(), async_tunnel_sender->getLogger(), "tunnel {} consumer finished in advance", async_tunnel_sender->getTunnelId());

        if (!msg.empty())
        {
            msg = fmt::format("{}: {}", async_tunnel_sender->getTunnelId(), msg);
        }
        // Trigger mpp tunnel finish work.
        async_tunnel_sender->consumerFinish(msg);
    }

    responder.Finish(status, this);
}

void EstablishCallData::unexpectedWriteDone()
{
    grpc::Status status(static_cast<grpc::StatusCode>(GRPC_STATUS_UNKNOWN), "grpc writes failed");
    writeDone("unexpectedWriteDone called", status);
}

void EstablishCallData::trySendOneMsg()
{
    TrackedMppDataPacketPtr res;
    switch (async_tunnel_sender->pop(res, this))
    {
    case GRPCSendQueueRes::OK:
        write(res->packet);
        return;
    case GRPCSendQueueRes::FINISHED:
        writeDone("", grpc::Status::OK);
        return;
    case GRPCSendQueueRes::EMPTY:
        // No new message.
        return;
    }
}

} // namespace DB
