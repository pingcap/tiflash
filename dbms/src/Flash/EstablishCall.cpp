// Copyright 2023 PingCAP, Inc.
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
#include <Common/GRPCQueue.h>
#include <Common/TiFlashMetrics.h>
#include <Common/VariantOp.h>
#include <Flash/EstablishCall.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/Utils.h>
#include <Interpreters/Context.h>
#include <Storages/KVStore/TMTContext.h>

namespace DB
{
namespace FailPoints
{
extern const char random_tunnel_init_rpc_failure_failpoint[];
} // namespace FailPoints

EstablishCallData::EstablishCallData(
    AsyncFlashService * service,
    grpc::ServerCompletionQueue * cq,
    grpc::ServerCompletionQueue * notify_cq,
    const std::shared_ptr<std::atomic<bool>> & is_shutdown)
    : service(service)
    , cq(cq)
    , notify_cq(notify_cq)
    , is_shutdown(is_shutdown)
    , responder(&ctx)
    , state(NEW_REQUEST)
{
    GET_METRIC(tiflash_establish_calldata_count, type_new_request_calldata).Increment();
    // As part of the initial CREATE state, we *request* that the system
    // start processing requests. In this request, "asGRPCKickTag" acts are
    // the tag uniquely identifying the request.
    service->RequestEstablishMPPConnection(&ctx, &request, &responder, cq, notify_cq, asGRPCKickTag());
}

EstablishCallData::EstablishCallData()
    : service(nullptr)
    , cq(nullptr)
    , notify_cq(nullptr)
    , is_shutdown(std::make_shared<std::atomic<bool>>(false))
    , responder(&ctx)
    , state(NEW_REQUEST)
{
    GET_METRIC(tiflash_establish_calldata_count, type_new_request_calldata).Increment();
}

void EstablishCallData::decreaseStateMetrics(CallStatus status)
{
    switch (status)
    {
    case NEW_REQUEST:
        GET_METRIC(tiflash_establish_calldata_count, type_new_request_calldata).Decrement();
        break;
    case WAIT_TUNNEL:
        GET_METRIC(tiflash_establish_calldata_count, type_wait_tunnel_calldata).Decrement();
        break;
    case WAIT_WRITE:
        GET_METRIC(tiflash_establish_calldata_count, type_wait_write_calldata).Decrement();
        break;
    case WAIT_IN_QUEUE:
        GET_METRIC(tiflash_establish_calldata_count, type_wait_in_queue_calldata).Decrement();
        break;
    case WAIT_WRITE_ERR:
        GET_METRIC(tiflash_establish_calldata_count, type_wait_write_err_calldata).Decrement();
        break;
    case FINISH:
        GET_METRIC(tiflash_establish_calldata_count, type_finish_calldata).Decrement();
        break;
    }
}

EstablishCallData::~EstablishCallData()
{
    decreaseStateMetrics(state);
    if (stopwatch)
    {
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_mpp_establish_conn)
            .Observe(stopwatch->elapsedSeconds());
    }
}

void EstablishCallData::setCallStateAndUpdateMetrics(
    EstablishCallData::CallStatus new_state,
    prometheus::Gauge & new_metric)
{
    decreaseStateMetrics(state);
    state = new_state;
    new_metric.Increment();
}

void EstablishCallData::execute(bool ok)
{
    switch (state)
    {
    case NEW_REQUEST:
    {
        if unlikely (!ok)
        {
            delete this;
            return;
        }
        spawn(service, cq, notify_cq, is_shutdown);
        initRpc();
        break;
    }
    case WAIT_TUNNEL:
    {
        /// ok == true means the alarm meet deadline, otherwise means alarm is cancelled
        /// here we don't care the alarm is cancelled or meet deadline, in both cases just
        /// try connect tunnel is ok
        tryConnectTunnel();
        break;
    }
    case WAIT_WRITE:
    case WAIT_IN_QUEUE:
    {
        if unlikely (is_shutdown->load(std::memory_order_relaxed))
        {
            unexpectedWriteDone();
            break;
        }

        // If ok is false,
        // For WAIT_WRITE state, it means grpc write is failed.
        // For WAIT_IN_QUEUE state, it means queue state is finished or cancelled so
        // it is convenient to call trySendOneMsg(call pop queue inside) to handle it which
        // is the same as the case that the pop function is not blocked and the queue is finished
        // or cancelled.
        if (!ok && state == WAIT_WRITE)
        {
            unexpectedWriteDone();
            break;
        }

        trySendOneMsg();
        break;
    }
    case WAIT_WRITE_ERR:
    {
        if unlikely (!ok)
        {
            unexpectedWriteDone();
            break;
        }
        writeDone("state is WAIT_WRITE_ERR", grpc::Status::OK);
        break;
    }
    case FINISH:
    {
        // Once in the FINISH state, deallocate ourselves (EstablishCallData).
        // That's the way GRPC official examples do. link: https://github.com/grpc/grpc/blob/master/examples/cpp/helloworld/greeter_async_server.cc
        delete this;
        break;
    }
    }
}

void EstablishCallData::attachAsyncTunnelSender(const std::shared_ptr<DB::AsyncTunnelSender> & async_tunnel_sender_)
{
    assert(stopwatch != nullptr);
    async_tunnel_sender = async_tunnel_sender_;
    waiting_task_time_ms = stopwatch->elapsedMilliseconds();
    setCall(ctx.c_call());
}

void EstablishCallData::startEstablishConnection()
{
    stopwatch = std::make_unique<Stopwatch>();
}

EstablishCallData * EstablishCallData::spawn(
    AsyncFlashService * service,
    grpc::ServerCompletionQueue * cq,
    grpc::ServerCompletionQueue * notify_cq,
    const std::shared_ptr<std::atomic<bool>> & is_shutdown)
{
    return new EstablishCallData(service, cq, notify_cq, is_shutdown);
}

void EstablishCallData::initRpc()
{
    try
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_tunnel_init_rpc_failure_failpoint);

        connection_id = fmt::format("tunnel{}+{}", request.sender_meta().task_id(), request.receiver_meta().task_id());
        query_id = MPPQueryId(request.sender_meta()).toString();
        resource_group_name = request.sender_meta().resource_group_name();
        auto res = service->establishMPPConnectionAsync(this);

        if (!res.ok())
            writeDone("initRpc called with no-ok status", res);
    }
    catch (...)
    {
        grpc::Status status(static_cast<grpc::StatusCode>(GRPC_STATUS_UNKNOWN), getCurrentExceptionMessage(false));
        writeDone("initRpc called with exception", status);
    }
}

grpc::Alarm & EstablishCallData::getAlarm()
{
    return alarm;
}

void EstablishCallData::tryConnectTunnel()
{
    auto * task_manager = service->getContext()->getTMTContext().getMPPTaskManager().get();
    auto [tunnel, err_msg] = task_manager->findAsyncTunnel(&request, this, cq, *service->getContext());
    if (tunnel == nullptr && err_msg.empty())
    {
        /// Call data will be put to cq by alarm, just return is ok
        return;
    }
    else if (tunnel == nullptr && !err_msg.empty())
    {
        /// Meet error, write the error message
        writeErr(getPacketWithError(err_msg));
        return;
    }
    else if (tunnel != nullptr && err_msg.empty())
    {
        /// Found tunnel
        try
        {
            /// Connect the tunnel
            tunnel->connectAsync(this);
            /// Initialization is successful.
            /// Try to send one message.
            /// If there is no message, the pointer of this class will be saved in `async_tunnel_sender`.
            trySendOneMsg();
        }
        catch (...)
        {
            writeErr(getPacketWithError(getCurrentExceptionMessage(false)));
        }
        return;
    }
    else if (tunnel != nullptr && !err_msg.empty())
    {
        /// Should not reach here
        __builtin_unreachable();
    }
}

void EstablishCallData::write(const mpp::MPPDataPacket & packet)
{
    responder.Write(packet, asGRPCKickTag());
}

void EstablishCallData::writeErr(const mpp::MPPDataPacket & packet)
{
    setCallStateAndUpdateMetrics(
        WAIT_WRITE_ERR,
        GET_METRIC(tiflash_establish_calldata_count, type_wait_write_err_calldata));
    write(packet);
}

static LoggerPtr & getLogger()
{
    static auto logger = Logger::get("EstablishCallData");
    return logger;
}

void EstablishCallData::writeDone(String msg, const grpc::Status & status)
{
    setCallStateAndUpdateMetrics(FINISH, GET_METRIC(tiflash_establish_calldata_count, type_finish_calldata));

    if (async_tunnel_sender)
    {
        auto time = stopwatch->elapsedMilliseconds();
        LOG_IMPL(
            async_tunnel_sender->getLogger(),
            /// if time cost is less than 1s and there is no error, use debug log level
            msg.empty() && time < 1000 ? Poco::Message::PRIO_DEBUG : Poco::Message::PRIO_INFORMATION,
            "async connection for {} cost {} ms, including {} ms to wait task.",
            async_tunnel_sender->getTunnelId(),
            time,
            waiting_task_time_ms);

        RUNTIME_ASSERT(
            !async_tunnel_sender->isConsumerFinished(),
            async_tunnel_sender->getLogger(),
            "tunnel {} consumer finished in advance",
            async_tunnel_sender->getTunnelId());

        if (!msg.empty())
        {
            msg = fmt::format("{}: {}", async_tunnel_sender->getTunnelId(), msg);
        }
        // Trigger mpp tunnel finish work.
        async_tunnel_sender->consumerFinish(msg);
    }
    else if (!connection_id.empty())
    {
        if (stopwatch != nullptr)
            LOG_WARNING(
                getLogger(),
                "EstablishCallData finishes without connected, time cost {}ms, query id: {}, connection id: {}",
                stopwatch != nullptr ? stopwatch->elapsedMilliseconds() : 0,
                query_id,
                connection_id);
        else
            LOG_WARNING(
                getLogger(),
                "EstablishCallData finishes without connected, query id: {}, connection id: {}",
                query_id,
                connection_id);
    }

    responder.Finish(status, asGRPCKickTag());
}

void EstablishCallData::unexpectedWriteDone()
{
    grpc::Status status(static_cast<grpc::StatusCode>(GRPC_STATUS_UNKNOWN), "grpc writes failed");
    writeDone("unexpectedWriteDone called", status);
}

void EstablishCallData::trySendOneMsg()
{
    TrackedMppDataPacketPtr packet;
    auto original_state = state;
    // state must be set to `WAIT_IN_QUEUE` state before calling popWithTag, because if
    // popWithTag returns `MPMCQueueResult::EMPTY`, current `EstablishCallData` will be
    // put into the sender queue, and can be notified by other threads at anytime, which
    // means we should not modify the data of current `EstablishCallData` if popWithTag
    // returns `MPMCQueueResult::EMPTY`.
    state = WAIT_IN_QUEUE;
    auto res = async_tunnel_sender->popWithTag(packet, asGRPCKickTag());
    switch (res)
    {
    case MPMCQueueResult::OK:
        // set state back to original_state so we can use setCallStateAndUpdateMetrics later
        state = original_state;
        async_tunnel_sender->subDataSizeMetric(packet->getPacket().ByteSizeLong());
        /// Note: has to switch the memory tracker before `write`
        /// because after `write`, `async_tunnel_sender` can be destroyed at any time
        /// so there is a risk that `res` is destructed after `aysnc_tunnel_sender`
        /// is destructed which may cause the memory tracker in `res` become invalid
        packet->switchMemTracker(nullptr);
        setCallStateAndUpdateMetrics(
            WAIT_WRITE,
            GET_METRIC(tiflash_establish_calldata_count, type_wait_write_calldata));
        write(packet->packet);
        return;
    case MPMCQueueResult::FINISHED:
        // set state back to original_state so we can use setCallStateAndUpdateMetrics later
        state = original_state;
        writeDone("", grpc::Status::OK);
        return;
    case MPMCQueueResult::CANCELLED:
        // set state back to original_state so we can use setCallStateAndUpdateMetrics later
        state = original_state;
        RUNTIME_ASSERT(!async_tunnel_sender->getCancelReason().empty(), "Tunnel sender cancelled without reason");
        writeErr(getPacketWithError(async_tunnel_sender->getCancelReason()));
        return;
    case MPMCQueueResult::EMPTY:
        // No new message.
        // can not modify the data of current `EstablishCallData` but still we can update metrics here
        decreaseStateMetrics(original_state);
        GET_METRIC(tiflash_establish_calldata_count, type_wait_in_queue_calldata).Increment();
        return;
    default:
        RUNTIME_ASSERT(false, getLogger(), "Result {} is invalid", magic_enum::enum_name(res));
    }
}

} // namespace DB
