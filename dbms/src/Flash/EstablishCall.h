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

#pragma once

#include <Common/GRPCQueue.h>
#include <Common/MPMCQueue.h>
#include <Common/Stopwatch.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <kvproto/tikvpb.grpc.pb.h>

namespace DB
{
class AsyncTunnelSender;
class AsyncFlashService;

class IAsyncCallData
{
public:
    virtual ~IAsyncCallData() = default;

    /// Attach async sender in order to notify consumer finish msg directly.
    virtual void attachAsyncTunnelSender(const std::shared_ptr<DB::AsyncTunnelSender> &) = 0;

    /// The default `GRPCKickFunc` implementation is to push tag into completion queue.
    /// Here return a user-defined `GRPCKickFunc` only for test.
    virtual std::optional<GRPCKickFunc> getGRPCKickFuncForTest() { return std::nullopt; }
};

class EstablishCallData final
    : public IAsyncCallData
    , public GRPCKickTag
{
public:
    // A state machine used for async grpc api EstablishMPPConnection. When a relative grpc event arrives,
    // it reacts base on current state. The completion queue "cq" and "notify_cq"
    // used for asynchronous communication with the gRPC runtime.
    // "notify_cq" gets the tag back indicating a call has started. All subsequent operations (reads, writes, etc) on that call report back to "cq".
    EstablishCallData(
        AsyncFlashService * service,
        grpc::ServerCompletionQueue * cq,
        grpc::ServerCompletionQueue * notify_cq,
        const std::shared_ptr<std::atomic<bool>> & is_shutdown);

    /// for test
    EstablishCallData();

    ~EstablishCallData() override;

    void execute(bool ok) override;

    void attachAsyncTunnelSender(const std::shared_ptr<DB::AsyncTunnelSender> &) override;
    void startEstablishConnection();
    void setToWaitingTunnelState() { state = WAIT_TUNNEL; }
    bool isWaitingTunnelState() { return state == WAIT_TUNNEL; }

    // Spawn a new EstablishCallData instance to serve new clients while we process the one for this EstablishCallData.
    // The instance will deallocate itself as part of its FINISH state.
    // EstablishCallData will handle its lifecycle by itself.
    static EstablishCallData * spawn(
        AsyncFlashService * service,
        grpc::ServerCompletionQueue * cq,
        grpc::ServerCompletionQueue * notify_cq,
        const std::shared_ptr<std::atomic<bool>> & is_shutdown);

    void tryConnectTunnel();

    const mpp::EstablishMPPConnectionRequest & getRequest() const { return request; }
    grpc::ServerContext * getGrpcContext() { return &ctx; }

    String getResourceGroupName() const { return resource_group_name; }

private:
    /// WARNING: Since a event from one grpc completion queue may be handled by different
    /// thread, it's EXTREMELY DANGEROUS to read/write any data after calling a grpc function
    /// with a `this` pointer because the pointer may be gotten by another grpc thread in
    /// a very short time.
    /// Keep it in mind if you want to change any logic here.

    void initRpc();

    /// The packet will be written to grpc.
    void write(const mpp::MPPDataPacket & packet);
    /// Called when an application error happens.
    /// No more packet can be written after writing this packet.
    void writeErr(const mpp::MPPDataPacket & packet);
    /// Called when write is done.
    /// It will try to call async_sender's consumerFinish to inform it's finished.
    void writeDone(String msg, const grpc::Status & status);
    /// Called when a grpc error happens or in shutdown progress.
    void unexpectedWriteDone();
    /// Try to send one msg.
    void trySendOneMsg();

    // Server instance
    AsyncFlashService * service;

    // The producer-consumer queue where for asynchronous server notifications.
    grpc::ServerCompletionQueue * cq;
    grpc::ServerCompletionQueue * notify_cq;
    std::shared_ptr<std::atomic<bool>> is_shutdown;
    grpc::ServerContext ctx;

    // What we get from the client.
    mpp::EstablishMPPConnectionRequest request;

    // The means to get back to the client.
    grpc::ServerAsyncWriter<mpp::MPPDataPacket> responder;

    // Let's implement a state machine with the following states.
    enum CallStatus
    {
        NEW_REQUEST,
        WAIT_TUNNEL,
        WAIT_WRITE,
        WAIT_POP_FROM_QUEUE,
        WAIT_WRITE_ERR,
        FINISH
    };
    // The current serving state.
    CallStatus state;

    std::shared_ptr<DB::AsyncTunnelSender> async_tunnel_sender;
    std::unique_ptr<Stopwatch> stopwatch;
    String query_id;
    String resource_group_name;
    String connection_id;
    double waiting_task_time_ms = 0;
};
} // namespace DB
