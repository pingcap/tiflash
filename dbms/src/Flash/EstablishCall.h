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

#pragma once

#include <Common/MPMCQueue.h>
#include <Common/Stopwatch.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/PacketWriter.h>

namespace DB
{
class MPPTunnel;
class AsyncFlashService;

class SyncPacketWriter : public PacketWriter
{
public:
    explicit SyncPacketWriter(grpc::ServerWriter<mpp::MPPDataPacket> * writer)
        : writer(writer)
    {}

    bool write(const mpp::MPPDataPacket & packet) override { return writer->Write(packet); }

private:
    ::grpc::ServerWriter<::mpp::MPPDataPacket> * writer;
};

class EstablishCallData : public PacketWriter
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

    ~EstablishCallData();

    bool write(const mpp::MPPDataPacket & packet) override;

    void tryFlushOne() override;

    void writeDone(const ::grpc::Status & status) override;

    void writeErr(const mpp::MPPDataPacket & packet);

    void proceed();

    void cancel();

    void attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpp_tunnel_);

    // Spawn a new EstablishCallData instance to serve new clients while we process the one for this EstablishCallData.
    // The instance will deallocate itself as part of its FINISH state.
    // EstablishCallData will handle its lifecycle by itself.
    static EstablishCallData * spawn(
        AsyncFlashService * service,
        grpc::ServerCompletionQueue * cq,
        grpc::ServerCompletionQueue * notify_cq,
        const std::shared_ptr<std::atomic<bool>> & is_shutdown);

private:
    void notifyReady();

    void initRpc();

    void finishTunnelAndResponder();

    void responderFinish(const grpc::Status & status);

    std::mutex mu;
    std::mutex proceed_mutex;
    // server instance
    AsyncFlashService * service;

    // The producer-consumer queue where for asynchronous server notifications.
    ::grpc::ServerCompletionQueue * cq;
    ::grpc::ServerCompletionQueue * notify_cq;
    std::shared_ptr<std::atomic<bool>> is_shutdown;
    ::grpc::ServerContext ctx;
    ::grpc::Status err_status;

    // What we get from the client.
    ::mpp::EstablishMPPConnectionRequest request;

    // The means to get back to the client.
    ::grpc::ServerAsyncWriter<::mpp::MPPDataPacket> responder;

    // If the CallData is ready to write a msg. Like a semaphore. We can only write once, when it's CQ event comes.
    // It's protected by mu.
    bool ready = false;

    // Let's implement a state machine with the following states.
    enum CallStatus
    {
        NEW_REQUEST,
        PROCESSING,
        ERR_HANDLE,
        FINISH
    };
    CallStatus state; // The current serving state.
    std::shared_ptr<DB::MPPTunnel> mpp_tunnel = nullptr;
    std::shared_ptr<Stopwatch> stopwatch;
};
} // namespace DB
