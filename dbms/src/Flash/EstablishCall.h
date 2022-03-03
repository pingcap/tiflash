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

    bool Write(const mpp::MPPDataPacket & packet) override { return writer->Write(packet); }

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
    EstablishCallData(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq);

    bool Write(const mpp::MPPDataPacket & packet) override;

    void TryWrite() override;

    void WriteDone(const ::grpc::Status & status) override;

    void WriteErr(const mpp::MPPDataPacket & packet);

    void Proceed();

    void attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpptunnel);

    // Spawn a new EstablishCallData instance to serve new clients while we process the one for this EstablishCallData.
    // The instance will deallocate itself as part of its FINISH state.
    // EstablishCallData will handle its lifecycle by itself.
    static EstablishCallData* spawn(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq);

private:
    void notifyReady();

    void initRpc();

    std::mutex mu;
    // server instance
    AsyncFlashService * service;

    // The producer-consumer queue where for asynchronous server notifications.
    ::grpc::ServerCompletionQueue * cq;
    ::grpc::ServerCompletionQueue * notify_cq;
    ::grpc::ServerContext ctx;
    ::grpc::Status err_status;

    // What we get from the client.
    ::mpp::EstablishMPPConnectionRequest request;

    // The means to get back to the client.
    ::grpc::ServerAsyncWriter<::mpp::MPPDataPacket> responder;

    // If the CallData is ready to write a msg. Like a semaphore. We can only write once, when it's CQ event comes.
    bool ready = false;

    // Let's implement a state machine with the following states.
    enum CallStatus
    {
        PROCESS,
        JOIN,
        ERR_HANDLE,
        FINISH
    };
    std::atomic<CallStatus> state; // The current serving state.
    std::shared_ptr<DB::MPPTunnel> mpptunnel = nullptr;
    std::shared_ptr<Stopwatch> stopwatch;
};
} // namespace DB
