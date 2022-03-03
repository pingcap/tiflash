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
    SyncPacketWriter(grpc::ServerWriter<mpp::MPPDataPacket> * writer)
        : writer(writer)
    {}

    bool Write(const mpp::MPPDataPacket & packet) override { return writer->Write(packet); }

    ::grpc::ServerWriter<::mpp::MPPDataPacket> * writer;
};

class EstablishCallData : public PacketWriter
{
public:
    // A state machine used for async grpc api EstablishMPPConnection. When a relative grpc event arrives,
    // it reacts base on current state. The completion queue "cq" and "notify_cq"
    // used for asynchronous communication with the gRPC runtime.
    EstablishCallData(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq);

    bool Write(const mpp::MPPDataPacket & packet) override;

    bool TryWrite() override;

    void WriteDone(const ::grpc::Status & status) override;

    void WriteErr(const mpp::MPPDataPacket & packet);

    void Proceed();

    ::mpp::EstablishMPPConnectionRequest * getRequest()
    {
        return &request_;
    }

    void attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpptunnel);

private:
    void notifyReady();

    void rpcInitOp();

    std::mutex mu;
    // server instance
    AsyncFlashService * service_;

    // The producer-consumer queue where for asynchronous server notifications.
    grpc::ServerCompletionQueue *cq_, *notify_cq_;
    grpc::ServerContext ctx_;
    ::grpc::Status status4err;

    // What we get from the client.
    ::mpp::EstablishMPPConnectionRequest request_;

    // The means to get back to the client.
    ::grpc::ServerAsyncWriter<::mpp::MPPDataPacket> responder_;

    // If the CallData is ready to write a msg.
    bool ready = false;

    // Let's implement a state machine with the following states.
    enum CallStatus
    {
        PROCESS,
        JOIN,
        ERR_HANDLE,
        FINISH
    };
    std::atomic<CallStatus> state_; // The current serving state.
    MPMCQueue<std::shared_ptr<mpp::MPPDataPacket>> * send_queue_ = nullptr;
    std::shared_ptr<DB::MPPTunnel> mpptunnel_ = nullptr;
    std::shared_ptr<Stopwatch> stopwatch;
};
} // namespace DB
