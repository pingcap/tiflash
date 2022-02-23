#pragma once

#include <Common/MPMCQueue.h>
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
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    EstablishCallData(AsyncFlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq);

    void Pending();

    bool Write(const mpp::MPPDataPacket & packet) override;

    bool TryWrite() override;

    void WriteDone(const ::grpc::Status & status) override;

    void WriteErr(const mpp::MPPDataPacket & packet);

    void Proceed();

    ::mpp::EstablishMPPConnectionRequest * getRequest()
    {
        return &request_;
    }

    std::mutex mu;
    std::condition_variable cv;

    void attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpptunnel);

    void ContinueFromPending(const std::shared_ptr<MPPTunnel> & tunnel, std::string & err_msg);

private:
    void notifyReady();

    void rpcInitOp();

    // server.
    AsyncFlashService * service_;
    //    CallExecPool * exec_pool;

    // The producer-consumer queue where for asynchronous server notifications.
    grpc::ServerCompletionQueue *cq_, *notify_cq_;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    grpc::ServerContext ctx_;

    grpc::Status status4err;

    // What we get from the client.
    ::mpp::EstablishMPPConnectionRequest request_;

private:
    // The means to get back to the client.
    ::grpc::ServerAsyncWriter<::mpp::MPPDataPacket> responder_;
    std::atomic<bool> ready{false};

    // Let's implement a tiny state machine with the following states.
    enum CallStatus
    {
        CREATE,
        PROCESS,
        PENDING,
        JOIN,
        ERR_HANDLE,
        FINISH
    };
    std::atomic<CallStatus> state_; // The current serving state.

    MPMCQueue<std::shared_ptr<mpp::MPPDataPacket>> * send_queue_ = nullptr;

    std::shared_ptr<DB::MPPTunnel> mpptunnel_ = nullptr;
};

} // namespace DB