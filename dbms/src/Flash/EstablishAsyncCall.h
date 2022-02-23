#pragma once


#include <Common/MPMCQueue.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/MPPTunnel.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <kvproto/tikvpb.grpc.pb.h>


#pragma GCC diagnostic pop


namespace DB
{

class EstablishCallData;
class MPPTunnel;
class FlashService;

struct CallDataReg
{
    FlashService * service;
    grpc::ServerCompletionQueue * cq;
    grpc::ServerCompletionQueue * notify_cq;
};

class CallExecPool
{
public:
    CallExecPool();
    ~CallExecPool();
    void SubmitTunnelSendOp(const std::shared_ptr<DB::MPPTunnel> & mpptunnel);
    void SubmitCalldataProcTask(EstablishCallData * cd);
    MPMCQueue<CallDataReg> calldata_to_reg_queue;
    std::vector<std::shared_ptr<MPMCQueue<EstablishCallData *>>> calldata_proc_queues;
    std::vector<std::shared_ptr<MPMCQueue<std::shared_ptr<DB::MPPTunnel>>>> tunnel_send_op_queues;
    std::atomic<long long> tunnel_send_idx{0}, rpc_exe_idx{0};
    std::shared_ptr<ThreadManager> thd_manager;
};

class EstablishCallData
{
public:
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    EstablishCallData(FlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq);

    void Pending();

    bool Write(const mpp::MPPDataPacket & packet, bool need_wait = true);

    bool TryWrite();

    void WriteDone(const ::grpc::Status & status, bool need_wait = true);

    void WriteErr(const mpp::MPPDataPacket & packet);

    void Proceed();

    void Proceed0();

    ::mpp::EstablishMPPConnectionRequest *getRequest(){
        return &request_;
    }

    std::mutex mu;
    std::condition_variable cv;

    void attachQueue(MPMCQueue<std::shared_ptr<mpp::MPPDataPacket>> * send_queue);

    void attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpptunnel);

    void ContinueFromPending(const std::shared_ptr<MPPTunnel> &tunnel, std::string &err_msg);

private:
    void notifyReady();

    void AsyncRpcInitOp();

    // server.
    FlashService * service_;
    CallExecPool * exec_pool;

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