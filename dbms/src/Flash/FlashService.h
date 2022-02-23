#pragma once

#include <Common/TiFlashSecurity.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Interpreters/Context.h>
#include <common/ThreadPool.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>
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
class IServer;

class EstablishCallData;

class MPPTunnel;

struct CallDataReg;

class FlashService final : public tikvpb::Tikv::WithAsyncMethod_EstablishMPPConnection<tikvpb::Tikv::Service>
    , public std::enable_shared_from_this<FlashService>
    , private boost::noncopyable
{
public:
    explicit FlashService(IServer & server_);

    ~FlashService();

    grpc::Status Coprocessor(
        grpc::ServerContext * grpc_context,
        const coprocessor::Request * request,
        coprocessor::Response * response) override;

    grpc::Status BatchCommands(grpc::ServerContext * grpc_context,
                               grpc::ServerReaderWriter<tikvpb::BatchCommandsResponse, tikvpb::BatchCommandsRequest> * stream) override;

    ::grpc::Status BatchCoprocessor(::grpc::ServerContext * context,
                                    const ::coprocessor::BatchRequest * request,
                                    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer) override;

    ::grpc::Status DispatchMPPTask(
        ::grpc::ServerContext * context,
        const ::mpp::DispatchTaskRequest * request,
        ::mpp::DispatchTaskResponse * response) override;

    ::grpc::Status IsAlive(
        ::grpc::ServerContext * context,
        const ::mpp::IsAliveRequest * request,
        ::mpp::IsAliveResponse * response) override;

    //    ::grpc::Status EstablishMPPConnection(::grpc::ServerContext * context,
    //                                          const ::mpp::EstablishMPPConnectionRequest * request,
    //                                          ::grpc::ServerWriter<::mpp::MPPDataPacket> * writer) override;

    bool EstablishMPPConnection4Async(::grpc::ServerContext * context, const ::mpp::EstablishMPPConnectionRequest * request, EstablishCallData * calldata);

    ::grpc::Status CancelMPPTask(::grpc::ServerContext * context, const ::mpp::CancelTaskRequest * request, ::mpp::CancelTaskResponse * response) override;

    std::atomic<int> current_active_establish_thds{0};
    std::atomic<int> max_active_establish_thds{0};
    MPMCQueue<CallDataReg> calldata_to_reg_queue;
    std::vector<std::shared_ptr<MPMCQueue<EstablishCallData *>>> calldata_proc_queues;
    std::vector<std::shared_ptr<MPMCQueue<std::shared_ptr<DB::MPPTunnel>>>> tunnel_send_op_queues;
    std::atomic<long long> tunnel_send_idx{0}, rpc_exe_idx{0};

private:
    std::tuple<ContextPtr, ::grpc::Status> createDBContext(const grpc::ServerContext * grpc_context) const;

    // Use executeInThreadPool to submit job to thread pool which return grpc::Status.
    grpc::Status executeInThreadPool(const std::unique_ptr<ThreadPool> & pool, std::function<grpc::Status()>);

private:
    IServer & server;
    const TiFlashSecurityConfig & security_config;
    Poco::Logger * log;

    // Put thread pool member(s) at the end so that ensure it will be destroyed firstly.
    std::unique_ptr<ThreadPool> cop_pool, batch_cop_pool;
};

struct CallDataReg
{
    FlashService * service;
    grpc::ServerCompletionQueue * cq;
    grpc::ServerCompletionQueue * notify_cq;
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

    void notifyReady();

    std::mutex mu;
    std::condition_variable cv;

    void attachQueue(MPMCQueue<std::shared_ptr<mpp::MPPDataPacket>> * send_queue);

    void attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpptunnel);

private:
    void AsyncRpcInitOp();

    void ContinueFromPending();

    // server.
    FlashService * service_;

    // The producer-consumer queue where for asynchronous server notifications.
    grpc::ServerCompletionQueue *cq_, *notify_cq_;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    grpc::ServerContext ctx_;

    grpc::Status status4err;

public:
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
