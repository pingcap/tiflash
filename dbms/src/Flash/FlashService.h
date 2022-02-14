#pragma once

#include <Common/TiFlashSecurity.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Interpreters/Context.h>
#include <common/ThreadPool.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/tikvpb.grpc.pb.h>

#pragma GCC diagnostic pop

namespace DB
{
class IServer;

class CallData;


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

    bool EstablishMPPConnection4Async(::grpc::ServerContext * context, const ::mpp::EstablishMPPConnectionRequest * request, CallData * calldata);

    ::grpc::Status CancelMPPTask(::grpc::ServerContext * context, const ::mpp::CancelTaskRequest * request, ::mpp::CancelTaskResponse * response) override;

    std::atomic<int> current_active_establish_thds{0};
    std::atomic<int> max_active_establish_thds{0};

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
    std::atomic<bool> end_syn = {false}, end_fin{false};
};

class MPPTunnel;

class CallData
{
public:
    struct StatsHook
    {
        StatsHook(FlashService * service_)
        {
            service_->current_active_establish_thds++;
            service_->max_active_establish_thds = std::max(service_->max_active_establish_thds.load(), service_->current_active_establish_thds.load());
        }
        ~StatsHook()
        {
            service_->current_active_establish_thds--;
            service_->max_active_establish_thds = std::max(service_->max_active_establish_thds.load(), service_->current_active_establish_thds.load());
        }
        FlashService * service_;
    };

    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    CallData(FlashService * service, grpc::ServerCompletionQueue * cq);

    void Pending();

    void OnCancel();

    bool Write(const mpp::MPPDataPacket & packet, bool need_wait = true);

    bool TryWrite(std::unique_lock<std::mutex> * p_lk = nullptr, bool trace = false);

    void WriteDone(const ::grpc::Status & status, bool need_wait = true);

    void WriteErr(const mpp::MPPDataPacket & packet);

    void Proceed();

    void notifyReady();

    std::mutex mu;
    std::condition_variable cv;

    void attachQueue(MPMCQueue<std::shared_ptr<mpp::MPPDataPacket>> * send_queue);

    void attachTunnel(std::shared_ptr<DB::MPPTunnel> mpptunnel);

//private:
    // The means of communication with the gRPC runtime for an asynchronous
    // server.
    FlashService * service_;

    // The producer-consumer queue where for asynchronous server notifications.
    grpc::ServerCompletionQueue * cq_;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    grpc::ServerContext ctx_;

    grpc::Status status4err;

    // What we get from the client.
    ::mpp::EstablishMPPConnectionRequest request_;
    // What we send back to the client.
    ::mpp::DispatchTaskResponse reply_;

    // The means to get back to the client.
    ::grpc::ServerAsyncWriter<::mpp::MPPDataPacket> responder_;
    std::atomic<bool> ready{false};
    std::atomic<int> fail_token{-1};

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
