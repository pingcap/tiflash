#pragma once

#include <Common/TiFlashSecurity.h>
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
#include <Flash/EstablishAsyncCall.h>
#include <kvproto/tikvpb.grpc.pb.h>

#pragma GCC diagnostic pop

namespace DB
{
class IServer;
class CallExecPool;
class EstablishCallData;

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

    std::unique_ptr<CallExecPool> exec_pool;

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

} // namespace DB
