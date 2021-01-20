#pragma once

#include <Common/TiFlashSecurity.h>
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
class TiFlashMetrics;
using TiFlashMetricsPtr = std::shared_ptr<TiFlashMetrics>;

class FlashService final : public tikvpb::Tikv::Service, public std::enable_shared_from_this<FlashService>, private boost::noncopyable
{
public:
    explicit FlashService(IServer & server_);

    grpc::Status Coprocessor(
        grpc::ServerContext * grpc_context, const coprocessor::Request * request, coprocessor::Response * response) override;

    grpc::Status BatchCommands(grpc::ServerContext * grpc_context,
        grpc::ServerReaderWriter<tikvpb::BatchCommandsResponse, tikvpb::BatchCommandsRequest> * stream) override;

    ::grpc::Status BatchCoprocessor(::grpc::ServerContext * context,
        const ::coprocessor::BatchRequest * request,
        ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer) override;

private:
    std::tuple<Context, ::grpc::Status> createDBContext(const grpc::ServerContext * grpc_context) const;

    // Use executeInThreadPool to submit job to thread pool which return grpc::Status.
    grpc::Status executeInThreadPool(const std::unique_ptr<ThreadPool> & pool, std::function<grpc::Status()>);

private:
    IServer & server;
    TiFlashMetricsPtr metrics;
    const TiFlashSecurityConfig & security_config;
    Logger * log;

    // Put thread pool member(s) at the end so that ensure it will be destroyed firstly.
    std::unique_ptr<ThreadPool> cop_pool, batch_cop_pool;
};

} // namespace DB
