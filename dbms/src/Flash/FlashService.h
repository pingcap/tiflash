#pragma once

#include <Interpreters/Context.h>
#include <Server/IServer.h>
#include <common/logger_useful.h>
#include <grpc++/grpc++.h>
#include <boost/noncopyable.hpp>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/tikvpb.grpc.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

using GRPCServerPtr = std::unique_ptr<grpc::Server>;
class FlashService;
using FlashServicePtr = std::shared_ptr<FlashService>;

class FlashService final : public tikvpb::Tikv::Service, public std::enable_shared_from_this<FlashService>, private boost::noncopyable
{
public:
    FlashService(const std::string & address_, IServer & server_);

    ~FlashService() final;

    grpc::Status Coprocessor(
        grpc::ServerContext * grpc_context, const coprocessor::Request * request, coprocessor::Response * response) override;

    grpc::Status BatchCommands(grpc::ServerContext * grpc_context,
        grpc::ServerReaderWriter<tikvpb::BatchCommandsResponse, tikvpb::BatchCommandsRequest> * stream) override;

private:
    std::tuple<Context, ::grpc::Status> createDBContext(grpc::ServerContext * grpc_contex);

private:
    std::string address;
    IServer & server;
    GRPCServerPtr grpc_server;

    Logger * log;
};

} // namespace DB
