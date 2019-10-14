#pragma once

#include <Interpreters/Context.h>
#include <common/logger_useful.h>
#include <boost/noncopyable.hpp>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/tikvpb.grpc.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

class IServer;

class FlashService final : public tikvpb::Tikv::Service, public std::enable_shared_from_this<FlashService>, private boost::noncopyable
{
public:
    explicit FlashService(IServer & server_);

    grpc::Status Coprocessor(
        grpc::ServerContext * grpc_context, const coprocessor::Request * request, coprocessor::Response * response) override;

    grpc::Status BatchCommands(grpc::ServerContext * grpc_context,
        grpc::ServerReaderWriter<tikvpb::BatchCommandsResponse, tikvpb::BatchCommandsRequest> * stream) override;

private:
    std::tuple<Context, ::grpc::Status> createDBContext(const grpc::ServerContext * grpc_contex) const;

private:
    IServer & server;

    Logger * log;
};

} // namespace DB
