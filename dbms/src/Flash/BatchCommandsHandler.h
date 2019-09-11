#pragma once

#include <Interpreters/Context.h>
#include <common/logger_useful.h>
#include <grpcpp/server_context.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/tikvpb.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

struct BatchCommandsContext
{
    using DBContextCreationFunc = std::function<std::tuple<Context, grpc::Status>(grpc::ServerContext *)>;
    DBContextCreationFunc db_context_creation_func;
    grpc::ServerContext & grpc_server_context;

    BatchCommandsContext(DBContextCreationFunc && db_context_creation_func_, grpc::ServerContext & grpc_server_context_)
        : db_context_creation_func(std::move(db_context_creation_func_)), grpc_server_context(grpc_server_context_)
    {}
};

class BatchCommandsHandler
{
public:
    BatchCommandsHandler(BatchCommandsContext & batch_commands_context_, const tikvpb::BatchCommandsRequest & request_,
        tikvpb::BatchCommandsResponse & response_);

    ~BatchCommandsHandler() = default;

    grpc::Status execute();

protected:
    BatchCommandsContext & batch_commands_context;
    const tikvpb::BatchCommandsRequest & request;
    tikvpb::BatchCommandsResponse & response;

    Logger * log;
};

} // namespace DB
