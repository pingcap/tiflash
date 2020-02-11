#pragma once

#include <Interpreters/Context.h>
#include <common/ThreadPool.h>
#include <common/logger_useful.h>
#include <grpcpp/server_context.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/tikvpb.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

class TiFlashMetrics;
using TiFlashMetricsPtr = std::shared_ptr<TiFlashMetrics>;

struct BatchCommandsContext
{
    /// Context for this batch commands.
    Context & db_context;

    /// Context creation function for each individual command - they should be handled isolated,
    /// given that context is being used to pass arguments regarding queries.
    using DBContextCreationFunc = std::function<std::tuple<Context, grpc::Status>(const grpc::ServerContext *)>;
    DBContextCreationFunc db_context_creation_func;

    const grpc::ServerContext & grpc_server_context;

    TiFlashMetricsPtr metrics;

    BatchCommandsContext(
        Context & db_context_, DBContextCreationFunc && db_context_creation_func_, grpc::ServerContext & grpc_server_context_);
};

class BatchCommandsHandler
{
public:
    BatchCommandsHandler(BatchCommandsContext & batch_commands_context_, const tikvpb::BatchCommandsRequest & request_,
        tikvpb::BatchCommandsResponse & response_);

    ~BatchCommandsHandler() = default;

    grpc::Status execute();

protected:
    ThreadPool::Job handleCommandJob(
        const tikvpb::BatchCommandsRequest::Request & req, tikvpb::BatchCommandsResponse::Response & resp, grpc::Status & ret) const;

protected:
    const BatchCommandsContext & batch_commands_context;
    const tikvpb::BatchCommandsRequest & request;
    tikvpb::BatchCommandsResponse & response;

    Logger * log;
};

} // namespace DB
