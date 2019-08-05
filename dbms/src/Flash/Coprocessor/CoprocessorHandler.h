#pragma once

#include <common/logger_useful.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>
#include <grpcpp/server_context.h>

namespace DB
{

struct CoprocessorContext
{
    Context & db_context;
    const kvrpcpb::Context & kv_context;
    grpc::ServerContext & grpc_server_context;

    CoprocessorContext(Context & db_context_, const kvrpcpb::Context & kv_context_, grpc::ServerContext & grpc_server_context_)
        : db_context(db_context_), kv_context(kv_context_), grpc_server_context(grpc_server_context_)
    {}
};

/// Coprocessor request handler, deals with:
/// 1. DAG request: WIP;
/// 2. Analyze request: NOT IMPLEMENTED;
/// 3. Checksum request: NOT IMPLEMENTED;
class CoprocessorHandler
{
public:
    CoprocessorHandler(CoprocessorContext & cop_context_, const coprocessor::Request * cop_request_, coprocessor::Response * response_);

    ~CoprocessorHandler() = default;

    void execute();

protected:
    enum
    {
        COP_REQ_TYPE_DAG = 103,
        COP_REQ_TYPE_ANALYZE = 104,
        COP_REQ_TYPE_CHECKSUM = 105,
    };

    CoprocessorContext & cop_context;
    const coprocessor::Request * cop_request;
    coprocessor::Response * cop_response;

    Logger * log;
};

} // namespace DB
