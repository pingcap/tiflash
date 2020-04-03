#pragma once

#include <DataStreams/BlockIO.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop
#include <Flash/CoprocessorHandler.h>
#include <grpcpp/impl/codegen/sync_stream.h>
#include <grpcpp/server_context.h>

namespace DB
{

/// Coprocessor request handler, deals with:
/// 1. DAG request: WIP;
/// 2. Analyze request: NOT IMPLEMENTED;
/// 3. Checksum request: NOT IMPLEMENTED;
class BatchCoprocessorHandler
{
public:
    BatchCoprocessorHandler(CoprocessorContext & cop_context_, const coprocessor::BatchRequest * cop_request_,
        ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_);

    ~BatchCoprocessorHandler() = default;

    grpc::Status execute();

protected:
    grpc::Status recordError(grpc::StatusCode err_code, const String & err_msg);

protected:
    enum
    {
        COP_REQ_TYPE_DAG = 103,
        COP_REQ_TYPE_ANALYZE = 104,
        COP_REQ_TYPE_CHECKSUM = 105,
    };

    CoprocessorContext & cop_context;
    const coprocessor::BatchRequest * cop_request;
    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer;

    ::coprocessor::BatchResponse err_response;

    Logger * log;
};

using BatchCopHandlerPtr = std::shared_ptr<BatchCoprocessorHandler>;

} // namespace DB
