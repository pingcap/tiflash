#pragma once

#include <Flash/CoprocessorHandler.h>
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/impl/codegen/sync_stream.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

namespace DB
{
class BatchCoprocessorHandler : public CoprocessorHandler
{
public:
    BatchCoprocessorHandler(CoprocessorContext & cop_context_, const coprocessor::BatchRequest * cop_request_, ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_);

    ~BatchCoprocessorHandler() = default;

    grpc::Status execute();

protected:
    grpc::Status recordError(grpc::StatusCode err_code, const String & err_msg);

protected:
    const coprocessor::BatchRequest * cop_request;
    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer;

    ::coprocessor::BatchResponse err_response;
};

using BatchCopHandlerPtr = std::shared_ptr<BatchCoprocessorHandler>;

} // namespace DB
