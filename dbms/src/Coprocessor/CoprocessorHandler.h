#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#include <kvproto/coprocessor.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>
#include <grpcpp/server_context.h>

namespace DB {

struct CoprocessorContext {
    Context & ch_context;
    const kvrpcpb::Context & kv_context;
    grpc::ServerContext & grpc_server_context;
    CoprocessorContext(Context & ch_context_, const kvrpcpb::Context & kv_context_,
            grpc::ServerContext & grpc_server_context_)
            : ch_context(ch_context_), kv_context(kv_context_), grpc_server_context(grpc_server_context_) {
    }
};

/** handle coprocesssor request, this is used by tiflash coprocessor.
  */
class CoprocessorHandler {
public:
    CoprocessorHandler(const coprocessor::Request *cop_request, coprocessor::Response *response, CoprocessorContext &context);

    ~CoprocessorHandler();

    bool execute();

private:
    String buildSqlString();
    BlockIO buildCHPlan();
    const coprocessor::Request *cop_request;
    coprocessor::Response *cop_response;
    CoprocessorContext &context;
    tipb::DAGRequest dag_request;

};
}
