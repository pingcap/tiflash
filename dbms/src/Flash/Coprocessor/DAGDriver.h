#pragma once

#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/RegionInfo.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/impl/codegen/sync_stream.h>
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include <vector>

namespace DB
{
class Context;

/// An abstraction of driver running DAG request.
/// Now is a naive native executor. Might get evolved to drive MPP-like computation.

template <bool batch = false>
class DAGDriver
{
public:
    DAGDriver(
        Context & context_,
        UInt64 start_ts,
        UInt64 schema_ver,
        tipb::SelectResponse * dag_response_,
        bool internal_ = false);

    DAGDriver(
        Context & context_,
        UInt64 start_ts,
        UInt64 schema_ver,
        ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer,
        bool internal_ = false);

    void execute();

private:
    void recordError(Int32 err_code, const String & err_msg);

    const tipb::DAGRequest & dagRequest() const;

    Context & context;

    tipb::SelectResponse * dag_response;

    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer;

    bool internal;

    Poco::Logger * log;
};
} // namespace DB
