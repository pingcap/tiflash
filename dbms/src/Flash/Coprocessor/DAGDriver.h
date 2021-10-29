#pragma once

#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/RegionInfo.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>
#include <grpcpp/impl/codegen/sync_stream.h>
#include <kvproto/coprocessor.pb.h>

#include <tipb/select.pb.h>

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
        const tipb::DAGRequest & dag_request_,
        const RegionInfoMap & regions_,
        const RegionInfoList & retry_regions_,
        UInt64 start_ts,
        UInt64 schema_ver,
        tipb::SelectResponse * dag_response_,
        bool internal_ = false);

    DAGDriver(
        Context & context_,
        const tipb::DAGRequest & dag_request_,
        const RegionInfoMap & regions_,
        const RegionInfoList & retry_regions_,
        UInt64 start_ts,
        UInt64 schema_ver,
        ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer,
        bool internal_ = false);

    void execute();

private:
    void recordError(Int32 err_code, const String & err_msg);

private:
    Context & context;

    const tipb::DAGRequest & dag_request;

    const RegionInfoMap & regions;
    const RegionInfoList & retry_regions;

    tipb::SelectResponse * dag_response;

    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer;

    bool internal;

    Poco::Logger * log;
};
} // namespace DB
