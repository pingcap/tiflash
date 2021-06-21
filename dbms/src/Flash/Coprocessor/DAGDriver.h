#pragma once

#include <DataStreams/BlockIO.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>
#include <grpcpp/server_context.h>
#include <kvproto/coprocessor.pb.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Storages/RegionQueryInfo.h>
#include <kvproto/tikvpb.grpc.pb.h>
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop

namespace DB
{

class Context;

class RegionInfo
{
public:
    const RegionVerID region_ver_id;
    std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> key_ranges;
    const std::unordered_set<UInt64> * bypass_lock_ts;

    RegionInfo(const RegionVerID & region_ver_id_, std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> && key_ranges_,
        const std::unordered_set<UInt64> * bypass_lock_ts_)
        : region_ver_id(region_ver_id_), key_ranges(std::move(key_ranges_)), bypass_lock_ts(bypass_lock_ts_)
    {}
};

/// An abstraction of driver running DAG request.
/// Now is a naive native executor. Might get evolved to drive MPP-like computation.

template <bool batch = false>
class DAGDriver
{
public:
    DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, const std::unordered_map<RegionVerID, RegionInfo> & regions_,
        UInt64 start_ts, UInt64 schema_ver, tipb::SelectResponse * dag_response_, bool internal_ = false);

    DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, const std::unordered_map<RegionVerID, RegionInfo> & regions_,
        UInt64 start_ts, UInt64 schema_ver, ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer, bool internal_ = false);

    void execute();

private:
    void recordError(Int32 err_code, const String & err_msg);

private:
    Context & context;

    const tipb::DAGRequest & dag_request;

    const std::unordered_map<RegionVerID, RegionInfo> & regions;

    tipb::SelectResponse * dag_response;

    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer;

    bool internal;

    Poco::Logger * log;
};
} // namespace DB
