#pragma once

#include <DataStreams/BlockIO.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>
#include <grpcpp/server_context.h>
#include <kvproto/coprocessor.pb.h>
#include <kvproto/tikvpb.grpc.pb.h>
#include <tipb/select.pb.h>

namespace DB
{

class Context;

class RegionInfo
{
public:
    RegionID region_id;
    UInt64 region_version;
    UInt64 region_conf_version;
    std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> key_ranges;
    RegionInfo(RegionID id, UInt64 ver, UInt64 conf_ver, std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> && key_ranges_)
        : region_id(id), region_version(ver), region_conf_version(conf_ver), key_ranges(std::move(key_ranges_))
    {}

    RegionInfo(const RegionInfo & info) : RegionInfo(info.region_id, info.region_version, info.region_conf_version, {})
    {
        for (auto & key_range : info.key_ranges)
            key_ranges.emplace_back(
                std::make_pair(DecodedTiKVKey(std::string(key_range.first)), DecodedTiKVKey(std::string(key_range.second))));
    }

    RegionInfo(RegionInfo && info) : RegionInfo(info.region_id, info.region_version, info.region_conf_version, std::move(info.key_ranges))
    {}
};

/// An abstraction of driver running DAG request.
/// Now is a naive native executor. Might get evolved to drive MPP-like computation.
class DAGDriver
{
public:
    DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, const std::unordered_map<RegionID, RegionInfo> & regions_,
        UInt64 start_ts, UInt64 schema_ver, tipb::SelectResponse & dag_response_, bool internal_ = false);

    void execute();

    void batchExecute(::grpc::ServerWriter<::coprocessor::BatchResponse> * writer);

private:
    void recordError(Int32 err_code, const String & err_msg);

private:
    Context & context;

    const tipb::DAGRequest & dag_request;

    const std::unordered_map<RegionID, RegionInfo> & regions;

    tipb::SelectResponse & dag_response;

    bool internal;

    Poco::Logger * log;
};
} // namespace DB
